"""``workflow_rules``: the workflow consumer process (workflows v2), started by supervisord as
``python -m hysds.workflow_rules``. A thin kombu shell around :class:`hysds.workflow_engine.Engine`.

It reads ``WORKFLOW_QUEUE`` (job status events from ``log_utils`` and engine events published with
:func:`hysds.workflow_state.publish_run_event`), runs the engine against Redis, and acks only after the
event was handled. A first handler exception requeues the event once; a second one, a malformed event or
a rejected event (unknown run/step) goes to ``WORKFLOW_DLQ`` and is acked. Several consumer processes may
run at once: the engine's state changes are atomic and idempotent (see ``workflow_engine``).

Running workflows do not touch OpenSearch: jobs are submitted from the frozen job definitions straight to
the orchestrator, and kill/cancel revoke the attempts directly through the celery control API (see
:func:`make_revoke_fn`), not through mozart's revoke job (which reads job_status).
"""

import json
import logging
import socket
import time
from datetime import datetime, timezone

from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin

from hysds import workflow_state as ws
from hysds.workflow_engine import Engine, EventRejected

log = logging.getLogger("hysds.workflow_rules")

PREFETCH_COUNT = 10
DEFAULT_DLQ = "workflow_dead_letter"


class WorkflowRulesConsumer(ConsumerMixin):
    def __init__(self, connection, queue_name, engine, prefetch=PREFETCH_COUNT, dlq_name=DEFAULT_DLQ,
                 dlq_publish_fn=None, sleep_fn=time.sleep):
        self.connection = connection
        self.queue_name = queue_name
        self.engine = engine
        self.prefetch = prefetch
        self.dlq_name = dlq_name
        self._dlq_publish_fn = dlq_publish_fn or self._kombu_dlq_publish
        self._sleep = sleep_fn
        exchange = Exchange(queue_name, type="direct", durable=True)
        self.queue = Queue(queue_name, exchange, routing_key=queue_name, durable=True)

    def get_consumers(self, Consumer, channel):
        c = Consumer(queues=[self.queue], callbacks=[self.on_message], accept=["json"])
        c.qos(prefetch_count=self.prefetch)
        return [c]

    # -- dead letters
    def _kombu_dlq_publish(self, payload):
        exchange = Exchange(self.dlq_name, type="direct", durable=True)
        queue = Queue(self.dlq_name, exchange, routing_key=self.dlq_name, durable=True)
        with self.connection.clone() as conn:
            conn.Producer().publish(payload, exchange=exchange, routing_key=self.dlq_name,
                                    declare=[queue], serializer="json", retry=True)

    def dead_letter(self, event, reason):
        try:
            json.dumps(event)
        except (TypeError, ValueError):
            event = repr(event)
        self._dlq_publish_fn({
            "event": event, "reason": reason, "consumer_host": socket.gethostname(),
            "timestamp": f"{datetime.now(timezone.utc).replace(tzinfo=None).isoformat()}Z",
        })
        log.error("workflow event dead-lettered to %s: %s", self.dlq_name, reason)

    @staticmethod
    def validate_event(body):
        """A reason string if ``body`` cannot be a workflow event, else None."""
        if not isinstance(body, dict):
            return f"event body is not an object: {type(body).__name__}"
        keys = ("run_id",) if body.get("kind") else ("run_id", "step_id", "status")
        for k in keys:
            if not isinstance(body.get(k), str) or not body[k]:
                return f"missing or non-string {k!r}"
        return None

    def on_decode_error(self, message, exc):
        try:
            raw = message.body
            raw = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
            self.dead_letter(raw[:8192], f"malformed event: undecodable body: {type(exc).__name__}: {exc}")
        except Exception:
            log.exception("DLQ publish failed for an undecodable event; requeueing")
            self._sleep(1)
            message.requeue()
            return
        message.ack()

    def on_message(self, body, message):
        try:
            problem = self.validate_event(body)
            if problem:
                self.dead_letter(body, f"malformed event: {problem}")
                message.ack()
                return
            note = self.engine.handle_event(body)
            log.info("workflow_event run_id=%s kind=%s step_id=%s status=%s -> %s", body.get("run_id"),
                     body.get("kind") or "job_status", body.get("step_id"), body.get("status"), note)
        except EventRejected as exc:
            self.dead_letter(body, f"rejected: {exc}")
        except Exception as exc:
            if message.delivery_info.get("redelivered"):
                log.exception("POISON workflow event (failed twice), dead-lettering: %r", body)
                try:
                    self.dead_letter(body, f"poison event: failed twice: {type(exc).__name__}: {exc}")
                except Exception:
                    log.exception("DLQ publish failed; requeueing")
                    self._sleep(1)
                    message.requeue()
                    return
                message.ack()
            else:
                log.exception("workflow event handler failed, requeueing once: %r", body)
                self._sleep(1)
                message.requeue()
            return
        message.ack()


def make_revoke_fn(redis_client, control=None, marker_ttl=None, signal="SIGTERM", marker_tmpl=None):
    """``revoke(run_id, payload_ids, username)``: revoke the attempts directly through celery, with no
    OpenSearch and no revoke job (mozart's revoke job reads job_status to find the task ids; the engine
    already knows them). For each attempt (its payload id = the orchestrator task id):

    * the run_job **task id** recorded from the job-queued/job-started event (``wf:task:<run>``) gets what
      ``lightweight-jobs/utils.revoke`` (mozart's revoke job) does: the Redis marker
      ``hysds-revoked-task-<task id>`` (TTL ``HYSDS_JOB_STATUS_EXPIRES``; ``job_worker.run_job`` checks it when
      the task starts, so a task that sat in the broker or on a worker that missed the broadcast still stops),
      then ``control.revoke(task_id, terminate=True, signal=...)``. The worker's ``task_revoked`` handler
      (HC-654) then stops the job's containers whether the task was running (signal) or still queued;
    * the **payload id** itself is revoked without terminating, so an orchestrator task that has not run
      yet never creates the run_job task. If it is already creating it, the task id arrives later in a
      job-queued event and the engine revokes it then (the attempt is recorded ``cancelled``).

    Failures are logged per attempt and never raised (the state change is committed)."""
    def revoke(run_id, payload_ids, username):
        ctl = control
        if ctl is None:
            from hysds.celery import app
            ctl = app.control
        tmpl = marker_tmpl
        ttl = marker_ttl
        if tmpl is None:
            from hysds.log_utils import REVOKED_TASK_TMPL
            tmpl = REVOKED_TASK_TMPL
        if ttl is None:
            from hysds.celery import app
            ttl = int(app.conf.get("HYSDS_JOB_STATUS_EXPIRES", 86400))
        for pid in payload_ids:
            try:
                ctl.revoke(pid)  # orchestrator task: only matters while it is still queued
                tid = ws.get_task_id(redis_client, run_id, pid)
                if tid:
                    redis_client.setex(tmpl % tid, ttl, "workflow-revoked")
                    ctl.revoke(tid, terminate=True, signal=signal)
                else:
                    log.info("run %s: attempt %s has no task id yet; revoked the payload id, the job-queued "
                             "event will revoke the task", run_id, pid)
            except Exception:  # noqa: BLE001
                log.exception("run %s: could not revoke attempt %s", run_id, pid)
    return revoke


def main():
    from redis import StrictRedis

    from hysds.celery import app
    from hysds_commons.workflow_job2 import submit_frozen_job

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    queue_name = app.conf.WORKFLOW_QUEUE
    dlq_name = app.conf.get("WORKFLOW_DLQ", DEFAULT_DLQ)
    r = StrictRedis.from_url(app.conf.REDIS_JOB_STATUS_URL, ssl_cert_reqs="none")
    engine = Engine(r, submit_fn=lambda job, payload_id: submit_frozen_job(job, payload_id),
                    revoke_fn=make_revoke_fn(r, app.control,
                                             marker_ttl=int(app.conf.get("HYSDS_JOB_STATUS_EXPIRES", 86400)),
                                             signal=app.conf.get("WORKFLOW_REVOKE_SIGNAL", "SIGTERM")),
                    list_key=app.conf.get("REDIS_JOB_STATUS_KEY", "logstash"))
    with app.connection() as conn:
        log.info("workflow_rules (v2 engine) consuming %s", queue_name)
        WorkflowRulesConsumer(conn, queue_name, engine, dlq_name=dlq_name).run()


if __name__ == "__main__":
    main()
