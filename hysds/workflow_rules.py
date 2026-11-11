"""Stateless ``workflow_rules`` consumer (workflow-orchestration v1 contract section 5).

Two layers:

* :func:`decide` -- a pure function ``(event, spec, status) -> [Action]``. No I/O.
* :class:`WorkflowRulesConsumer` -- a thin kombu shell that loads the spec and status, calls
  ``decide`` and executes the resulting actions with ``submit_workflow_job``.

The consumer never writes ``workflow_status`` (connector jobs do). Delivery is at-least-once, so
duplicate events are expected; the ``pending`` pre-check plus submit-time dedup make them harmless.

Phase 3 adds scatter/gather (contract section 10). The barrier is I/O (Redis), so ``decide`` stays
pure by running in two passes for a scatter-child event: pass 1 (``barrier=None``) returns a
``BarrierRecord`` action; the shell executes it (with the Redis-loss rebuild) and calls ``decide``
again with the resulting BarrierResult, which yields the final actions. Retry is TODO(phase4).
"""

import functools
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import requests
from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin

from hysds import workflow_barrier
from hysds_commons.workflow_refs import WorkflowRefError, build_context, resolve_inputs
from hysds_commons.workflow_status import WorkflowStatusNotFound, get_status
from hysds_commons.workflow_submit import (
    CONNECTOR_JOB_TYPES,
    FINALIZE_STEP,
    HALT_STEP,
    connector_params,
    submit_workflow_job,
    workflow_tag,
)

log = logging.getLogger("hysds.workflow_rules")

TERMINAL_RUN_STATUSES = ("completed", "failed", "cancelled")
FAILURE_STATUSES = ("job-failed", "job-offline")
INTERNAL_STEPS = (FINALIZE_STEP, HALT_STEP)
# submit-claim keys outlive any realistic run
CLAIM_TTL_SECONDS = 30 * 24 * 3600
PREFETCH_COUNT = 10


# ----------------------------------------------------------------------------- actions
@dataclass
class Submit:
    job_type: str
    queue: str
    params: dict
    workflow: dict

    def summary(self):
        return f"submit {self.workflow['step_id']}({self.job_type}@{self.queue})"


@dataclass
class Ignore:
    reason: str
    loud: bool = False  # log at ERROR instead of INFO

    def summary(self):
        return f"ignore: {self.reason}"


@dataclass
class DeadLetter:
    reason: str

    def summary(self):
        return f"dead-letter: {self.reason}"


@dataclass
class BarrierRecord:
    """Shell must record this child event in the barrier, then re-run ``decide`` with the result."""
    run_id: str
    step_id: str
    job_id: Optional[str]
    status: str
    child_total: Optional[int]

    def summary(self):
        return f"barrier-record {self.step_id}/{self.job_id}={self.status}"


Action = Any  # Submit | Ignore | DeadLetter | BarrierRecord


# ----------------------------------------------------------------------------- decide
def _no_recommended_queue(job_type):
    return None


def _connector(kind, run_id, step_id, spec_id, event):
    return Submit(
        job_type=CONNECTOR_JOB_TYPES[kind],
        queue=f"workflow-connector-{kind}",
        params=connector_params(run_id, step_id, spec_id, event),
        workflow=workflow_tag(run_id, step_id, spec_id),
    )


def _halt(run_id, spec_id, event):
    return _connector("halt", run_id, HALT_STEP, spec_id, event)


def _state(status, node_id):
    return ((status.get("nodes") or {}).get(node_id) or {}).get("state")


def _pending(status, node_id):
    return _state(status, node_id) == "pending"


def _scatter_for_child(nodes, child_id):
    for sid, n in nodes.items():
        if n.get("kind") == "scatter" and n.get("child") == child_id:
            return sid, n
    return None, None


def _barrier_event(event, spec_id, barrier):
    return {
        "run_id": event.get("run_id"),
        "step_id": event.get("step_id"),
        "spec_id": spec_id,
        "status": "barrier-released",
        "barrier": {"expected": barrier.expected, "completed": barrier.completed, "failed": barrier.failed},
    }


def _decide_barrier(event, spec, status, nodes, spec_id, barrier):
    """Second pass for a scatter-child event: ``barrier`` is the BarrierResult."""
    run_id, step = event.get("run_id"), event.get("step_id")
    counts = f"{barrier.completed} completed, {barrier.failed} failed of {barrier.expected}"
    if status.get("status") in TERMINAL_RUN_STATUSES:
        return [Ignore(f"run {run_id} already {status.get('status')} (child recorded: {counts})")]
    if not barrier.winner:
        return [Ignore(f"barrier {step} not released ({counts})")]
    sid, snode = _scatter_for_child(nodes, step)
    if snode is None or len(snode.get("next") or []) != 1:
        return [Ignore(f"barrier {step} released ({counts}) but no scatter with exactly one successor; "
                       "NOT submitting", loud=True)]
    gather = snode["next"][0]
    threshold = (snode.get("completion") or {}).get("threshold", 1.0)
    n, k = barrier.expected, barrier.completed
    if n > 0 and k / n >= threshold:
        if _state(status, gather) != "pending":
            return [Ignore(f"pre-check: {gather} is {_state(status, gather)!r}, not pending "
                           f"(barrier winner, {counts})")]
        return [_connector("gather", run_id, gather, spec_id, _barrier_event(event, spec_id, barrier))]
    return [_halt(run_id, spec_id, _synthetic_failure(
        event, step, f"scatter {step}: {k}/{n} completed < threshold {threshold}"))]


def decide(event, spec, status, recommended_queue=_no_recommended_queue, barrier=None):
    """Pure decision function for one WORKFLOW_QUEUE event. Returns a list of actions.

    ``recommended_queue(job_type) -> str | None`` supplies the job_spec's first recommended queue
    and ``barrier`` (a BarrierResult) is the outcome of the shell executing a prior
    ``BarrierRecord`` for this same event. Both are injected so this function stays pure.
    """
    run_id = event.get("run_id")
    step = event.get("step_id")
    ev_status = event.get("status")

    # 1. run / spec presence and terminal check
    if status is None:
        # TODO(phase4): real DLQ
        return [DeadLetter(f"run not found: {run_id}")]
    if spec is None:
        return [DeadLetter(f"spec not found for run {run_id}: {status.get('spec_id')}")]
    spec_id = status.get("spec_id") or event.get("spec_id")
    nodes = spec.get("nodes") or {}
    is_child = event.get("child_index") is not None and step in nodes and step not in INTERNAL_STEPS
    if status.get("status") in TERMINAL_RUN_STATUSES:
        if is_child and barrier is None and _child_recordable(event):
            # harmless bookkeeping: keep the barrier accurate, take no action
            return [_barrier_record(event)]
        if is_child and barrier is not None:
            return _decide_barrier(event, spec, status, nodes, spec_id, barrier)
        return [Ignore(f"run {run_id} already {status.get('status')}")]

    # 2. internal connectors: no recursion
    if step in INTERNAL_STEPS:
        if ev_status == "job-completed":
            return [Ignore(f"internal {step} completed")]
        return [Ignore(f"internal connector {step} ended {ev_status} for run {run_id}; "
                       "NOT recursing, reaper must catch the stall", loud=True)]

    if step not in nodes:
        return [DeadLetter(f"unknown step_id {step!r} for run {run_id}")]
    node = nodes[step]

    # 3. scatter child -> barrier (two passes, see module docstring)
    if event.get("child_index") is not None:
        if barrier is not None:
            return _decide_barrier(event, spec, status, nodes, spec_id, barrier)
        if not _child_recordable(event):
            return [Ignore(f"scatter-child event for {step} cannot be recorded (status="
                           f"{ev_status!r} job_id={event.get('job_id')!r} child_total="
                           f"{event.get('child_total')!r}); NOT advancing", loud=True)]
        return [_barrier_record(event)]

    # 4. failure
    if ev_status in FAILURE_STATUSES:
        # TODO(phase4): retry_on / max_retries check before halting
        return [_halt(run_id, spec_id, event)]

    if ev_status != "job-completed":
        return [Ignore(f"unhandled event status {ev_status!r} for step {step}", loud=True)]

    # 5. completed
    kind = node.get("kind")
    succ = node.get("next") or []
    if kind == "job":
        if not succ:
            return [_connector("finalize", run_id, FINALIZE_STEP, spec_id, event)]
        target = succ[0]
        tkind = nodes[target].get("kind")
        if tkind not in ("pass", "scatter"):
            return [Ignore(f"job {step} successor {target} has unsupported kind {tkind!r}", loud=True)]
        if not _pending(status, target):
            return [Ignore(f"pre-check: {target} is not pending (duplicate event?)")]
        return [_connector(tkind, run_id, target, spec_id, event)]

    if kind == "pass":
        if len(succ) != 1:
            return [Ignore(f"pass {step} must have exactly one successor", loud=True)]
        target = succ[0]
        tnode = nodes[target]
        if tnode.get("kind") != "job":
            return [Ignore(f"pass {step} successor {target} is not a job", loud=True)]
        # the pass connector marks its successor 'queued' (binds resolved) before
        # its own completion event arrives; 'queued' is still submittable.
        # Duplicate pass events re-submit and collapse onto the existing job via
        # orchestrator dedup.
        if _state(status, target) not in ("pending", "queued"):
            return [Ignore(f"pre-check: {target} is {_state(status, target)!r}, not pending/queued (duplicate event?)")]
        try:
            params = resolve_inputs(tnode, build_context(status, event))
        except WorkflowRefError as e:
            return [_halt(run_id, spec_id, _synthetic_failure(event, target, f"ref resolution error: {e}"))]
        queue = (tnode.get("queue") or (spec.get("defaults") or {}).get("queue")
                 or recommended_queue(tnode["run"]))
        if not queue:
            return [_halt(run_id, spec_id, _synthetic_failure(
                event, target, f"no queue for {target}: none on node/defaults/job_spec {tnode['run']}"))]
        return [Submit(tnode["run"], queue, params, workflow_tag(run_id, target, spec_id))]

    if kind == "gather":
        if not succ:
            return [_connector("finalize", run_id, FINALIZE_STEP, spec_id, event)]
        if len(succ) != 1 or nodes[succ[0]].get("kind") != "pass":
            return [Ignore(f"gather {step} successor must be exactly one pass; got {succ}", loud=True)]
        target = succ[0]
        if not _pending(status, target):
            return [Ignore(f"pre-check: {target} is not pending (duplicate event?)")]
        return [_connector("pass", run_id, target, spec_id, event)]
    if kind == "scatter":
        child = node.get("child")
        expected = ((status.get("nodes") or {}).get(child) or {}).get("expected")
        if expected != 0:
            return [Ignore(f"scatter {step} completed; children drive the barrier (expected={expected!r})")]
        if len(succ) != 1:
            return [Ignore(f"scatter {step} must have exactly one successor", loud=True)]
        gather = succ[0]
        if not _pending(status, gather):
            return [Ignore(f"pre-check: {gather} is not pending (duplicate event?)")]
        return [_connector("gather", run_id, gather, spec_id, event)]
    return [Ignore(f"step {step} has unsupported kind {kind!r}", loud=True)]


def _child_recordable(event):
    ct = event.get("child_total")
    return bool(event.get("status") in workflow_barrier.TERMINAL_CHILD_STATUSES and event.get("job_id")
                and isinstance(ct, int) and not isinstance(ct, bool) and ct > 0)


def _barrier_record(event):
    return BarrierRecord(event.get("run_id"), event.get("step_id"), event.get("job_id"),
                         event.get("status"), event.get("child_total"))


def _synthetic_failure(event, step_id, error):
    return {
        "run_id": event.get("run_id"),
        "spec_id": event.get("spec_id"),
        "step_id": step_id,
        "child_index": None,
        "child_total": None,
        "job_id": None,
        "payload_id": None,
        "status": "job-failed",
        "short_error": error,
        "workflow_outputs": None,
        "synthetic": True,
    }


# ----------------------------------------------------------------------------- I/O shell
class WorkflowRulesConsumer(ConsumerMixin):
    """kombu consumer: ack only after actions executed; one requeue, then poison-ack."""

    def __init__(self, connection, queue_name, es=None, specs_index="workflow_specs",
                 status_index="workflow_status", job_specs_index="job_specs",
                 get_status_fn=get_status, submit_fn=submit_workflow_job, prefetch=PREFETCH_COUNT,
                 redis_client=None):
        self.connection = connection
        self.queue_name = queue_name
        self._es = es
        self.specs_index = specs_index
        self.status_index = status_index
        self.job_specs_index = job_specs_index
        self.get_status_fn = get_status_fn
        self.submit_fn = submit_fn
        self.prefetch = prefetch
        self._spec_cache = {}
        self._queue_cache = {}
        self._redis = redis_client
        exchange = Exchange(queue_name, type="direct", durable=True)
        self.queue = Queue(queue_name, exchange, routing_key=queue_name, durable=True)

    # -- lazy ES (raw client)
    @property
    def es(self):
        if self._es is None:
            from hysds.es_util import get_mozart_es
            self._es = get_mozart_es().es
        return self._es

    def get_consumers(self, Consumer, channel):
        c = Consumer(queues=[self.queue], callbacks=[self.on_message], accept=["json"])
        c.qos(prefetch_count=self.prefetch)
        return [c]

    # -- submit claims (Redis; the consumer's only store, per design)
    @property
    def redis(self):
        if self._redis is None:
            from redis import StrictRedis

            from hysds.celery import app
            self._redis = StrictRedis.from_url(app.conf.REDIS_JOB_STATUS_URL, ssl_cert_reqs="none")
        return self._redis

    @staticmethod
    def claim_key(workflow):
        return "wf-claim:%s:%s:%s" % (workflow.get("run_id"), workflow.get("step_id"),
                                      workflow.get("child_index"))

    def claim(self, workflow):
        """Atomically claim the right to submit (run, step, child). Orchestrator dedup
        reads a lagging index and can't stop near-simultaneous duplicate submits
        (seen in the slice 2.7 E2E: a doubled job-failed event submitted _halt twice)."""
        return bool(self.redis.set(self.claim_key(workflow), "1", nx=True, ex=CLAIM_TTL_SECONDS))

    def release(self, workflow):
        self.redis.delete(self.claim_key(workflow))

    # -- loaders
    def load_spec(self, spec_id):
        if spec_id in self._spec_cache:  # specs are immutable
            return self._spec_cache[spec_id]
        r = self.es.get(index=self.specs_index, id=spec_id, ignore=404)
        if not isinstance(r, dict) or not r.get("found", False) or "_source" not in r:
            return None  # not cached: may appear later
        self._spec_cache[spec_id] = r["_source"]
        return r["_source"]

    def recommended_queue(self, job_type):
        if job_type not in self._queue_cache:
            r = self.es.get(index=self.job_specs_index, id=job_type, ignore=404)
            qs = ((r or {}).get("_source") or {}).get("recommended-queues") or []
            if not qs:
                return None
            self._queue_cache[job_type] = qs[0]
        return self._queue_cache[job_type]

    # -- processing
    def process_event(self, event):
        """Load, decide, execute. Returns the action list. Raises on transient failure."""
        run_id = event.get("run_id")
        try:
            status, _, _ = self.get_status_fn(self.es, self.status_index, run_id)
        except WorkflowStatusNotFound:
            status = None
        spec = None
        if status is not None:
            spec = self.load_spec(status.get("spec_id"))
        actions = decide(event, spec, status, self.recommended_queue)
        barrier = None
        if len(actions) == 1 and isinstance(actions[0], BarrierRecord):
            barrier = self.record_barrier(actions[0], status)
            actions = decide(event, spec, status, self.recommended_queue, barrier=barrier)
        executed = []
        for a in actions:
            if isinstance(a, Submit):
                if not self.claim(a.workflow):
                    executed.append(Ignore(f"already claimed: {self.claim_key(a.workflow)} (duplicate event)"))
                    continue
                try:
                    self.submit_fn(a.job_type, a.queue, a.params, a.workflow)
                except Exception:
                    self.release(a.workflow)  # let the requeued delivery retry the submit
                    raise
            executed.append(a)
        actions = executed
        self._log_event(event, status, actions, barrier)
        return actions

    def record_barrier(self, rec, status):
        """Execute a BarrierRecord: rebuild from job_status if Redis lost the barrier, then record."""
        rebuilt = False
        if (status.get("status") not in TERMINAL_RUN_STATUSES and _state(status, rec.step_id) == "running"
                and not workflow_barrier.barrier_exists(self.redis, rec.run_id, rec.step_id)):
            seed = workflow_barrier.rebuild_from_job_status(self.es, rec.run_id, rec.step_id)
            workflow_barrier.seed_barrier(self.redis, rec.run_id, rec.step_id, rec.child_total, seed)
            rebuilt = True
            log.warning("workflow barrier %s/%s missing in Redis; rebuilt %d children from job_status",
                        rec.run_id, rec.step_id, len(seed))
        result = workflow_barrier.record_child(self.redis, rec.run_id, rec.step_id, rec.job_id,
                                               rec.status, rec.child_total)
        result.rebuilt = rebuilt
        return result

    @staticmethod
    def _log_event(event, status, actions, barrier=None):
        level = logging.INFO
        for a in actions:
            if isinstance(a, DeadLetter) or (isinstance(a, Ignore) and a.loud):
                level = logging.ERROR
        log.log(level, "workflow_event run_id=%s step_id=%s child_index=%s status=%s run_status=%s "
                "barrier=%s actions=%s",
                event.get("run_id"), event.get("step_id"), event.get("child_index"), event.get("status"),
                (status or {}).get("status"), barrier.summary() if barrier else None,
                [a.summary() for a in actions])

    def on_message(self, body, message):
        try:
            if not isinstance(body, dict):
                raise ValueError(f"event body is not an object: {body!r}")
            self.process_event(body)
        except Exception:
            if message.delivery_info.get("redelivered"):
                # TODO(phase4): publish to a real DLQ
                log.exception("POISON workflow event (failed twice), acking and dropping: %r", body)
                message.ack()
            else:
                log.exception("workflow event handler failed, requeueing once: %r", body)
                time.sleep(1)
                message.requeue()
            return
        message.ack()  # only after actions executed


def main():
    from hysds.celery import app

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    queue_name = app.conf.WORKFLOW_QUEUE
    mozart_rest_url = app.conf.MOZART_REST_URL
    # submit through mozart's REST API so workflow jobs get exactly the
    # semantics (and any policy) of a normal /job/submit
    session = requests.Session()
    submit_fn = functools.partial(submit_workflow_job, mozart_rest_url=mozart_rest_url, session=session)
    with app.connection() as conn:
        log.info("workflow_rules consuming %s, submitting via %s/job/submit", queue_name, mozart_rest_url)
        WorkflowRulesConsumer(conn, queue_name, submit_fn=submit_fn).run()


if __name__ == "__main__":
    main()
