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
again with the resulting BarrierResult, which yields the final actions.

Phase 4 (contract 12): a retryable ``job-failed``/``job-offline`` (per ``retry_on`` / ``max_retries``)
yields a ``RetryAction`` instead of a halt or a barrier record; poison/malformed events and
``DeadLetter`` actions go to ``WORKFLOW_DLQ``; ``_finalize``/``_halt`` shorten the run's barrier TTL.
"""

import functools
import json
import logging
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin

from hysds import workflow_barrier
from hysds_commons.workflow_refs import WorkflowRefError, build_context, resolve_inputs
from hysds_commons.workflow_spec import is_retryable, retry_policy
from hysds_commons.workflow_status import WorkflowStatusNotFound, get_status
from hysds_commons.workflow_submit import (
    CONNECTOR_JOB_TYPES,
    FINALIZE_STEP,
    HALT_STEP,
    RETRY_JOB_TYPE,
    RETRY_QUEUE,
    retry_params,
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
DEFAULT_DLQ = "workflow_dead_letter"
DEFAULT_BARRIER_TERMINAL_TTL = 86400
DEFAULT_STALL_SECONDS = 600
# seconds the consumer waits for a failed job's status doc to become visible/terminal before a retry (indexer lag)
DEFAULT_RETRY_VISIBILITY_WAIT = 15
JOB_STATUS_INDEX = "job_status-current"
# job_status states after which an attempt is finished (anything else is still in flight)
TERMINAL_JOB_STATUSES = ("job-completed", "job-failed", "job-offline", "job-revoked", "job-deduped")


# ----------------------------------------------------------------------------- actions
@dataclass
class Submit:
    job_type: str
    queue: str
    params: dict
    workflow: dict
    # replayed event (contract 12.8): a held claim may be stale; the shell checks its age
    stale_ok: bool = False

    def summary(self):
        return f"submit {self.workflow['step_id']}({self.job_type}@{self.queue})"


@dataclass
class Ignore:
    reason: str
    loud: bool = False  # log at ERROR instead of INFO

    def summary(self):
        return f"ignore: {self.reason}"


@dataclass
class RetryAction:
    """Resubmit the failed job via mozart's ``job-lw-mozart-retry`` (contract 12.3). The retry job
    is *not* workflow-tagged; the retried job keeps its own tag (retry.py)."""
    run_id: str
    step_id: str
    child_index: Optional[int]
    job_id: str
    max_retries: int
    retry_count: int  # of the failed attempt; the retry is attempt retry_count + 1
    error_class: Optional[str]
    queue: str = RETRY_QUEUE

    @property
    def claim_key(self):
        return "wf-claim:%s:%s:%s:retry%d" % (self.run_id, self.step_id, self.child_index,
                                              self.retry_count + 1)

    @property
    def params(self):
        return retry_params(self.job_id, self.max_retries)

    def summary(self):
        return (f"retry {self.step_id}/{self.job_id} attempt {self.retry_count + 1}/{self.max_retries} "
                f"({self.error_class})")


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


Action = Any  # Submit | RetryAction | Ignore | DeadLetter | BarrierRecord


# ----------------------------------------------------------------------------- decide
PLUGGABLE_KINDS = ("pass", "scatter", "gather")


def _no_recommended_queue(job_type):
    return None


def _connector(kind, run_id, step_id, spec_id, event, node=None):
    """Connector submit. For pluggable kinds (pass/scatter/gather) the spec node's resolved
    ``connector`` / ``connector_queue`` win (contract 11.4); absent (pre-3b specs) or for
    finalize/halt, fall back to ``CONNECTOR_JOB_TYPES`` on ``workflow-connector-<kind>``."""
    node = node if (node and kind in PLUGGABLE_KINDS) else {}
    return Submit(
        job_type=node.get("connector") or CONNECTOR_JOB_TYPES[kind],
        queue=node.get("connector_queue") or f"workflow-connector-{kind}",
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
    replay_resubmit = False
    if not barrier.winner:
        # contract 12.8: replayed event, barrier already released (by an earlier event) but the gather
        # never ran -> fall through and submit it again under the existing claim
        if not (event.get("replayed") and barrier.released):
            return [Ignore(f"barrier {step} not released ({counts})")]
        replay_resubmit = True
    sid, snode = _scatter_for_child(nodes, step)
    if snode is None or len(snode.get("next") or []) != 1:
        return [Ignore(f"barrier {step} released ({counts}) but no scatter with exactly one successor; "
                       "NOT submitting", loud=True)]
    gather = snode["next"][0]
    threshold = (snode.get("completion") or {}).get("threshold", 1.0)
    n, k = barrier.expected, barrier.completed
    if n > 0 and k / n >= threshold:
        ok_states = ("pending", "queued") if replay_resubmit else ("pending",)
        if _state(status, gather) not in ok_states:
            return [Ignore(f"pre-check: {gather} is {_state(status, gather)!r}, not pending "
                           f"({'replayed, barrier already released' if replay_resubmit else 'barrier winner'}, "
                           f"{counts})")]
        return [_connector("gather", run_id, gather, spec_id, _barrier_event(event, spec_id, barrier),
                          node=nodes.get(gather))]
    return [_halt(run_id, spec_id, _synthetic_failure(
        event, step, f"scatter {step}: {k}/{n} completed < threshold {threshold}"))]


def _default_retry_job_type():
    return RETRY_JOB_TYPE


def _retry_action(event, node, spec, step):
    """RetryAction if this failed attempt is retryable under the node's policy, else None."""
    if node.get("kind") != "job" or event.get("synthetic") or not event.get("job_id"):
        return None
    policy = retry_policy(spec, step)
    retry_count = event.get("retry_count")
    retry_count = retry_count if isinstance(retry_count, int) and not isinstance(retry_count, bool) else 0
    if not is_retryable(policy, event.get("error_class"), retry_count):
        return None
    return RetryAction(event.get("run_id"), step, event.get("child_index"), event["job_id"],
                       policy["max_retries"], retry_count, event.get("error_class"))


def decide(event, spec, status, recommended_queue=_no_recommended_queue, barrier=None):
    """``_decide`` plus contract 12.8: on a replayed event every Submit may take over a stale claim."""
    actions = _decide(event, spec, status, recommended_queue, barrier)
    if event.get("replayed"):
        for a in actions:
            if isinstance(a, Submit):
                a.stale_ok = True
    return actions


def _decide(event, spec, status, recommended_queue=_no_recommended_queue, barrier=None):
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

    # 3a. retryable failure (job nodes and scatter children): resubmit; NOT a barrier record, no halt
    if ev_status in FAILURE_STATUSES and barrier is None:
        retry = _retry_action(event, node, spec, step)
        if retry is not None:
            return [retry]

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
        # not retryable (or retries exhausted): halt (retryable failures returned above)
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
        return [_connector(tkind, run_id, target, spec_id, event, node=nodes[target])]

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
        return [_connector("pass", run_id, target, spec_id, event, node=nodes[target])]
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
        return [_connector("gather", run_id, gather, spec_id, event, node=nodes.get(gather))]
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
    """kombu consumer: ack only after actions executed. A first handler exception requeues once;
    a second one, a malformed event or a ``DeadLetter`` action publishes to ``WORKFLOW_DLQ`` and acks."""

    def __init__(self, connection, queue_name, es=None, specs_index="workflow_specs",
                 status_index="workflow_status", job_specs_index="job_specs",
                 get_status_fn=get_status, submit_fn=submit_workflow_job, prefetch=PREFETCH_COUNT,
                 redis_client=None, dlq_name=DEFAULT_DLQ, barrier_ttl=DEFAULT_BARRIER_TERMINAL_TTL,
                 dlq_publish_fn=None, retry_job_type=None, stall_seconds=DEFAULT_STALL_SECONDS,
                 retry_claim_ttl=None, job_status_index=JOB_STATUS_INDEX, job_exists_fn=None,
                 job_doc_fn=None, clock=time.time, retry_visibility_wait=0, sleep_fn=time.sleep):
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
        self.dlq_name = dlq_name
        self.barrier_ttl = barrier_ttl
        self._dlq_publish_fn = dlq_publish_fn or self._kombu_dlq_publish
        self._retry_job_type = retry_job_type
        self.stall_seconds = stall_seconds
        self.retry_claim_ttl = retry_claim_ttl if retry_claim_ttl is not None else 2 * stall_seconds
        self.job_status_index = job_status_index
        self._job_exists_fn = job_exists_fn or self._es_job_exists
        self._job_doc_fn = job_doc_fn or self._es_job_doc
        self._clock = clock
        self.retry_visibility_wait = retry_visibility_wait
        self._sleep = sleep_fn
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
        (seen in the slice 2.7 E2E: a doubled job-failed event submitted _halt twice).
        The value is the claim's creation time (epoch seconds), used for staleness (12.8)."""
        return bool(self.redis.set(self.claim_key(workflow), repr(self._clock()), nx=True,
                                   ex=CLAIM_TTL_SECONDS))

    def release(self, workflow):
        self.redis.delete(self.claim_key(workflow))

    def claim_age(self, key):
        """Seconds since the claim was created, or None if it is gone. Legacy value ('1') -> derived
        from the TTL (age = full TTL - remaining TTL)."""
        raw = self.redis.get(key)
        if raw is None:
            return None
        try:
            created = float(raw)
        except (TypeError, ValueError):
            created = 0.0
        if created > 1e8:  # an epoch timestamp (legacy claims hold "1")
            return max(0.0, self._clock() - created)
        else:
            ttl = self.redis.ttl(key)
            return float(CLAIM_TTL_SECONDS - ttl) if isinstance(ttl, int) and ttl >= 0 else 0.0

    def _es_job_exists(self, run_id, step_id, child_index):
        must = [{"term": {"workflow.run_id": run_id}}, {"term": {"workflow.step_id": step_id}}]
        if child_index is not None:
            must.append({"term": {"workflow.child_index": child_index}})
        r = self.es.count(index=self.job_status_index, body={"query": {"bool": {"filter": must}}})
        return (r.get("count") or 0) > 0

    def _es_job_doc(self, event):
        """Latest job_status doc of the event's job (payload_id = doc id, else job_id), or None."""
        ident = event.get("payload_id") or event.get("job_id")
        r = self.es.search(index=self.job_status_index, body={
            "size": 1, "_source": ["status", "job.retry_count"],
            "query": {"bool": {"should": [{"ids": {"values": [ident]}}, {"term": {"job_id": ident}}],
                               "minimum_should_match": 1}},
            "sort": [{"@timestamp": {"order": "desc", "unmapped_type": "date"}}]})
        hits = (r.get("hits") or {}).get("hits") or []
        return hits[0].get("_source") if hits else None

    def take_stale_claim(self, workflow):
        """Contract 12.8 (replayed events only): the claim is held. If no job for (run, step[, child])
        exists in job_status and the claim is older than ``stall_seconds``, release it and re-claim."""
        key = self.claim_key(workflow)
        age = self.claim_age(key)
        if age is None:
            return self.claim(workflow)  # expired/released since the failed SET NX
        if age <= self.stall_seconds:
            return False
        try:
            exists = self._job_exists_fn(workflow.get("run_id"), workflow.get("step_id"),
                                         workflow.get("child_index"))
        except Exception:
            log.exception("job_status lookup failed for stale-claim check %s; keeping claim", key)
            return False
        if exists:
            return False
        log.warning("stale claim %s (age %.0fs > %ss, no job in job_status): releasing and resubmitting",
                    key, age, self.stall_seconds)
        self.release(workflow)
        return self.claim(workflow)

    def claim_retry(self, action):
        return bool(self.redis.set(action.claim_key, repr(self._clock()), nx=True,
                                   ex=int(self.retry_claim_ttl)))

    def release_retry(self, action):
        self.redis.delete(action.claim_key)

    def retry_is_stale(self, action, event):
        """Contract 12.8: the job moved on (its retry_count is beyond the event's) or is not terminal.

        The event is published when the job fails, but the job_status doc reaches OpenSearch a moment
        later (status indexer + refresh). A doc that is still missing or still non-terminal is therefore
        re-read for up to ``retry_visibility_wait`` seconds before it is judged: without this the retry
        job (which looks the job up by id) races the indexer and fails with "job not found"."""
        doc = None
        for attempt in range(int(self.retry_visibility_wait) + 1):
            doc = self._job_doc_fn(event)
            if doc:
                rc = (doc.get("job") or {}).get("retry_count") or 0
                if rc > action.retry_count:
                    return f"job retry_count {rc} > event retry_count {action.retry_count}"
                if doc.get("status") in TERMINAL_JOB_STATUSES:
                    return None
            if attempt < int(self.retry_visibility_wait):
                self._sleep(1)
        if not doc:
            return None  # never became visible: let the retry job report it
        return f"job status is {doc.get('status')!r} (non-terminal)"

    # -- dead letters (contract 12.7)
    def _kombu_dlq_publish(self, payload):
        exchange = Exchange(self.dlq_name, type="direct", durable=True)
        queue = Queue(self.dlq_name, exchange, routing_key=self.dlq_name, durable=True)
        with self.connection.clone() as conn:
            conn.Producer().publish(payload, exchange=exchange, routing_key=self.dlq_name,
                                    declare=[queue], serializer="json", retry=True)

    def dead_letter(self, event, reason):
        """Publish ``{event, reason, consumer_host, timestamp}`` to ``WORKFLOW_DLQ``."""
        try:
            json.dumps(event)
        except (TypeError, ValueError):
            event = repr(event)
        self._dlq_publish_fn({
            "event": event,
            "reason": reason,
            "consumer_host": socket.gethostname(),
            "timestamp": f"{datetime.now(timezone.utc).replace(tzinfo=None).isoformat()}Z",
        })
        log.error("workflow event dead-lettered to %s: %s", self.dlq_name, reason)

    # -- retry job type (versioned id, e.g. job-lw-mozart-retry:v6.1.2)
    def retry_job_type(self):
        if self._retry_job_type:
            return self._retry_job_type
        resolved = None
        try:
            r = self.es.search(index=self.job_specs_index, body={
                "query": {"prefix": {"id": RETRY_JOB_TYPE}}, "size": 20, "_source": ["id"]})
            ids = sorted(h["_source"]["id"] for h in r["hits"]["hits"]
                         if h["_source"].get("id", "").split(":")[0] == RETRY_JOB_TYPE)
            resolved = ids[-1] if ids else None
        except Exception:
            log.exception("could not look up the %s job_spec", RETRY_JOB_TYPE)
        if resolved:
            self._retry_job_type = resolved  # cache only a successful lookup
        return resolved or RETRY_JOB_TYPE

    def _expire_barriers(self, spec, run_id):
        """Contract 12.7: _finalize/_halt submitted -> barrier keys get the terminal TTL."""
        children = [n["child"] for n in (spec.get("nodes") or {}).values()
                    if n.get("kind") == "scatter" and n.get("child")]
        if children:
            workflow_barrier.expire_run_barriers(self.redis, run_id, children, self.barrier_ttl)

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
            if isinstance(a, DeadLetter):
                self.dead_letter(event, a.reason)
            elif isinstance(a, RetryAction):
                stale = self.retry_is_stale(a, event)
                if stale:
                    log.info("stale retry event for %s/%s ignored: %s", a.step_id, a.job_id, stale)
                    executed.append(Ignore(f"stale retry event: {stale}"))
                    continue
                if not self.claim_retry(a):
                    executed.append(Ignore(f"already claimed: {a.claim_key} (duplicate event)"))
                    continue
                try:
                    self.submit_fn(self.retry_job_type(), a.queue, a.params, None, enable_dedup=False)
                except Exception:
                    self.release_retry(a)  # let the requeued delivery retry the submit
                    raise
            elif isinstance(a, Submit):
                if not self.claim(a.workflow) and not (a.stale_ok and self.take_stale_claim(a.workflow)):
                    executed.append(Ignore(f"already claimed: {self.claim_key(a.workflow)} (duplicate event)"))
                    continue
                try:
                    self.submit_fn(a.job_type, a.queue, a.params, a.workflow)
                except Exception:
                    self.release(a.workflow)  # let the requeued delivery retry the submit
                    raise
                if a.workflow.get("step_id") in INTERNAL_STEPS and spec is not None:
                    try:
                        self._expire_barriers(spec, a.workflow.get("run_id"))
                    except Exception:  # best effort: the submit already happened
                        log.exception("failed to set barrier TTL for run %s", a.workflow.get("run_id"))
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

    @staticmethod
    def validate_event(body):
        """Return a reason string if ``body`` cannot be a workflow event, else None."""
        if not isinstance(body, dict):
            return f"event body is not an object: {type(body).__name__}"
        for k in ("run_id", "step_id", "status"):
            if not isinstance(body.get(k), str) or not body[k]:
                return f"missing or non-string {k!r}"
        return None

    def on_decode_error(self, message, exc):
        """A body kombu cannot decode (invalid JSON, wrong content type): kombu's default logs and
        drops it. Dead-letter the raw body instead (contract 12.7), then ack."""
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
            self.process_event(body)
        except Exception as exc:
            if message.delivery_info.get("redelivered"):
                log.exception("POISON workflow event (failed twice), dead-lettering: %r", body)
                try:
                    self.dead_letter(body, f"poison event: failed twice: {type(exc).__name__}: {exc}")
                except Exception:
                    # never lose an event: keep it on the queue if the DLQ itself is down
                    log.exception("DLQ publish failed; requeueing")
                    time.sleep(1)
                    message.requeue()
                    return
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
    dlq_name = app.conf.get("WORKFLOW_DLQ", DEFAULT_DLQ)
    barrier_ttl = int(app.conf.get("WORKFLOW_BARRIER_TERMINAL_TTL", DEFAULT_BARRIER_TERMINAL_TTL))
    retry_job_type = app.conf.get("WORKFLOW_RETRY_JOB_TYPE")
    stall_seconds = int(app.conf.get("WORKFLOW_STALL_SECONDS", DEFAULT_STALL_SECONDS))
    retry_claim_ttl = int(app.conf.get("WORKFLOW_RETRY_CLAIM_TTL", 2 * stall_seconds))
    retry_visibility_wait = int(app.conf.get("WORKFLOW_RETRY_VISIBILITY_WAIT", DEFAULT_RETRY_VISIBILITY_WAIT))
    mozart_rest_url = app.conf.MOZART_REST_URL
    # submit through mozart's REST API so workflow jobs get exactly the
    # semantics (and any policy) of a normal /job/submit
    session = requests.Session()
    submit_fn = functools.partial(submit_workflow_job, mozart_rest_url=mozart_rest_url, session=session)
    with app.connection() as conn:
        log.info("workflow_rules consuming %s, submitting via %s/job/submit", queue_name, mozart_rest_url)
        WorkflowRulesConsumer(conn, queue_name, submit_fn=submit_fn, dlq_name=dlq_name,
                              barrier_ttl=barrier_ttl, retry_job_type=retry_job_type,
                              stall_seconds=stall_seconds, retry_claim_ttl=retry_claim_ttl,
                              retry_visibility_wait=retry_visibility_wait).run()


if __name__ == "__main__":
    main()
