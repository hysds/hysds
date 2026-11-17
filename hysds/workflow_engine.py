"""Dataflow engine of workflows v2 (PLAN 6.1-6.7): readiness, join, ``when``, pull-model input
resolution, submit, retries, kill-on-failure, cancel and finalize. Redis is the live state
(:mod:`hysds.workflow_state`); nothing here reads or writes OpenSearch.

Model B, per event:

* ``run_started``: release every step with no deps.
* a job status event settles a step (``completed``; ``failed`` once its retries are used up). Settling
  is one atomic transaction that also records the step's outputs and adds it to the done set.
* a settled ``completed``/``skipped`` step is SADDed into ``join:{run}:{D}`` for each dependent D by a Lua
  script that reports each D exactly once when all its deps are settled.
* a released D: ``when`` false -> a ``step_skipped`` event (a skip is itself a settle event, no cascade);
  otherwise inputs are pulled from the recorded outputs and the job is submitted from the frozen
  definitions. Input errors fail the step (``input_resolution``).
* when the done set reaches the step count the workflow outputs are resolved and the run completes.

Exactly-once submission does not rely on the Lua release alone: queuing an attempt is a compare-and-set on
the run doc (``waiting -> queued``, or ``failed attempt -> new attempt``), done BEFORE the job is sent
(with a chosen task id), so a job can never report back for an attempt the doc does not know. Duplicate or
replayed events are state-guarded no-ops; a duplicate settle event re-runs the join so a crash between
settle and release heals.

On-error modes (PLAN 6.6, S6), chosen by the run's ``on_error``: ``kill`` ends the run at once; ``drain``
revokes queued jobs, cancels steps not started and lets running jobs finish (run ``halting``, then final);
``continue`` cancels only the transitive dependents of the failure (``upstream_failed``) and runs the
independent branches on. A halt always stops the run (``continue`` behaves like ``drain`` for it). A user
cancel is always ``kill``. Every end of a run goes through :func:`_final_doc`, the one place that decides
failed/completed from the step states, so ``halting`` -> final and the settled-set finalize agree.


Scatter (PLAN 6.5, B14): a scattered step is ONE node. When released it (optionally first runs the mission's
``hysds:ScatterConnector`` job, whose ``items`` output is the list) reads the item list, records N and submits
one child job per item (workflow tag ``child_index``/``child_total``). Each child's state, outputs and the
barrier counters live in Redis (``wf:sc*`` keys, one Lua script per transition): a child settling touches no
run doc, so a big scatter does not flood the snapshot queue; the doc gets counters at most
``WORKFLOW_SCATTER_MAX_SNAPSHOTS`` (default 20) times plus the fan-out and the final settle. Retries are per
child. Individual child failures never fail the run: when every child has settled only the threshold decides
(completed fraction >= threshold: the step completes with index-aligned output arrays, ``null`` at failed
indices; else it fails with ``scatter_threshold``). Gather is done here, from Redis, with no extra job.

Scatter under drain (and a halt under continue): the run is ``halting``. The scatter step is NOT cancelled while
it is in its children phase. Its queued children (and those between retries, or not yet submitted) are revoked and
settled as failed children with error class ``cancelled`` (:meth:`Engine._drain_scatters`, run after every state
change that makes a run halting); its running children finish and keep their outputs, and no child retries. The
step then settles by the normal threshold rule, so cancelled children count as failed ones. Under kill and user
cancel the scatter step is cancelled and all its live children are revoked (``_live_scatter_children``).

Run-level recovery is the reaper's (S8), which reads ``wf:active``.
"""

import copy
import json
import logging
import math
import uuid

from hysds import workflow_state as ws
from hysds_commons.workflow_refs import WorkflowRefError, outputs_to_dict
from hysds_commons.workflow_refs2 import evaluate_when, resolve_outputs, resolve_step_inputs
from hysds_commons.workflow_run2 import (
    TERMINAL_STEP_STATES,
    end_attempt,
    mark_scatter_connector,
    mark_scatter_fanout,
    mark_step_cancelled,
    mark_step_completed,
    mark_step_failed,
    mark_step_halted,
    mark_step_queued,
    mark_step_running,
    mark_step_skipped,
    set_run_outputs,
    set_run_status,
    short_error,
    unsettled_steps,
    update_scatter,
)
from hysds_commons.workflow_spec import is_retryable
from hysds_commons.workflow_spec2 import dependents_index, retry_policy2, step_queue

log = logging.getLogger("hysds.workflow_engine")

FAILURE_STATUSES = ("job-failed", "job-offline", "job-revoked", "job-deduped")
DEFAULT_ERROR_CLASS = {"job-failed": "worker", "job-offline": "offline", "job-revoked": "revoked",
                       "job-deduped": "deduped"}
ERROR_INPUT_RESOLUTION = "input_resolution"
ERROR_OUTPUT_RESOLUTION = "output_resolution"
ERROR_NOT_IMPLEMENTED = "not_implemented"
ERROR_SUBMIT = "submit"
ERROR_OUTPUTS = "outputs"
ERROR_HALTED = "halted"
ERROR_SCATTER_THRESHOLD = "scatter_threshold"
SCATTER_ITEMS_OUTPUT = "items"  # the output a hysds:ScatterConnector job writes: the list to fan out over
DEFAULT_SCATTER_MAX_SNAPSHOTS = 20  # WORKFLOW_SCATTER_MAX_SNAPSHOTS: progress snapshots per scatter step
DEFAULT_SCATTER_DOC_CHILDREN_MAX = 500  # WORKFLOW_SCATTER_DOC_CHILDREN_MAX: child job ids kept in the run doc
SCATTER_CANCEL_CHECK = 50  # re-read the run status every this many child submits
ACTIVE_ATTEMPT = ("queued", "running")
LIVE_RUN_STATUSES = ("running", "halting")  # the run still takes job events


class EventRejected(Exception):
    """The event cannot be handled (unknown run/step, malformed): the shell dead-letters it."""


def _is_active_attempt(att):
    return att["status"] in ACTIVE_ATTEMPT


def _cancel_rest(doc, kind, by_step, reason, keep_running=False):
    """Cancel every non-settled step of ``doc``: the active attempt ends ``cancelled`` and the step
    ``cancelled``. ``keep_running`` (drain) spares the steps whose job is running (their attempt is
    ``running``); a queued job, or a step between retries, is still cancelled.
    Returns ``(doc, payload_ids_to_revoke, cancelled_steps)``."""
    revoke, cancelled = [], []
    for sid in sorted(doc["steps"]):
        st = doc["steps"][sid]
        if st["state"] in TERMINAL_STEP_STATES:
            continue
        if keep_running and st["attempts"] and st["attempts"][-1]["status"] == "running":
            continue
        if keep_running and _scatter_in_children(st):  # its children settle it (see Engine._drain_scatters)
            continue
        if st["attempts"] and _is_active_attempt(st["attempts"][-1]):
            revoke.append(st["attempts"][-1]["job_id"])
            doc = end_attempt(doc, sid, "cancelled")
        doc = mark_step_cancelled(doc, sid, kind, by_step=by_step, reason=reason)
        cancelled.append(sid)
    return doc, revoke, cancelled


def _scatter_in_children(st):
    """True for a scatter step that has fanned out and has not settled (its children are live in Redis)."""
    return st["state"] == "running" and (st.get("scatter") or {}).get("phase") == "children"


def _descendants(spec, sid):
    """Every step that transitively depends on ``sid``."""
    idx, seen, todo = dependents_index(spec), set(), [sid]
    while todo:
        for d in idx.get(todo.pop(), []):
            if d not in seen:
                seen.add(d)
                todo.append(d)
    return sorted(seen)


def _note_failure(doc, sid, error, error_class):
    """Record the FIRST failure on the run doc (``failed_step``/``error_class``/``reason``) without changing
    its status, so a ``halting`` or ``running`` run already shows why it will fail."""
    if doc.get("failed_step") or doc.get("error_class"):
        return doc
    return set_run_status(doc, doc["status"], error_class=error_class, reason=short_error(error), failed_step=sid)


def _final_doc(doc, spec, settled):
    """The terminal doc of a run whose steps have all settled (or that a kill/halt just ended). Pure. The
    outcome comes from the step states, in this order: a failed step -> ``failed`` (a failure outranks a
    halt that would have completed the run); a fired halt with status ``failed`` -> ``failed``/``halted``;
    a fired halt with status ``completed`` -> ``completed`` with the halt's reason; otherwise ``completed``.
    Workflow outputs are resolved and type checked only for the completed outcomes (PLAN 6.7, B21): under a
    completed halt a step that never produced a result reads as null, so an optional output is null and a
    required one fails the run with ``output_resolution``."""
    steps = doc["steps"]
    failed = sorted((sid for sid, st in steps.items() if st["state"] == "failed"),
                    key=lambda i: (steps[i].get("ended_at") or "", i))
    if failed:
        sid = doc.get("failed_step") if doc.get("failed_step") in failed else failed[0]
        st = steps[sid]
        return set_run_status(doc, "failed", error_class=st.get("error_class") or doc.get("error_class") or "worker",
                              reason=st.get("error") or doc.get("reason"), failed_step=sid)
    fired = [(sid, st["halt"]) for sid, st in sorted(steps.items()) if st.get("halt", {}).get("fired")]
    bad = [(sid, h) for sid, h in fired if h["status"] != "completed"]
    if bad:
        sid, h = bad[0]
        return set_run_status(doc, "failed", error_class=ERROR_HALTED, reason=h.get("reason") or "halted",
                              failed_step=sid)
    completed_halt = [h for _, h in fired]
    try:
        outs = resolve_outputs(spec, complete_settled(settled, doc) if completed_halt else settled, doc["inputs"])
    except WorkflowRefError as e:
        return set_run_status(doc, "failed", error_class=ERROR_OUTPUT_RESOLUTION, reason=short_error(e),
                              failed_step=(fired[0][0] if fired else None))
    reason = completed_halt[0].get("reason") if completed_halt else None
    return set_run_status(set_run_outputs(doc, outs), "completed", reason=reason)


def _end_or_halting(doc, spec, settled_fn, error_class=None, reason=None, failed_step=None):
    """After the steps that need not run were cancelled: if jobs are still running the run is ``halting``
    (drain), otherwise it ends now."""
    if unsettled_steps(doc):
        return set_run_status(doc, "halting", error_class=error_class, reason=reason, failed_step=failed_step)
    return _final_doc(doc, spec, settled_fn())


def _fail_step(doc, sid, error, error_class):
    st = doc["steps"][sid]
    if st["attempts"] and _is_active_attempt(st["attempts"][-1]):
        doc = end_attempt(doc, sid, "failed", error=error)
    return mark_step_failed(doc, sid, error=error, error_class=error_class)


def _fail_kill(doc, sid, error, error_class, spec=None, settled_fn=None):
    """``kill`` (the default): the step fails permanently, the run fails, every other queued/running job
    is revoked and every other unsettled step is cancelled. Pure.
    Returns ``(doc, revoke_ids, cancelled_steps)``."""
    doc = _fail_step(doc, sid, error, error_class)
    doc, revoke, cancelled = _cancel_rest(doc, "failure", sid, error_class)
    doc = set_run_status(doc, "failed", error_class=error_class, reason=short_error(error), failed_step=sid)
    return doc, revoke, cancelled


def _fail_drain(doc, sid, error, error_class, spec=None, settled_fn=None):
    """``drain``: queued jobs are revoked and steps not started are cancelled; running jobs finish and keep
    their outputs. The run is ``halting`` until they settle, then ``failed`` (via :func:`_final_doc`)."""
    doc = _note_failure(_fail_step(doc, sid, error, error_class), sid, error, error_class)
    doc, revoke, cancelled = _cancel_rest(doc, "failure", sid, error_class, keep_running=True)
    return _end_or_halting(doc, spec, settled_fn, error_class, short_error(error), sid), revoke, cancelled


def _fail_continue(doc, sid, error, error_class, spec=None, settled_fn=None):
    """``continue``: only the transitive dependents of the failure are cancelled (``upstream_failed``; a
    failed step never reads as null downstream). Nothing is revoked; the run stays ``running`` until every
    step settled, then it is ``failed``."""
    doc = _note_failure(_fail_step(doc, sid, error, error_class), sid, error, error_class)
    cancelled = []
    for d in _descendants(spec, sid):
        if doc["steps"][d]["state"] not in TERMINAL_STEP_STATES:
            doc = mark_step_cancelled(doc, d, "upstream_failed", by_step=sid, reason=error_class)
            cancelled.append(d)
    if not unsettled_steps(doc):
        doc = _final_doc(doc, spec, settled_fn())
    return doc, [], cancelled


def _fail_halting(doc, sid, error, error_class, spec=None, settled_fn=None):
    """A job that was left running by a drain/halt failed: record it (no retry, nothing else to cancel); the
    run ends once the last running job settles and is ``failed`` whatever the halt's status was."""
    doc = _note_failure(_fail_step(doc, sid, error, error_class), sid, error, error_class)
    if not unsettled_steps(doc):
        doc = _final_doc(doc, spec, settled_fn())
    return doc, [], []


FAILURE_HANDLERS = {"kill": _fail_kill, "drain": _fail_drain, "continue": _fail_continue}


def apply_failure(doc, sid, error, error_class, spec=None, settled_fn=None):
    """Step ``sid`` failed permanently: apply the run's ``on_error`` mode. Pure. ``settled_fn()`` returns the
    recorded step results (only read when the run ends). Returns ``(doc, payload_ids_to_revoke, cancelled)``."""
    if doc["status"] == "halting":
        handler = _fail_halting
    else:
        mode = doc.get("on_error", "kill")
        handler = FAILURE_HANDLERS.get(mode)
        if handler is None:
            log.warning("run %s: unknown on_error %r; using kill", doc["run_id"], mode)
            handler = FAILURE_HANDLERS["kill"]
    return handler(doc, sid, error, error_class, spec=spec, settled_fn=settled_fn)


def complete_settled(settled, doc):
    """``settled`` with every step that has no recorded result read as skipped (null outputs); used when a
    halt ends the run before every step settled."""
    out = dict(settled)
    for sid in doc["steps"]:
        out.setdefault(sid, {"state": "skipped", "outputs": {}})
    return out


class Engine:
    """Executes events against Redis. ``submit_fn(job_json, payload_id)`` sends a job to the orchestrator;
    ``publish_fn(event)`` queues an engine event; ``revoke_fn(run_id, payload_ids, username)`` revokes jobs
    (failures are logged, never raised: the state change is already committed)."""

    def __init__(self, redis_client, submit_fn, publish_fn=None, revoke_fn=None, new_id=None, build_job_fn=None,
                 list_key=None, priority=5):
        self.r = redis_client
        self.submit_fn = submit_fn
        self.publish_fn = publish_fn
        self.revoke_fn = revoke_fn
        self.new_id = new_id or (lambda: str(uuid.uuid4()))
        self.build_job_fn = build_job_fn
        self.list_key = list_key
        self.priority = priority

    # ------------------------------------------------------------------ loading
    def _load(self, run_id):
        doc = ws.get_run(self.r, run_id)
        if doc is None:
            raise EventRejected("run not found: %s" % run_id)
        spec = ws.get_spec(self.r, run_id)
        if spec is None:
            raise EventRejected("spec not found for run %s" % run_id)
        return doc, spec

    def _update(self, run_id, fn):
        return ws.update_run(self.r, run_id, fn, list_key=self.list_key)

    # ------------------------------------------------------------------ dispatch
    def handle_event(self, event):
        """Process one queue event. Returns a short description (for the log). Raises on a transient
        failure (the shell requeues once) or :class:`EventRejected` (dead letter)."""
        kind = event.get("kind")
        if kind is None:
            return self._job_event(event)
        handler = {"run_started": self._run_started, "cancel_requested": self._cancel,
                   "step_skipped": self._step_skipped}.get(kind)
        if handler is None:
            raise EventRejected("unknown event kind %r" % (kind,))
        return handler(event)

    # ------------------------------------------------------------------ run_started
    def _run_started(self, event):
        run_id = event["run_id"]
        doc, spec = self._load(run_id)
        if doc["status"] != "running":
            return "run_started ignored: run is %s" % doc["status"]
        roots = sorted(s for s, step in spec["steps"].items() if not step.get("deps"))
        for sid in roots:
            self._release(run_id, sid)
        return "run_started: released %s" % roots

    # ------------------------------------------------------------------ release
    def _release(self, run_id, sid):
        """D's deps are all settled: evaluate ``when``, then skip it, fail it or submit it."""
        doc, spec = self._load(run_id)
        st = doc["steps"][sid]
        if doc["status"] == "running" and st["state"] == "running" and (st.get("scatter") or {}).get("phase") == "children":
            return self._fanout(run_id, sid)  # a replayed release finishes a fan-out a crash interrupted
        if doc["status"] != "running" or st["state"] != "waiting":
            return "release %s ignored" % sid
        step = spec["steps"][sid]
        settled = ws.settled_map(self.r, run_id)
        if step.get("when"):
            try:
                flag = evaluate_when(step["when"], {}, settled, doc["inputs"])
            except WorkflowRefError as e:
                return self._fail_waiting(run_id, sid, "when: %s" % e, ERROR_INPUT_RESOLUTION)
            if not flag:
                self._publish({"kind": "step_skipped", "run_id": run_id, "step_id": sid,
                               "flag": step["when"], "value": False})
                return "release %s: when false, skip event published" % sid
        if "halt" in step:
            return self._fire_halt(run_id, sid, settled)
        try:
            params = resolve_step_inputs(step, settled, doc["inputs"])
        except WorkflowRefError as e:
            return self._fail_waiting(run_id, sid, str(e), ERROR_INPUT_RESOLUTION)
        if "scatter" in step:
            if step["scatter"].get("connector"):  # the mission lists the items first (hysds:ScatterConnector)
                return self._queue_and_submit(run_id, sid, params, connector=True)
            items = params.get(step["scatter"]["input"])
            if not isinstance(items, list):  # B20: a null (or non-list) scatter input is a run-time error
                return self._fail_waiting(run_id, sid, "scatter input '%s' must be a list, got %s" % (
                    step["scatter"]["input"], "null" if items is None else type(items).__name__), ERROR_INPUT_RESOLUTION)
            return self._begin_fanout(run_id, sid, items)
        return self._queue_and_submit(run_id, sid, params)

    def _publish(self, event):
        if self.publish_fn is not None:
            self.publish_fn(event)
        else:
            ws.publish_run_event(event["run_id"], event["kind"],
                                 {k: v for k, v in event.items() if k not in ("kind", "run_id")})

    # ------------------------------------------------------------------ submit
    def _build_job(self, doc, spec, sid, params, child_index=None, child_total=None, connector=False):
        """The job JSON of a step; of one scatter child (``child_index``/``child_total`` in its workflow tag);
        or of the step's scatter connector job (``connector=True``: its own job type and queue)."""
        step = spec["steps"][sid]
        if connector:
            jt = step["scatter"]["connector"]["job_type"]
            definition = (doc.get("job_definitions") or {}).get(jt) or {}
            queue = step["scatter"]["connector"].get("queue") or definition.get("queue")
        else:
            jt = step["job_type"]
            definition = (doc.get("job_definitions") or {}).get(jt) or {}
            queue = step_queue(spec, sid, definition.get("queue"))
        workflow = {"run_id": doc["run_id"], "step_id": sid, "spec_id": doc["spec_id"],
                    "child_index": child_index, "child_total": child_total}
        tl = None if connector else step.get("time_limit")
        kwargs = dict(workflow=workflow, tags=[step.get("tag") or sid], priority=self.priority,
                      time_limit=tl if tl else None, enable_dedup=False)
        if self.build_job_fn is not None:
            return self.build_job_fn(definition, jt, queue, params, **kwargs)
        from hysds_commons.workflow_job2 import resolve_frozen_job
        return resolve_frozen_job(definition, jt, queue, params, **kwargs)

    def _queue_and_submit(self, run_id, sid, params, retry_of=None, connector=False):
        """Record the attempt (compare-and-set), then send the job. ``retry_of`` is the number of attempts
        the step had when the retry was decided (a concurrent duplicate loses the race). ``connector``: the
        job is the step's scatter connector (the step's attempts are the connector's)."""
        doc, spec = self._load(run_id)
        if connector:
            conn = spec["steps"][sid]["scatter"]["connector"]
            has_queue = conn.get("queue") or (doc.get("job_definitions") or {}).get(conn["job_type"], {}).get("queue")
        else:
            has_queue = spec["steps"][sid].get("queue") or (doc.get("job_definitions") or {}).get(
                spec["steps"][sid].get("job_type"), {}).get("queue")
        if not has_queue:
            return self._fail_waiting(run_id, sid, "no queue for step %s" % sid, "no_queue", retry_of=retry_of)
        try:
            job = self._build_job(doc, spec, sid, params, connector=connector)
        except Exception as e:  # FrozenJobError: a required param is missing / bad frozen definition
            return self._fail_waiting(run_id, sid, "cannot build job: %s" % e, ERROR_SUBMIT, retry_of=retry_of)
        if retry_of:
            job["retry_count"] = retry_of  # as lw-mozart-retry does: the PGE reads it from _job.json
        payload_id = self.new_id()

        def fn(d):
            st = d["steps"][sid]
            if d["status"] != "running" or st["state"] in TERMINAL_STEP_STATES:
                return None
            if retry_of is None:
                if st["state"] != "waiting":
                    return None
            elif len(st["attempts"]) != retry_of or st["attempts"][-1]["status"] != "failed":
                return None
            if connector:
                return mark_scatter_connector(d, sid, payload_id), payload_id
            return mark_step_queued(d, sid, payload_id), payload_id

        if self._update(run_id, fn) is None:
            return "submit %s skipped: already queued or run over" % sid
        try:
            self.submit_fn(job, payload_id)
        except Exception:
            self._unqueue(run_id, sid, payload_id)
            raise
        return "submitted %s as %s" % (sid, payload_id)

    def _unqueue(self, run_id, sid, payload_id):
        """The send failed: forget the attempt so a redelivered event can queue it again."""
        def fn(d):
            st = d["steps"][sid]
            if not st["attempts"] or st["attempts"][-1]["job_id"] != payload_id or st["state"] in TERMINAL_STEP_STATES:
                return None
            new = copy.deepcopy(d)
            nst = new["steps"][sid]
            nst["attempts"].pop()
            if not nst["attempts"]:
                nst["state"], nst["job_id"] = "waiting", None
            else:
                nst["job_id"] = nst["attempts"][-1]["job_id"]
            new["version"] = d["version"] + 1
            return new, None
        try:
            self._update(run_id, fn)
        except Exception:  # noqa: BLE001
            log.exception("run %s step %s: could not undo the attempt %s", run_id, sid, payload_id)

    # ------------------------------------------------------------------ failing / halting
    def _fail_waiting(self, run_id, sid, error, error_class, retry_of=None):
        """A step that was never submitted (or cannot be re-submitted) fails permanently."""
        def fn(d):
            st = d["steps"][sid]
            if d["status"] != "running" or st["state"] in TERMINAL_STEP_STATES:
                return None
            new, revoke, cancelled = apply_failure(d, sid, error, error_class, self._spec_of(run_id),
                                                   lambda: ws.settled_map(self.r, run_id))

            def extra(pipe):
                ws.record_done(pipe, run_id, sid)
                for c in cancelled:
                    ws.record_done(pipe, run_id, c)
            return new, revoke, extra
        res = self._update(run_id, fn)
        if res is None:
            return "fail %s ignored" % sid
        self._revoke(run_id, res[1], None)
        return "step %s failed (%s): %s" % (sid, error_class, error)

    def _fire_halt(self, run_id, sid, settled=None):
        """A halt step's ``when`` held: it is never submitted, its settle IS the trigger (B19: a halt always
        stops the run). ``kill`` ends the run at once; ``drain`` and ``continue`` (which behaves like drain
        for a halt) revoke queued jobs, cancel steps not started and let running jobs finish: the run is
        ``halting`` until they settle. The outcome (the halt's status) is decided by :func:`_final_doc`."""
        def fn(d):
            if d["status"] != "running" or d["steps"][sid]["state"] != "waiting":
                return None
            h = d["steps"][sid]["halt"]
            spec = self._spec_of(run_id)
            settled_fn = lambda: ws.settled_map(self.r, run_id)  # noqa: E731
            new = mark_step_halted(d, sid)
            if d.get("on_error", "kill") == "kill":
                new, revoke, cancelled = _cancel_rest(new, "halt", sid, h.get("reason"))
                new = _final_doc(new, spec, settled_fn())
            else:
                new, revoke, cancelled = _cancel_rest(new, "halt", sid, h.get("reason"), keep_running=True)
                failing = h["status"] != "completed"
                new = _end_or_halting(new, spec, settled_fn, ERROR_HALTED if failing else None,
                                      h.get("reason") or ("halted" if failing else None), sid if failing else None)

            def extra(pipe):
                ws.record_settle(pipe, run_id, sid, "completed")
                for c in cancelled:
                    ws.record_done(pipe, run_id, c)
            return new, revoke, extra
        res = self._update(run_id, fn)
        if res is None:
            return "halt %s ignored" % sid
        self._revoke(run_id, res[1], None)
        return "halt %s fired: run %s" % (sid, res[0]["status"])

    def _spec_of(self, run_id):
        return ws.get_spec(self.r, run_id)

    def finish_halting(self, run_id):
        """The reaper's recovery of a ``halting`` run whose running jobs have all settled (the event that would
        have ended it was lost): cancel what is still unsettled and end the run through :func:`_final_doc`, the
        same decision every other end of a run makes (a halt that completes ends ``completed``; a failure ends
        ``failed``). A run with a scatter step still in its children phase is left to its children. Returns
        True if it acted."""
        def fn(d):
            if d["status"] != "halting" or any(_is_active_attempt(s["attempts"][-1]) for s in d["steps"].values()
                                               if s["attempts"]) \
                    or any(_scatter_in_children(s) for s in d["steps"].values()):
                return None
            new, revoke, cancelled = _cancel_rest(d, "halt", d.get("failed_step"), d.get("reason"))
            new = _final_doc(new, self._spec_of(run_id), ws.settled_map(self.r, run_id))
            return new, revoke, lambda pipe: [ws.record_done(pipe, run_id, c) for c in cancelled]
        return self._update(run_id, fn) is not None

    def _live_scatter_children(self, run_id):
        """Job ids of the queued/running children of every scatter step the run doc now says is cancelled or failed
        (children are tracked in Redis, so the doc-pure cancel helpers cannot list them)."""
        doc, spec = ws.get_run(self.r, run_id), ws.get_spec(self.r, run_id)
        ids = []
        for sid, step in ((spec or {}).get("steps") or {}).items():
            if "scatter" in step and doc and doc["steps"][sid]["state"] in ("cancelled", "failed"):
                ids += [c["job"] for _, c in sorted(ws.scatter_children(self.r, run_id, sid).items())
                        if c["state"] in ws.CHILD_ACTIVE_STATES]
        return ids

    def _drain_scatters(self, run_id):
        """A ``halting`` run (drain, or a halt under drain/continue) keeps its running scatter steps: the children
        still ``queued`` (or between retries, or not yet submitted when the halt came) are cancelled now, each
        settled as a failed child with error class ``cancelled``; the children already running finish and keep
        their outputs, and nothing retries. Once every child has settled the step is settled by the ordinary
        threshold rule (so cancelled children count as failures: a step that cannot meet its threshold fails
        ``scatter_threshold``, and that failure outranks a completed halt). Idempotent. Returns the job ids of
        the queued children, which the caller revokes."""
        doc = ws.get_run(self.r, run_id)
        if not doc or doc["status"] != "halting":
            return []
        revoke = []
        result = json.dumps({"ok": False, "error": "cancelled: the run is halting", "error_class": "cancelled"})
        for sid, st in sorted(doc["steps"].items()):
            if not _scatter_in_children(st):
                continue
            n = ws.scatter_counts(self.r, run_id, sid)["n"]
            recs = ws.scatter_children(self.r, run_id, sid)
            last = None
            for i in range(n):
                rec = recs.get(i)
                if rec is None:  # the fan-out was interrupted before this child: it never starts
                    job = self.new_id()
                    if not ws.scatter_op(self.r, run_id, sid, "claim", i, job)[0]:
                        continue
                elif rec["state"] == "queued":
                    job = rec["job"]
                    revoke.append(job)
                elif rec["state"] == "retry_wait":
                    job = rec["job"]
                else:
                    continue
                ok, c, f, n_ = ws.scatter_op(self.r, run_id, sid, "settle", i, job, "failed", result)
                last = (c, f, n_) if ok else last
            if last:
                self._scatter_after(run_id, sid, *last, changed=True)
        return revoke

    def _revoke(self, run_id, payload_ids, username):
        try:
            payload_ids = list(payload_ids or []) + self._live_scatter_children(run_id) + self._drain_scatters(run_id)
            if not payload_ids or self.revoke_fn is None:
                return
            self.revoke_fn(run_id, list(dict.fromkeys(payload_ids)), username)
        except Exception:  # noqa: BLE001  (state is committed; the reaper sweeps leftovers)
            log.exception("run %s: could not revoke %s", run_id, payload_ids)

    # ------------------------------------------------------------------ settle -> join -> finalize
    def _after_settle(self, run_id, sid, replay=False):
        doc, spec = self._load(run_id)
        notes = []
        if doc["status"] == "running":
            deps = dependents_index(spec)[sid]
            for d in ws.join(self.r, run_id, spec, sid, deps, replay=replay):
                notes.append(self._release(run_id, d))
        if ws.n_done(self.r, run_id) >= len(spec["steps"]):
            notes.append(self._finalize(run_id))
        return "; ".join(n for n in notes if n)

    def _finalize(self, run_id):
        """Every step settled (the done set reached the step count): end the run, from ``running`` or from
        ``halting`` (drain: the last running job settled). The outcome comes from :func:`_final_doc`."""
        def fn(d):
            if d["status"] not in LIVE_RUN_STATUSES or unsettled_steps(d):
                return None
            new = _final_doc(d, self._spec_of(run_id), ws.settled_map(self.r, run_id))
            return new, new["status"]
        res = self._update(run_id, fn)
        return "finalize: run %s" % res[1] if res else None

    # ------------------------------------------------------------------ step_skipped
    def _step_skipped(self, event):
        run_id, sid = event["run_id"], event.get("step_id")
        doc, spec = self._load(run_id)
        if sid not in spec["steps"]:
            raise EventRejected("unknown step %r" % (sid,))
        if doc["steps"][sid]["state"] == "skipped":
            return "step_skipped %s duplicate: %s" % (sid, self._after_settle(run_id, sid, replay=True))
        if doc["status"] != "running":
            return "step_skipped ignored: run is %s" % doc["status"]

        def fn(d):
            if d["status"] != "running" or d["steps"][sid]["state"] != "waiting":
                return None
            new = mark_step_skipped(d, sid, event.get("flag"), event.get("value"))
            return new, None, lambda pipe: ws.record_settle(pipe, run_id, sid, "skipped")
        if self._update(run_id, fn) is None:
            return "step_skipped %s ignored" % sid
        return "skipped %s: %s" % (sid, self._after_settle(run_id, sid))

    # ------------------------------------------------------------------ cancel
    def _cancel(self, event):
        run_id, user = event["run_id"], event.get("username")
        doc, _ = self._load(run_id)
        if doc["status"] in ws.TERMINAL_RUN_STATUSES:
            return "cancel ignored: run is %s" % doc["status"]

        def fn(d):
            if d["status"] in ws.TERMINAL_RUN_STATUSES:
                return None
            why = event.get("reason") or "cancelled by %s" % user  # the reaper passes "run timed out ..."
            new, revoke, cancelled = _cancel_rest(d, "user", None, why)
            new = set_run_status(new, "cancelled", reason=why, cancelled_by=user or "unknown")

            def extra(pipe):
                for c in cancelled:
                    ws.record_done(pipe, run_id, c)
            return new, revoke, extra
        res = self._update(run_id, fn)
        if res is None:
            return "cancel ignored"
        self._revoke(run_id, res[1], user)
        return "run %s cancelled by %s; revoked %d job(s)" % (run_id, user, len(res[1]))

    # ------------------------------------------------------------------ job status events
    def _job_event(self, event):
        run_id, sid, status = event.get("run_id"), event.get("step_id"), event.get("status")
        doc, spec = self._load(run_id)
        if sid not in spec["steps"]:
            raise EventRejected("unknown step_id %r for run %s" % (sid, run_id))
        self._note_task(run_id, event)
        if event.get("child_index") is not None:
            return self._child_event(run_id, spec, doc, sid, event)
        st = doc["steps"][sid]
        ids = {i for i in (event.get("payload_id"), event.get("job_id")) if i}
        idx = next((i for i, a in enumerate(st["attempts"]) if a["job_id"] in ids), None)

        if status == "job-completed" and st["state"] == "completed":  # duplicate: heal a crash after settle
            return "duplicate completion of %s: %s" % (sid, self._after_settle(run_id, sid, replay=True))
        if idx is not None and status in ("job-queued", "job-started") and st["attempts"][idx]["status"] == "cancelled":
            # the attempt was cancelled before its celery task id was known (or the job started anyway):
            # now that the task exists, revoke it (marker + terminate), so a cancelled job never runs on
            ids_ = [st["attempts"][idx]["job_id"]]
            self._revoke(run_id, ids_, None)
            return "late revoke of cancelled attempt %s of %s" % (ids_[0], sid)
        if doc["status"] not in LIVE_RUN_STATUSES:
            return "ignored: run %s is %s" % (run_id, doc["status"])
        if st["state"] in TERMINAL_STEP_STATES:
            return "ignored: step %s already %s" % (sid, st["state"])
        if idx is None:
            return "ignored: %s event for %s matches no attempt (stale or foreign)" % (status, sid)
        if status == "job-queued":
            return "queued %s (task id recorded)" % sid
        sc = st.get("scatter")
        if sc and sc["phase"] == "children":  # an event of the (finished) connector job: a duplicate
            return "ignored: %s of the connector of %s (already fanned out): %s" % (
                status, sid, self._fanout(run_id, sid) if status == "job-completed" else "")
        if status == "job-started":
            return self._started(run_id, sid, idx, event)
        if status == "job-completed" and sc and sc["phase"] == "connector":
            return self._connector_done(run_id, sid, idx, event)
        if status == "job-completed":
            return self._completed(run_id, sid, idx, event)
        if status in FAILURE_STATUSES:
            return self._failed(run_id, spec, sid, idx, event)
        return "ignored: status %r" % (status,)

    def _note_task(self, run_id, event):
        """Remember the celery task id of an attempt (job-queued / job-started carry it)."""
        if event.get("task_id") and event.get("payload_id"):
            ws.record_task_id(self.r, run_id, event["payload_id"], event["task_id"])

    def _started(self, run_id, sid, idx, event):
        def fn(d):
            st = d["steps"][sid]
            if d["status"] not in LIVE_RUN_STATUSES or idx != len(st["attempts"]) - 1 or st["attempts"][idx]["status"] != "queued":
                return None
            return mark_step_running(d, sid, job_id=st["attempts"][idx]["job_id"]), None
        self._update(run_id, fn)
        return "started %s" % sid

    def _completed(self, run_id, sid, idx, event):
        try:
            outputs = outputs_to_dict(event.get("workflow_outputs"))
        except WorkflowRefError as e:
            return self._failed(run_id, self._spec_of(run_id), sid, idx,
                                dict(event, status="job-failed", error_class=ERROR_OUTPUTS,
                                     short_error="invalid workflow outputs: %s" % e), retryable=False)

        def fn(d):
            st = d["steps"][sid]
            if d["status"] not in LIVE_RUN_STATUSES or st["state"] in TERMINAL_STEP_STATES \
                    or idx != len(st["attempts"]) - 1:
                return None
            new = mark_step_completed(end_attempt(d, sid, "completed"), sid)
            return new, None, lambda pipe: ws.record_settle(pipe, run_id, sid, "completed", outputs)
        if self._update(run_id, fn) is None:
            return "completion of %s ignored" % sid
        return "completed %s: %s" % (sid, self._after_settle(run_id, sid))

    def _failed(self, run_id, spec, sid, idx, event, retryable=True):
        status = event.get("status")
        ec = event.get("error_class") or DEFAULT_ERROR_CLASS.get(status, "worker")
        error = event.get("short_error") or status
        box = {}

        def fn(d):
            st = d["steps"][sid]
            if d["status"] not in LIVE_RUN_STATUSES or st["state"] in TERMINAL_STEP_STATES \
                    or idx != len(st["attempts"]) - 1:
                return None
            n = len(st["attempts"])
            active = _is_active_attempt(st["attempts"][-1])
            # a halting run (drain/halt) does not retry: its remaining jobs only settle
            if retryable and d["status"] == "running" and is_retryable(retry_policy2(spec, sid), ec, n - 1):
                box["retry_of"] = n
                return (end_attempt(d, sid, "failed", error=error), None) if active else None
            new, revoke, cancelled = apply_failure(d, sid, error, ec, spec, lambda: ws.settled_map(self.r, run_id))

            def extra(pipe):
                ws.record_done(pipe, run_id, sid)
                for c in cancelled:
                    ws.record_done(pipe, run_id, c)
            return new, revoke, extra
        res = self._update(run_id, fn)
        if "retry_of" in box:
            return self._retry(run_id, sid, box["retry_of"], ec)
        if res is None:
            return "failure of %s ignored" % sid
        self._revoke(run_id, res[1], None)
        return "step %s failed (%s); run %s" % (sid, ec, res[0]["status"])

    def _retry(self, run_id, sid, n_attempts, ec):
        doc, spec = self._load(run_id)
        settled = ws.settled_map(self.r, run_id)
        try:
            params = resolve_step_inputs(spec["steps"][sid], settled, doc["inputs"])
        except WorkflowRefError as e:
            return self._fail_waiting(run_id, sid, str(e), ERROR_INPUT_RESOLUTION)
        connector = (doc["steps"][sid].get("scatter") or {}).get("phase") == "connector"
        return "retry %s (attempt %d, after %s): %s" % (
            sid, n_attempts + 1, ec, self._queue_and_submit(run_id, sid, params, retry_of=n_attempts, connector=connector))

    # ------------------------------------------------------------------ scatter (PLAN 6.5, B14)
    @staticmethod
    def _scatter_conf(name, default):
        from hysds import workflow_snapshot
        return int(workflow_snapshot._conf(name, default))

    def _connector_done(self, run_id, sid, idx, event):
        """The scatter connector job finished: its ``items`` output is the list to fan out over."""
        try:
            items = outputs_to_dict(event.get("workflow_outputs")).get(SCATTER_ITEMS_OUTPUT)
            problem = None if isinstance(items, list) else (
                "scatter connector output '%s' must be a list, got %s" % (
                    SCATTER_ITEMS_OUTPUT, "nothing" if items is None else type(items).__name__))
        except WorkflowRefError as e:
            problem = "invalid workflow outputs: %s" % e
        if problem:  # not retried: the same job would write the same thing
            return self._failed(run_id, self._spec_of(run_id), sid, idx,
                                dict(event, status="job-failed", error_class=ERROR_OUTPUTS, short_error=problem),
                                retryable=False)
        if (ws.get_run(self.r, run_id) or {}).get("status") == "halting":
            return self._connector_halted(run_id, sid, idx)
        return self._begin_fanout(run_id, sid, items, connector_idx=idx)

    def _connector_halted(self, run_id, sid, idx):
        """The scatter connector job finished while the run is ``halting``: nothing fans out; the step is cancelled
        and the run ends if that was the last unsettled step."""
        spec = self._spec_of(run_id)

        def fn(d):
            st = d["steps"][sid]
            if d["status"] != "halting" or st["state"] in TERMINAL_STEP_STATES or st["scatter"]["phase"] != "connector" \
                    or idx != len(st["attempts"]) - 1:
                return None
            new = mark_step_cancelled(end_attempt(d, sid, "completed"), sid, "failure", by_step=d.get("failed_step"),
                                      reason=d.get("error_class"))
            if not unsettled_steps(new):
                new = _final_doc(new, spec, ws.settled_map(self.r, run_id))
            return new, None, lambda pipe: ws.record_done(pipe, run_id, sid)
        res = self._update(run_id, fn)
        return "scatter %s: connector done while halting, step cancelled" % sid if res else "connector event ignored"

    def _begin_fanout(self, run_id, sid, items, connector_idx=None):
        """Record N and zero the counters (one transaction with the run-doc change that puts the step into
        phase ``children``), then submit the children. An empty list settles the step at once."""
        def fn(d):
            st = d["steps"][sid]
            if d["status"] != "running" or st["state"] in TERMINAL_STEP_STATES:
                return None
            if connector_idx is None:
                if st["state"] != "waiting":
                    return None
                new = d
            else:
                if st["scatter"]["phase"] != "connector" or connector_idx != len(st["attempts"]) - 1:
                    return None
                new = end_attempt(d, sid, "completed")
            return (mark_scatter_fanout(new, sid, len(items)), None,
                    lambda pipe: ws.scatter_begin(pipe, run_id, sid, items))
        if self._update(run_id, fn) is None:
            return "scatter %s ignored: already fanned out or run over" % sid
        if not items:
            return "scatter %s: empty list; %s" % (sid, self._scatter_finish(run_id, sid))
        return self._fanout(run_id, sid)

    def _fanout(self, run_id, sid):
        """Submit every child that has no job yet (idempotent: a replay resumes where a crash stopped)."""
        doc, spec = self._load(run_id)
        step = spec["steps"][sid]
        items = ws.scatter_items(self.r, run_id, sid)
        if items is None:
            raise EventRejected("scatter %s of run %s has no recorded items" % (sid, run_id))
        n = len(items)
        try:
            base = resolve_step_inputs(step, ws.settled_map(self.r, run_id), doc["inputs"])
        except WorkflowRefError as e:
            return self._fail_waiting(run_id, sid, str(e), ERROR_INPUT_RESOLUTION)
        submitted = 0
        for i in range(n):
            if i and i % SCATTER_CANCEL_CHECK == 0:
                cur = ws.get_run(self.r, run_id)
                if cur is None or cur["status"] != "running" or cur["steps"][sid]["state"] in TERMINAL_STEP_STATES:
                    return "scatter %s: run ended after %d children; fan-out stopped" % (sid, submitted)
            pid = self.new_id()
            if not ws.scatter_op(self.r, run_id, sid, "claim", i, pid)[0]:
                continue  # already submitted (a replay)
            params = dict(base, **{step["scatter"]["input"]: items[i]})
            try:
                job = self._build_job(doc, spec, sid, params, child_index=i, child_total=n)
            except Exception as e:  # FrozenJobError etc.: the step itself is misconfigured, not one child
                ws.scatter_op(self.r, run_id, sid, "unclaim", i, pid)
                return self._fail_waiting(run_id, sid, "cannot build job: %s" % e, ERROR_SUBMIT)
            try:
                self.submit_fn(job, pid)
            except Exception:
                ws.scatter_op(self.r, run_id, sid, "unclaim", i, pid)
                raise
            submitted += 1
        self._scatter_sync(run_id, sid, with_children=True)
        return "scatter %s: %d children submitted" % (sid, submitted)

    def _scatter_sync(self, run_id, sid, with_children=False):
        """Write the Redis counters (and the child job ids, for scatters small enough) into the run doc.
        Counters never go backwards (concurrent consumers may write out of order)."""
        c = ws.scatter_counts(self.r, run_id, sid)
        cap = self._scatter_conf("WORKFLOW_SCATTER_DOC_CHILDREN_MAX", DEFAULT_SCATTER_DOC_CHILDREN_MAX)
        children = None
        if with_children and 0 < c["n"] <= cap:
            recs = ws.scatter_children(self.r, run_id, sid)
            children = [(recs[i]["job"] if i in recs else None) for i in range(c["n"])]

        def fn(d):
            st = d["steps"][sid]
            if d["status"] not in LIVE_RUN_STATUSES or st["state"] in TERMINAL_STEP_STATES:
                return None
            sc = st["scatter"]
            if children is None and c["completed"] <= sc["n_completed"] and c["failed"] <= sc["n_failed"]:
                return None
            return update_scatter(d, sid, n_completed=max(c["completed"], sc["n_completed"]),
                                  n_failed=max(c["failed"], sc["n_failed"]), children=children), None
        self._update(run_id, fn)

    def _child_event(self, run_id, spec, doc, sid, event):
        status = event.get("status")
        st = doc["steps"][sid]
        if "scatter" not in spec["steps"][sid]:
            raise EventRejected("child event for step %s, which is not a scatter step" % sid)
        try:
            idx = int(event["child_index"])
        except (TypeError, ValueError):
            raise EventRejected("bad child_index %r" % (event.get("child_index"),))
        if status == "job-completed" and st["state"] == "completed":  # duplicate: heal a crash after settle
            return "duplicate child completion of %s: %s" % (sid, self._after_settle(run_id, sid, replay=True))
        if doc["status"] not in LIVE_RUN_STATUSES:
            return "ignored: run %s is %s" % (run_id, doc["status"])
        if st["state"] in TERMINAL_STEP_STATES or st["scatter"]["phase"] != "children":
            return "ignored: child event for %s, which is %s" % (sid, st["state"])
        rec = ws.scatter_child(self.r, run_id, sid, idx)
        ids = {i for i in (event.get("payload_id"), event.get("job_id")) if i}
        if rec is None or rec["job"] not in ids:
            return "ignored: %s event for %s[%d] matches no attempt (stale or foreign)" % (status, sid, idx)
        if status == "job-started":
            ws.scatter_op(self.r, run_id, sid, "started", idx, rec["job"])
            return "started %s[%d]" % (sid, idx)
        if status == "job-completed":
            try:
                outputs = outputs_to_dict(event.get("workflow_outputs"))
            except WorkflowRefError as e:
                return self._child_failed(run_id, spec, sid, idx, rec, dict(
                    event, status="job-failed", error_class=ERROR_OUTPUTS,
                    short_error="invalid workflow outputs: %s" % e), retryable=False)
            return self._child_settle(run_id, sid, idx, rec, "completed", {"ok": True, "outputs": outputs})
        if status in FAILURE_STATUSES:
            return self._child_failed(run_id, spec, sid, idx, rec, event)
        return "ignored: status %r" % (status,)

    def _child_settle(self, run_id, sid, idx, rec, state, result):
        ok, c, f, n = ws.scatter_op(self.r, run_id, sid, "settle", idx, rec["job"], state, json.dumps(result))
        return "%s %s[%d] (%d ok, %d failed of %d): %s" % (
            state, sid, idx, c, f, n, self._scatter_after(run_id, sid, c, f, n, changed=ok))

    def _child_failed(self, run_id, spec, sid, idx, rec, event, retryable=True):
        ec = event.get("error_class") or DEFAULT_ERROR_CLASS.get(event.get("status"), "worker")
        error = short_error(event.get("short_error") or event.get("status"))
        if rec["state"] in ("completed", "failed"):
            return "ignored: failure of %s[%d], already %s" % (sid, idx, rec["state"])
        halting = (ws.get_run(self.r, run_id) or {}).get("status") == "halting"  # a halting run does not retry
        if retryable and not halting and is_retryable(retry_policy2(spec, sid), ec, rec["attempt"] - 1):
            if not ws.scatter_op(self.r, run_id, sid, "retry_mark", idx, rec["job"])[0]:
                return "failure of %s[%d] ignored" % (sid, idx)
            return "retry %s[%d] (attempt %d, after %s): %s" % (
                sid, idx, rec["attempt"] + 1, ec, self._retry_child(run_id, sid, idx, rec["job"], rec["attempt"]))
        return self._child_settle(run_id, sid, idx, rec, "failed", {"ok": False, "error": error, "error_class": ec})

    def _retry_child(self, run_id, sid, idx, old_job, attempt):
        doc, spec = self._load(run_id)
        step = spec["steps"][sid]
        items = ws.scatter_items(self.r, run_id, sid)
        try:
            params = dict(resolve_step_inputs(step, ws.settled_map(self.r, run_id), doc["inputs"]),
                          **{step["scatter"]["input"]: items[idx]})
            job = self._build_job(doc, spec, sid, params, child_index=idx, child_total=len(items))
            job["retry_count"] = attempt  # as for a linear retry: the PGE reads it from _job.json
        except Exception as e:  # cannot be re-submitted: this child fails for good
            return self._child_settle(run_id, sid, idx, {"job": old_job}, "failed", {
                "ok": False, "error": short_error("cannot build retry job: %s" % e), "error_class": ERROR_SUBMIT})
        new_job = self.new_id()
        if not ws.scatter_op(self.r, run_id, sid, "requeue", idx, old_job, new_job)[0]:
            return "already requeued"
        try:
            self.submit_fn(job, new_job)
        except Exception:
            ws.scatter_op(self.r, run_id, sid, "revert", idx, new_job, old_job)
            raise
        return "submitted as %s" % new_job

    def _scatter_after(self, run_id, sid, c, f, n, changed):
        """After a child settled (or a duplicate event): finish when every child has settled, else write a
        throttled progress snapshot."""
        if c + f >= n:
            return self._scatter_finish(run_id, sid)
        if changed:
            stride = max(1, math.ceil(n / max(1, self._scatter_conf("WORKFLOW_SCATTER_MAX_SNAPSHOTS",
                                                                     DEFAULT_SCATTER_MAX_SNAPSHOTS))))
            if (c + f) % stride == 0:
                self._scatter_sync(run_id, sid)
                return "progress snapshot"
        return "no snapshot (throttled)"

    def _scatter_finish(self, run_id, sid):
        """Every child settled. completed/N >= threshold: the step completes with one index-aligned array per
        output (``null`` where the child failed, B14); else it fails ``scatter_threshold``. Idempotent."""
        spec = self._spec_of(run_id)
        step = spec["steps"][sid]
        cnt = ws.scatter_counts(self.r, run_id, sid)
        n, c, f = cnt["n"], cnt["completed"], cnt["failed"]
        if c + f < n:
            return "scatter %s not finished (%d of %d settled)" % (sid, c + f, n)
        results = ws.scatter_results(self.r, run_id, sid)
        threshold = step["scatter"].get("threshold", 1.0)
        passed = n == 0 or c / n >= threshold - 1e-9
        cap = self._scatter_conf("WORKFLOW_SCATTER_DOC_CHILDREN_MAX", DEFAULT_SCATTER_DOC_CHILDREN_MAX)
        recs = ws.scatter_children(self.r, run_id, sid) if 0 < n <= cap else {}
        children = [(recs[i]["job"] if i in recs else None) for i in range(n)] if recs else None
        outputs = {name: [((results.get(i) or {}).get("outputs") or {}).get(name) if (results.get(i) or {}).get("ok")
                          else None for i in range(n)] for name in step.get("out") or []}
        first_error = next(((i, r) for i, r in sorted(results.items()) if not r.get("ok")), None)
        error = None
        if not passed:
            error = "scatter %s: %d of %d children completed (%.4g%%), below the threshold of %.4g%%" % (
                sid, c, n, 100.0 * c / n, 100.0 * threshold)
            if first_error:
                error += "; first failure: child %d (%s): %s" % (
                    first_error[0], first_error[1].get("error_class"), first_error[1].get("error"))
        box = {}

        def fn(d):
            st = d["steps"][sid]
            if d["status"] not in LIVE_RUN_STATUSES or st["state"] in TERMINAL_STEP_STATES \
                    or st["scatter"]["phase"] != "children":
                return None
            new = update_scatter(d, sid, n_completed=c, n_failed=f, children=children)
            if passed:
                return (mark_step_completed(new, sid), None,
                        lambda pipe: ws.record_settle(pipe, run_id, sid, "completed", outputs))
            new, revoke, cancelled = apply_failure(new, sid, error, ERROR_SCATTER_THRESHOLD, spec,
                                                     lambda: ws.settled_map(self.r, run_id))

            def extra(pipe):
                ws.record_done(pipe, run_id, sid)
                for x in cancelled:
                    ws.record_done(pipe, run_id, x)
            box["failed"] = True
            return new, revoke, extra
        res = self._update(run_id, fn)
        if res is None:
            return "scatter %s finish ignored" % sid
        if box.get("failed"):
            self._revoke(run_id, res[1], None)
            return "step %s failed (%s): %s" % (sid, ERROR_SCATTER_THRESHOLD, error)
        return "scatter %s completed (%d ok, %d failed of %d): %s" % (sid, c, f, n, self._after_settle(run_id, sid))
