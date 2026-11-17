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

Extension points: ``FAILURE_HANDLERS`` (S6: drain, continue, ``halting``), ``_release`` scatter branch
(S5), ``_fire_halt`` (S6 refines halts), the run-level reaper recovery (S8) reads ``wf:active``.
"""

import copy
import logging
import uuid

from hysds import workflow_state as ws
from hysds_commons.workflow_refs import WorkflowRefError, outputs_to_dict
from hysds_commons.workflow_refs2 import evaluate_when, resolve_outputs, resolve_step_inputs
from hysds_commons.workflow_run2 import (
    TERMINAL_STEP_STATES,
    end_attempt,
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
ACTIVE_ATTEMPT = ("queued", "running")


class EventRejected(Exception):
    """The event cannot be handled (unknown run/step, malformed): the shell dead-letters it."""


def _is_active_attempt(att):
    return att["status"] in ACTIVE_ATTEMPT


def _cancel_rest(doc, kind, by_step, reason):
    """Cancel every non-settled step of ``doc`` (kill semantics, PLAN 6.6): the active attempt ends
    ``cancelled`` and the step ``cancelled``. Returns ``(doc, payload_ids_to_revoke, cancelled_steps)``."""
    revoke, cancelled = [], []
    for sid in sorted(doc["steps"]):
        st = doc["steps"][sid]
        if st["state"] in TERMINAL_STEP_STATES:
            continue
        if st["attempts"] and _is_active_attempt(st["attempts"][-1]):
            revoke.append(st["attempts"][-1]["job_id"])
            doc = end_attempt(doc, sid, "cancelled")
        doc = mark_step_cancelled(doc, sid, kind, by_step=by_step, reason=reason)
        cancelled.append(sid)
    return doc, revoke, cancelled


def _fail_kill(doc, sid, error, error_class):
    """``kill`` (the default and, in S4, the only mode): the step fails permanently, the run fails, every
    other queued/running job is revoked and every other unsettled step is cancelled. Pure.
    Returns ``(doc, revoke_ids, cancelled_steps)``."""
    st = doc["steps"][sid]
    if st["attempts"] and _is_active_attempt(st["attempts"][-1]):
        doc = end_attempt(doc, sid, "failed", error=error)
    doc = mark_step_failed(doc, sid, error=error, error_class=error_class)
    doc, revoke, cancelled = _cancel_rest(doc, "failure", sid, error_class)
    doc = set_run_status(doc, "failed", error_class=error_class, reason=short_error(error), failed_step=sid)
    return doc, revoke, cancelled


# S6 extension point: add "drain" (-> "halting") and "continue" here with the same signature.
FAILURE_HANDLERS = {"kill": _fail_kill}


def apply_failure(doc, sid, error, error_class):
    mode = doc.get("on_error", "kill")
    handler = FAILURE_HANDLERS.get(mode)
    if handler is None:
        log.warning("run %s: on_error %r is not implemented yet; using kill", doc["run_id"], mode)
        handler = FAILURE_HANDLERS["kill"]
    return handler(doc, sid, error, error_class)


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
        if doc["status"] != "running" or doc["steps"][sid]["state"] != "waiting":
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
        if "scatter" in step:  # S5 extension point
            return self._fail_waiting(run_id, sid, "scatter not implemented yet", ERROR_NOT_IMPLEMENTED)
        try:
            params = resolve_step_inputs(step, settled, doc["inputs"])
        except WorkflowRefError as e:
            return self._fail_waiting(run_id, sid, str(e), ERROR_INPUT_RESOLUTION)
        return self._queue_and_submit(run_id, sid, params)

    def _publish(self, event):
        if self.publish_fn is not None:
            self.publish_fn(event)
        else:
            ws.publish_run_event(event["run_id"], event["kind"],
                                 {k: v for k, v in event.items() if k not in ("kind", "run_id")})

    # ------------------------------------------------------------------ submit
    def _build_job(self, doc, spec, sid, params):
        step = spec["steps"][sid]
        jt = step["job_type"]
        definition = (doc.get("job_definitions") or {}).get(jt) or {}
        queue = step_queue(spec, sid, definition.get("queue"))
        workflow = {"run_id": doc["run_id"], "step_id": sid, "spec_id": doc["spec_id"],
                    "child_index": None, "child_total": None}
        tl = step.get("time_limit")
        kwargs = dict(workflow=workflow, tags=[step.get("tag") or sid], priority=self.priority,
                      time_limit=tl if tl else None, enable_dedup=False)
        if self.build_job_fn is not None:
            return self.build_job_fn(definition, jt, queue, params, **kwargs)
        from hysds_commons.workflow_job2 import resolve_frozen_job
        return resolve_frozen_job(definition, jt, queue, params, **kwargs)

    def _queue_and_submit(self, run_id, sid, params, retry_of=None):
        """Record the attempt (compare-and-set), then send the job. ``retry_of`` is the number of attempts
        the step had when the retry was decided (a concurrent duplicate loses the race)."""
        doc, spec = self._load(run_id)
        if not spec["steps"][sid].get("queue") and not (doc.get("job_definitions") or {}).get(
                spec["steps"][sid].get("job_type"), {}).get("queue"):
            return self._fail_waiting(run_id, sid, "no queue for step %s" % sid, "no_queue", retry_of=retry_of)
        try:
            job = self._build_job(doc, spec, sid, params)
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
            new, revoke, cancelled = apply_failure(d, sid, error, error_class)

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

    def _fire_halt(self, run_id, sid, settled):
        """A halt step's ``when`` held: it is never submitted, its settle IS the trigger (S6 refines)."""
        def fn(d):
            if d["status"] != "running" or d["steps"][sid]["state"] != "waiting":
                return None
            h = d["steps"][sid]["halt"]
            new = mark_step_halted(d, sid)
            new, revoke, cancelled = _cancel_rest(new, "halt", sid, h.get("reason"))
            if h["status"] == "completed":
                try:
                    outs = resolve_outputs(self._spec_of(run_id), complete_settled(settled, new), new["inputs"])
                    new = set_run_status(set_run_outputs(new, outs), "completed", reason=h.get("reason"))
                except WorkflowRefError as e:
                    new = set_run_status(new, "failed", error_class=ERROR_OUTPUT_RESOLUTION,
                                         reason=short_error(e), failed_step=sid)
            else:
                new = set_run_status(new, "failed", error_class=ERROR_HALTED, reason=h.get("reason") or "halted",
                                     failed_step=sid)

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

    def _revoke(self, run_id, payload_ids, username):
        if not payload_ids or self.revoke_fn is None:
            return
        try:
            self.revoke_fn(run_id, list(payload_ids), username)
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
        """Every step settled: resolve the workflow outputs (type checked) and end the run."""
        settled = ws.settled_map(self.r, run_id)

        def fn(d):
            if d["status"] != "running":
                return None
            spec = self._spec_of(run_id)
            try:
                outs = resolve_outputs(spec, settled, d["inputs"])
                new = set_run_status(set_run_outputs(d, outs), "completed")
            except WorkflowRefError as e:
                new = set_run_status(d, "failed", error_class=ERROR_OUTPUT_RESOLUTION, reason=short_error(e))
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
            new, revoke, cancelled = _cancel_rest(d, "user", None, "cancelled by %s" % user)
            new = set_run_status(new, "cancelled", reason="cancelled by %s" % user, cancelled_by=user or "unknown")

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
        if event.get("child_index") is not None:  # S5 extension point (scatter children)
            return "ignored: scatter child event for %s (scatter not implemented yet)" % sid
        st = doc["steps"][sid]
        ids = {i for i in (event.get("payload_id"), event.get("job_id")) if i}
        idx = next((i for i, a in enumerate(st["attempts"]) if a["job_id"] in ids), None)

        if status == "job-completed" and st["state"] == "completed":  # duplicate: heal a crash after settle
            return "duplicate completion of %s: %s" % (sid, self._after_settle(run_id, sid, replay=True))
        if doc["status"] != "running":
            return "ignored: run %s is %s" % (run_id, doc["status"])
        if st["state"] in TERMINAL_STEP_STATES:
            return "ignored: step %s already %s" % (sid, st["state"])
        if idx is None:
            return "ignored: %s event for %s matches no attempt (stale or foreign)" % (status, sid)
        if status == "job-started":
            return self._started(run_id, sid, idx, event)
        if status == "job-completed":
            return self._completed(run_id, sid, idx, event)
        if status in FAILURE_STATUSES:
            return self._failed(run_id, spec, sid, idx, event)
        return "ignored: status %r" % (status,)

    def _started(self, run_id, sid, idx, event):
        def fn(d):
            st = d["steps"][sid]
            if d["status"] != "running" or idx != len(st["attempts"]) - 1 or st["attempts"][idx]["status"] != "queued":
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
            if d["status"] != "running" or st["state"] in TERMINAL_STEP_STATES or idx != len(st["attempts"]) - 1:
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
            if d["status"] != "running" or st["state"] in TERMINAL_STEP_STATES or idx != len(st["attempts"]) - 1:
                return None
            n = len(st["attempts"])
            active = _is_active_attempt(st["attempts"][-1])
            if retryable and is_retryable(retry_policy2(spec, sid), ec, n - 1):
                box["retry_of"] = n
                return (end_attempt(d, sid, "failed", error=error), None) if active else None
            new, revoke, cancelled = apply_failure(d, sid, error, ec)

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
        return "retry %s (attempt %d, after %s): %s" % (sid, n_attempts + 1, ec,
                                                         self._queue_and_submit(run_id, sid, params, retry_of=n_attempts))
