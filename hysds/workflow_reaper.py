"""Stalled-run reaper logic (workflow contract 12.6); driven by
``scripts/watchdog_workflow_run_timeouts.py``.

The per-run decision (:func:`classify_run`) and the replay plan (:func:`plan_replay`) are pure.
:func:`sweep_once` wires them to OpenSearch through injected callables so it is unit-testable.

Per sweep, for each candidate run:

====================================================  ==========================================
run state                                             action
====================================================  ==========================================
running, age (now - submitted_at) > timeout hours     ``timeout``: cancel as ``failed``
                                                      ("run timed out after <h>h") + revoke jobs
running, updated_at older than stall seconds, and no  ``stalled``: replay a WORKFLOW_QUEUE event
replay for this run in the last stall seconds         for every non-terminal node whose jobs are
                                                      all terminal
failed/cancelled, updated < 24h ago                   ``sweep``: revoke non-terminal jobs
anything else                                         ``none``
====================================================  ==========================================

The reaper never writes node state itself except through the shared cancel mutate (timeout).
"""
import logging
from datetime import datetime, timedelta, timezone

from hysds_commons.workflow_cancel import (
    TERMINAL_JOB_STATUSES,
    TERMINAL_NODE_STATES,
    cancel_mutate,
    resolve_revoke_job_type,
    sweep_run,
)
from hysds_commons.workflow_status import update_status

log = logging.getLogger("workflow_reaper")

DEFAULT_RUN_TIMEOUT_HOURS = 48
DEFAULT_STALL_SECONDS = 600
SWEEP_WINDOW_SECONDS = 24 * 3600
PAGE_SIZE = 500

TIMEOUT, STALLED, SWEEP, NONE = "timeout", "stalled", "sweep", "none"


def parse_ts(value):
    """ISO-8601 (``...Z`` or offset) -> aware UTC datetime; None if missing/unparseable."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def run_timeout_hours(spec, default=DEFAULT_RUN_TIMEOUT_HOURS):
    """``spec.defaults.timeout_hours`` (positive number) else ``default``."""
    v = ((spec or {}).get("defaults") or {}).get("timeout_hours")
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0:
        return default
    return v


def classify_run(run, now, timeout_hours, stall_seconds, last_replay_at=None,
                 sweep_window=SWEEP_WINDOW_SECONDS):
    """Pure per-run decision: one of ``timeout | stalled | sweep | none``."""
    status = run.get("status")
    updated = parse_ts(run.get("updated_at")) or parse_ts(run.get("submitted_at"))
    if status == "running":
        submitted = parse_ts(run.get("submitted_at"))
        if submitted and now - submitted > timedelta(hours=timeout_hours):
            return TIMEOUT
        if updated and now - updated > timedelta(seconds=stall_seconds):
            if last_replay_at is not None and now - last_replay_at < timedelta(seconds=stall_seconds):
                return NONE  # rate limit: one replay per run per stall window
            return STALLED
        return NONE
    if status in ("failed", "cancelled"):
        if updated and now - updated < timedelta(seconds=sweep_window):
            return SWEEP
    return NONE


def latest_per_job(docs):
    """Latest doc (by ``@timestamp``/``timestamp``) per ``(child_index, job_id)``."""
    best = {}
    for d in docs:
        wf = d.get("workflow") or {}
        key = (wf.get("child_index"), d.get("job_id") or d.get("uuid"))
        ts = d.get("@timestamp") or d.get("timestamp") or ""
        if key not in best or ts >= (best[key].get("@timestamp") or best[key].get("timestamp") or ""):
            best[key] = d
    return list(best.values())


def plan_replay(run, jobs_by_step):
    """Pure: job_status docs whose WORKFLOW_QUEUE event should be re-published.

    For every non-terminal node with jobs: if every job of the step is terminal, the latest
    terminal job of the step (linear) or one per child (scatter child, ``child_index`` set).
    A step with any non-terminal job is left alone. Ordered by node id then child index.
    """
    out = []
    for nid in sorted((run.get("nodes") or {})):
        if (run["nodes"][nid] or {}).get("state") in TERMINAL_NODE_STATES:
            continue
        jobs = latest_per_job(jobs_by_step.get(nid) or [])
        if not jobs or any(j.get("status") not in TERMINAL_JOB_STATUSES for j in jobs):
            continue
        if any((j.get("workflow") or {}).get("child_index") is not None for j in jobs):
            per_child = {}
            for j in jobs:
                per_child[(j["workflow"]).get("child_index")] = j
            out.extend(per_child[k] for k in sorted(per_child, key=lambda k: (k is None, k)))
        else:
            out.append(max(jobs, key=lambda j: j.get("@timestamp") or j.get("timestamp") or ""))
    return out


def replay_event(doc, now=None):
    """The WORKFLOW_QUEUE payload for a job_status doc: same shape as
    ``hysds.log_utils.publish_workflow_event`` plus ``replayed: true``
    (a test pins the two key sets together)."""
    workflow = doc.get("workflow") or {}
    job = doc.get("job") or {}
    job_info = job.get("job_info") or {}
    now = now or datetime.now(timezone.utc)
    return {
        "run_id": workflow.get("run_id"),
        "step_id": workflow.get("step_id"),
        "spec_id": workflow.get("spec_id"),
        "child_index": workflow.get("child_index"),
        "child_total": workflow.get("child_total"),
        "job_id": doc.get("job_id") or doc.get("uuid"),
        "payload_id": doc.get("payload_id") or doc.get("uuid"),
        "status": doc.get("status"),
        "short_error": doc.get("short_error"),
        "workflow_outputs": job_info.get("workflow_outputs"),
        "error_class": doc.get("error_class") or job_info.get("error_class"),
        "retry_count": job.get("retry_count") or 0,
        "timestamp": f"{now.astimezone(timezone.utc).replace(tzinfo=None).isoformat()}Z",
        "replayed": True,
    }


# ---------------------------------------------------------------- I/O

def _iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def candidate_runs_query(now, size=PAGE_SIZE, search_after=None, sweep_window=SWEEP_WINDOW_SECONDS):
    body = {
        "size": size,
        "query": {"bool": {"minimum_should_match": 1, "should": [
            {"term": {"status": "running"}},
            {"bool": {"filter": [
                {"terms": {"status": ["failed", "cancelled"]}},
                {"range": {"updated_at": {"gte": _iso(now - timedelta(seconds=sweep_window))}}}]}},
        ]}},
        "sort": [{"submitted_at": {"order": "asc", "unmapped_type": "date"}},
                 {"run_id": {"order": "asc", "unmapped_type": "keyword"}}],
    }
    if search_after is not None:
        body["search_after"] = search_after
    return body


def fetch_candidate_runs(es, status_index, now):
    runs, after = [], None
    while True:
        r = es.search(index=status_index, body=candidate_runs_query(now, search_after=after))
        hits = r.get("hits", {}).get("hits", [])
        runs.extend(h["_source"] for h in hits)
        if len(hits) < PAGE_SIZE or not hits[-1].get("sort"):
            return runs
        after = hits[-1]["sort"]


def step_jobs_query(run_id, step_id, size=PAGE_SIZE, search_after=None):
    body = {
        "size": size,
        "_source": ["uuid", "job_id", "payload_id", "status", "short_error", "error_class",
                    "workflow", "@timestamp", "timestamp", "job.retry_count",
                    "job.job_info.workflow_outputs", "job.job_info.error_class"],
        "query": {"bool": {"filter": [{"term": {"workflow.run_id": run_id}},
                                      {"term": {"workflow.step_id": step_id}}]}},
        "sort": [{"@timestamp": {"order": "asc", "unmapped_type": "date"}},
                 {"uuid": {"order": "asc", "unmapped_type": "keyword"}}],
    }
    if search_after is not None:
        body["search_after"] = search_after
    return body


def fetch_step_jobs(es, job_status_index, run_id, step_id):
    docs, after = [], None
    while True:
        r = es.search(index=job_status_index, body=step_jobs_query(run_id, step_id, search_after=after))
        hits = r.get("hits", {}).get("hits", [])
        docs.extend(h["_source"] for h in hits)
        if len(hits) < PAGE_SIZE or not hits[-1].get("sort"):
            return docs
        after = hits[-1]["sort"]


def load_spec(es, specs_index, spec_id, cache):
    if spec_id not in cache:
        r = es.get(index=specs_index, id=spec_id, ignore=404)
        found = isinstance(r, dict) and r.get("found", "_source" in r) and "_source" in r
        cache[spec_id] = r["_source"] if found else None
    return cache[spec_id]


class ReaperConfig:
    def __init__(self, status_index="workflow_status", specs_index="workflow_specs",
                 job_status_index="job_status-current", job_specs_index="job_specs",
                 stall_seconds=DEFAULT_STALL_SECONDS, timeout_hours=DEFAULT_RUN_TIMEOUT_HOURS,
                 revoke_job_type=None, user="workflow-reaper"):
        self.status_index, self.specs_index = status_index, specs_index
        self.job_status_index, self.job_specs_index = job_status_index, job_specs_index
        self.stall_seconds, self.timeout_hours = stall_seconds, timeout_hours
        self.revoke_job_type, self.user = revoke_job_type, user


def sweep_once(es, cfg, submit, publish, state=None, now=None):
    """One reaper sweep. ``submit`` is a ``submit_workflow_job``-shaped callable, ``publish``
    takes a WORKFLOW_QUEUE payload dict. ``state`` (mutated) holds the in-memory rate limits
    ``{"replays": {run_id: datetime}, "sweeps": {run_id: datetime}, "revoke_type": str}``.
    Per-run failures are logged and do not stop the sweep. Returns a summary dict."""
    es = getattr(es, "es", es)
    state = state if state is not None else {}
    replays = state.setdefault("replays", {})
    sweeps = state.setdefault("sweeps", {})
    now = now or datetime.now(timezone.utc)
    summary = {"runs": 0, TIMEOUT: [], STALLED: [], SWEEP: [], "replayed": 0, "revoked": 0}
    specs = {}

    def revoke_type():
        if not state.get("revoke_type"):
            state["revoke_type"] = resolve_revoke_job_type(es, cfg.job_specs_index, cfg.revoke_job_type)
        return state["revoke_type"]

    def revoke(run_id):
        n = sweep_run(es, cfg.job_status_index, run_id, submit, revoke_type(), username=cfg.user)
        summary["revoked"] += n
        return n

    for run in fetch_candidate_runs(es, cfg.status_index, now):
        summary["runs"] += 1
        run_id = run.get("run_id")
        try:
            hours = cfg.timeout_hours
            if run.get("status") == "running":
                hours = run_timeout_hours(load_spec(es, cfg.specs_index, run.get("spec_id"), specs),
                                          cfg.timeout_hours)
            decision = classify_run(run, now, hours, cfg.stall_seconds, replays.get(run_id))
            if decision == SWEEP and run_id in sweeps and \
                    now - sweeps[run_id] < timedelta(seconds=cfg.stall_seconds):
                decision = NONE
            if decision == TIMEOUT:
                err = f"run timed out after {hours:g}h"
                doc = update_status(es, cfg.status_index, run_id,
                                    cancel_mutate(cfg.user, "failed", err, now=_iso(now)))
                if doc.get("status") == "failed" and doc.get("error") == err:
                    summary[TIMEOUT].append(run_id)
                    log.warning("run %s %s; revoked %d job(s)", run_id, err, revoke(run_id))
            elif decision == STALLED:
                jobs_by_step = {nid: fetch_step_jobs(es, cfg.job_status_index, run_id, nid)
                                for nid, n in (run.get("nodes") or {}).items()
                                if (n or {}).get("state") not in TERMINAL_NODE_STATES}
                plan = plan_replay(run, jobs_by_step)
                for doc in plan:
                    publish(replay_event(doc, now))
                if plan:
                    replays[run_id] = now
                    summary[STALLED].append(run_id)
                    summary["replayed"] += len(plan)
                    log.warning("run %s stalled: replayed %d event(s)", run_id, len(plan))
            elif decision == SWEEP:
                n = revoke(run_id)
                if n:
                    sweeps[run_id] = now
                    summary[SWEEP].append(run_id)
                    log.warning("run %s is %s: revoked %d leftover job(s)", run_id, run.get("status"), n)
        except Exception:
            log.exception("reaper failed on run %s", run_id)
    # bound the in-memory maps
    for m in (replays, sweeps):
        for k in [k for k, t in m.items() if now - t > timedelta(seconds=SWEEP_WINDOW_SECONDS)]:
            del m[k]
    return summary
