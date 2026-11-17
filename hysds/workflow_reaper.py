"""Reaper of workflows v2 (PLAN 6.8): recovers stuck runs using Redis only (never touches the search index).

Per sweep, for every run in the ``wf:active`` registry (:func:`hysds.workflow_state.active_heartbeats`), under a
per-run Redis lock (``wf:lock:<R>``, so two reapers never act on the same run at once; the consumer needs no
lock: every engine change is a compare-and-set on the run doc, so it and the reaper cannot double-apply):

====================================  =====================================================================
case                                  action (always through the engine, so it is atomic and snapshotted)
====================================  =====================================================================
run older than its time limit         cancel it as a user cancel does (``cancel_requested`` + reason)
                                      [every sweep; ``WORKFLOW_RUN_TIMEOUT_HOURS``, spec
                                      ``defaults.timeout_hours``]
(the rest only when the heartbeat is  older than ``WORKFLOW_STALL_SECONDS``: a run that is making progress is
left alone)
``halting``, no active attempt        finish the halt: the engine's ``finish_halting`` cancels the unsettled steps
                                      and ends the run through ``_final_doc`` (``completed`` for a completed
                                      halt, ``failed`` for a failure)
active attempt, job status terminal   replay the job event (engine dedup makes it safe) once the status
in Redis, event not processed         has stood for ``WORKFLOW_REAPER_EVENT_GRACE_SECONDS``. The exact event
                                      is stored by the job worker under ``wf:evt:<job>:<status>`` (outputs
                                      included) and replayed as it was; if it is gone (expired) a failure or
                                      an output-less completion is replayed synthetically, and a completion
                                      whose step declares outputs is failed ``lost`` and re-run within retries
attempt ``queued``, no job status     after ``WORKFLOW_REAPER_QUEUED_SECONDS``: ``job-failed`` event, error_class
                                      ``lost`` (engine retries within the step's policy, else fails the step)
attempt ``running``, no job status    after ``WORKFLOW_REAPER_VANISHED_SECONDS``: ``job-offline`` event
                                      (error_class ``offline``)
attempt ``queued``, status started    replay ``job-started``
step ``waiting``, every dep settled   re-drive the engine's release (a CAS: already-queued steps are no-ops)
all steps in the done set, running    finalize
====================================  =====================================================================

"Stood for" ages are tracked in ``wf:reap:<R>:...`` keys (SET NX with a TTL), so a restarted reaper loses nothing.
"""

import logging
import time
import uuid
from datetime import datetime, timezone

from hysds import workflow_state as ws

log = logging.getLogger("hysds.workflow_reaper")

DEFAULTS = {
    "WORKFLOW_REAPER_INTERVAL": 300,          # seconds between sweeps (v1 name)
    "WORKFLOW_STALL_SECONDS": 600,            # heartbeat age before a run is inspected (v1 name)
    "WORKFLOW_RUN_TIMEOUT_HOURS": 48,         # run time limit (v1 name); spec defaults.timeout_hours overrides
    "WORKFLOW_REAPER_QUEUED_SECONDS": 600,    # queued attempt with no job status -> lost
    "WORKFLOW_REAPER_VANISHED_SECONDS": 300,  # running attempt with no job status -> offline
    "WORKFLOW_REAPER_EVENT_GRACE_SECONDS": 120,  # terminal job status not processed by the engine -> replay
    "WORKFLOW_REAPER_LOCK_TTL": 120,          # per-run lock expiry
}
REAPER_USER = "workflow-reaper"
TERMINAL_JOB_STATUSES = frozenset({"job-completed", "job-failed", "job-deduped", "job-offline", "job-revoked"})
SEEN_TTL = 7 * 24 * 3600
LOCK_RELEASE_LUA = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"


def conf(name):
    """Setting ``name`` from celeryconfig, else its default."""
    default = DEFAULTS[name]
    try:
        from hysds.celery import app

        v = app.conf.get(name, default)
        return v if isinstance(v, (int, float)) and not isinstance(v, bool) else default
    except Exception:  # noqa: BLE001  (unit tests / no celeryconfig)
        return default


def k_lock(run_id):
    return "wf:lock:%s" % run_id


def k_seen(run_id, tag, job_id):
    return "wf:reap:%s:%s:%s" % (run_id, tag, job_id)


def _epoch(iso):
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).replace(
            tzinfo=timezone.utc).timestamp() if iso else None
    except ValueError:
        return None


def run_timeout_hours(spec, default):
    v = ((spec or {}).get("defaults") or {}).get("timeout_hours")
    return default if isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0 else v


def active_attempt(st):
    att = st["attempts"][-1] if st["attempts"] else None
    return att if att is not None and att["status"] in ("queued", "running") else None


class Reaper:
    """``engine`` is a :class:`hysds.workflow_engine.Engine`; ``status_fn(job_id)`` returns the Redis job status
    string or None (production: ``hysds.log_utils.get_job_status``); ``now_fn`` returns epoch seconds."""

    def __init__(self, engine, status_fn, settings=None, now_fn=time.time):
        self.engine, self.r, self.status_fn, self.now_fn = engine, engine.r, status_fn, now_fn
        self.s = {k: conf(k) for k in DEFAULTS}
        self.s.update(settings or {})

    # ------------------------------------------------------------------ sweep
    def sweep(self):
        """One pass over the registry. Returns the list of ``(run_id, action)`` taken."""
        actions = []
        now = self.now_fn()
        for run_id, hb in ws.active_heartbeats(self.r).items():
            token = str(uuid.uuid4())
            if not self.r.set(k_lock(run_id), token, nx=True, ex=int(self.s["WORKFLOW_REAPER_LOCK_TTL"])):
                continue  # another reaper has it
            try:
                actions += [(run_id, a) for a in self.reap_run(run_id, hb, now)]
            except Exception:  # noqa: BLE001  (one bad run must not stop the sweep)
                log.exception("reaper failed on run %s", run_id)
            finally:
                self.r.eval(LOCK_RELEASE_LUA, 1, k_lock(run_id), token)
        for run_id, action in actions:
            log.warning("reaper: run %s: %s", run_id, action)
        return actions

    def _age(self, run_id, tag, job_id, now):
        """Seconds since the reaper first saw this condition (recorded in Redis)."""
        key = k_seen(run_id, tag, job_id)
        self.r.set(key, repr(now), nx=True, ex=SEEN_TTL)
        raw = self.r.get(key)
        return now - float(raw)

    def _forget(self, run_id, tag, job_id):
        self.r.delete(k_seen(run_id, tag, job_id))

    # ------------------------------------------------------------------ one run
    def reap_run(self, run_id, heartbeat, now):
        doc, spec = ws.get_run(self.r, run_id), ws.get_spec(self.r, run_id)
        if doc is None or spec is None:
            self.r.zrem(ws.ACTIVE_KEY, run_id)  # expired run: drop the stale registry entry
            return ["dropped stale registry entry"]
        if doc["status"] in ws.TERMINAL_RUN_STATUSES:
            return []
        done = []
        hours = run_timeout_hours(spec, self.s["WORKFLOW_RUN_TIMEOUT_HOURS"])
        created = _epoch(doc.get("created_at"))
        if created is not None and now - created > hours * 3600:
            self.engine.handle_event({"kind": "cancel_requested", "run_id": run_id, "username": REAPER_USER,
                                      "reason": "run timed out after %gh" % hours})
            return ["timeout: cancelled after %gh" % hours]
        if now - heartbeat < self.s["WORKFLOW_STALL_SECONDS"]:
            return []
        if doc["status"] == "halting":
            if self.engine.finish_halting(run_id):
                done.append("halting recovery: finished")
            return done
        for sid in sorted(doc["steps"]):
            att = active_attempt(doc["steps"][sid])
            if att:
                a = self._recover_attempt(run_id, spec, sid, doc["steps"][sid]["state"], att, now)
                if a:
                    done.append(a)
        doc = ws.get_run(self.r, run_id)
        if doc["status"] == "running":
            settled = ws.settled_map(self.r, run_id)
            for sid in sorted(doc["steps"]):
                deps = spec["steps"][sid].get("deps") or []
                if doc["steps"][sid]["state"] == "waiting" and all(d in settled for d in deps):
                    note = self.engine._release(run_id, sid)
                    done.append("lost join/release %s: %s" % (sid, note))
            if ws.n_done(self.r, run_id) >= len(spec["steps"]):
                note = self.engine._finalize(run_id)
                if note:
                    done.append("finalize missed: %s" % note)
        return done

    def _recover_attempt(self, run_id, spec, sid, state, att, now):
        jid = att["job_id"]
        # the Redis job status is keyed by the celery task id of the run_job task (recorded from the job-queued /
        # job-started event), not by the attempt's payload id; without a recorded task id nothing is known yet
        status = self.status_fn(ws.get_task_id(self.r, run_id, jid) or jid)
        s = self.s
        base = {"run_id": run_id, "step_id": sid, "job_id": jid, "payload_id": jid, "child_index": None,
                "workflow_outputs": None, "error_class": None, "replayed": True}
        if status in TERMINAL_JOB_STATUSES:
            if self._age(run_id, "term", jid, now) < s["WORKFLOW_REAPER_EVENT_GRACE_SECONDS"]:
                return None
            stored = ws.get_stored_event(self.r, jid, status)
            if stored is not None:
                ev, kind = dict(stored, replayed=True), "missed terminal event (%s): stored event replayed" % status
            elif status == "job-completed" and spec["steps"][sid].get("out"):
                ev = dict(base, status="job-failed", error_class="lost",
                          short_error="job completed but its stored event (and so its outputs) is gone")
                kind = "missed completion with outputs, stored event missing: failed as lost"
            else:
                ev, kind = dict(base, status=status), "missed terminal event (%s): replayed" % status
        elif status is None:
            if state == "queued":
                if self._age(run_id, "queued", jid, now) < s["WORKFLOW_REAPER_QUEUED_SECONDS"]:
                    return None
                ev = dict(base, status="job-failed", error_class="lost", short_error="job never reached a worker")
                kind = "stuck queued: lost"
            else:
                if self._age(run_id, "gone", jid, now) < s["WORKFLOW_REAPER_VANISHED_SECONDS"]:
                    return None
                ev = dict(base, status="job-offline", error_class="offline", short_error="job status vanished")
                kind = "job vanished: offline"
        elif status == "job-started" and state == "queued":
            if self._age(run_id, "started", jid, now) < s["WORKFLOW_REAPER_EVENT_GRACE_SECONDS"]:
                return None
            ev, kind = dict(base, status="job-started"), "missed job-started: replayed"
        else:
            for tag in ("term", "queued", "gone", "started"):
                self._forget(run_id, tag, jid)  # healthy: a later condition starts a fresh clock
            return None
        note = self.engine.handle_event(ev)
        return "%s %s (%s)" % (kind, sid, note)


# --------------------------------------------------------------------------- process entry


def main(once=False):
    """The reaper process (``scripts/watchdog_workflow_run_timeouts.py`` for supervisord)."""
    import functools
    import random

    import requests
    from redis import StrictRedis

    from hysds.celery import app
    from hysds.log_utils import get_job_status
    from hysds.workflow_engine import Engine
    from hysds.workflow_rules import make_revoke_fn
    from hysds_commons.workflow_job2 import submit_frozen_job
    from hysds_commons.workflow_submit import submit_workflow_job

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    r = StrictRedis.from_url(app.conf.REDIS_JOB_STATUS_URL, ssl_cert_reqs="none")
    submit = functools.partial(submit_workflow_job, mozart_rest_url=app.conf.MOZART_REST_URL,
                               session=requests.Session())
    engine = Engine(r, submit_fn=lambda job, payload_id: submit_frozen_job(job, payload_id),
                    revoke_fn=make_revoke_fn(app.conf.get("WORKFLOW_REVOKE_JOB_TYPE"), submit),
                    list_key=app.conf.get("REDIS_JOB_STATUS_KEY", "logstash"))
    reaper = Reaper(engine, get_job_status)
    interval = reaper.s["WORKFLOW_REAPER_INTERVAL"]
    while True:
        try:
            n = len(reaper.sweep())
            log.info("sweep done: %d action(s)", n)
        except Exception:  # noqa: BLE001
            log.exception("sweep failed")
        if once:
            return
        time.sleep(random.uniform(interval * 0.9, interval * 1.1))
