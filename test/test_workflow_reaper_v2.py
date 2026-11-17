"""workflow v2 reaper against fakeredis: one test per recovery case. No OpenSearch, no broker."""
import fakeredis
import pytest
from test_workflow_engine import LINEAR, Harness, _src, _step, make_spec

from hysds import workflow_reaper as wr
from hysds import workflow_state as ws

import time

T0 = time.time()
SET = {"WORKFLOW_STALL_SECONDS": 600, "WORKFLOW_REAPER_QUEUED_SECONDS": 600, "WORKFLOW_REAPER_VANISHED_SECONDS": 300,
       "WORKFLOW_REAPER_EVENT_GRACE_SECONDS": 120, "WORKFLOW_RUN_TIMEOUT_HOURS": 48}


class Rig:
    def __init__(self, spec=None, **kw):
        self.h = Harness(spec or make_spec(LINEAR), **kw)
        self.status = {}
        self.now = T0
        self.reaper = wr.Reaper(self.h.eng, lambda jid: self.status.get(jid), SET, now_fn=lambda: self.now)

    def age(self, seconds):
        """Time passes with no state change (the heartbeat stays where it was)."""
        self.now += seconds

    def sweep(self):
        acts = self.reaper.sweep()
        self.h.pump()
        return acts

    def heartbeat_at_start(self):
        self._hb = T0
        self.h.r.zadd(ws.ACTIVE_KEY, {"r1": T0})


def rig(spec=None, **kw):
    g = Rig(spec, **kw)
    g.h.start()
    g.heartbeat_at_start()
    return g


def settle_a(g):
    pid, _ = g.h.job_of("a")
    g.status[pid] = "job-completed"
    return pid


def test_healthy_run_untouched():
    g = rig()
    pid, _ = g.h.job_of("a")
    g.status[pid] = "job-started"
    g.h.send({"run_id": "r1", "step_id": "a", "status": "job-started", "job_id": pid, "payload_id": pid})
    g.h.r.zadd(ws.ACTIVE_KEY, {"r1": g.now})
    g.age(300)  # fresh heartbeat window (< stall)
    before = g.h.doc()
    assert g.sweep() == []
    g.age(10_000)  # stale heartbeat, but job healthy and started
    assert g.sweep() == []
    assert g.h.doc() == before and len(g.h.jobs) == 1


def test_lost_release_after_settle_crash():
    g = rig()
    pid, _ = g.h.job_of("a")
    # crash between settle and release: a is completed + in wf:out/done, join never ran
    g.h.eng._after_settle = lambda *a, **k: None
    g.h.send({"run_id": "r1", "step_id": "a", "status": "job-completed", "job_id": pid, "payload_id": pid,
              "workflow_outputs": [{"name": "o", "value_s": "A"}]})
    assert g.h.doc()["steps"]["b"]["state"] == "waiting" and len(g.h.jobs) == 1
    del g.h.eng._after_settle
    g.age(700)
    acts = g.sweep()
    assert any("lost join/release b" in a for _, a in acts)
    assert g.h.doc()["steps"]["b"]["state"] == "queued" and g.h.job_of("b")[1]["params"] == {"v": "A"}
    g.age(700)
    pid_b = g.h.job_of("b")[0]
    g.status[pid_b] = "job-started"
    assert not any("release" in a for _, a in g.sweep())
    assert len([j for j in g.h.jobs if j[1]["workflow"]["step_id"] == "b"]) == 1  # idempotent


def test_stuck_queued_resubmitted_within_retries():
    spec = make_spec(dict(LINEAR, a=_step(ins={"v": _src("$.inputs.x")}, max_retries=1, retry_on=["*"])))
    g = rig(spec)
    assert g.sweep() == []
    g.age(700)
    assert g.sweep() == []  # heartbeat stale, but the clock on the missing status starts at first sight
    g.age(700)
    acts = g.sweep()
    assert any("stuck queued" in a for _, a in acts)
    atts = g.h.doc()["steps"]["a"]["attempts"]
    assert [x["status"] for x in atts] == ["failed", "queued"] and len(g.h.jobs) == 2
    assert g.h.doc()["status"] == "running"


def test_stuck_queued_fails_lost_when_no_retries():
    g = rig()
    g.age(700)
    g.sweep()
    g.age(700)
    g.sweep()
    d = g.h.doc()
    assert d["status"] == "failed" and d["error_class"] == "lost" and d["failed_step"] == "a"


def test_missed_terminal_event_replayed_once():
    spec = make_spec({"a": _step(ins={"v": _src("$.inputs.x")}, out=())})
    g = rig(spec)
    pid = settle_a(g)  # job completed, event lost
    g.age(700)
    g.sweep()  # starts the grace clock
    g.age(200)
    acts = g.sweep()
    assert any("missed terminal" in a for _, a in acts)
    d = g.h.doc()
    assert d["steps"]["a"]["state"] == "completed" and d["status"] == "completed"
    # the same event replayed again is not double-applied
    g.h.send({"run_id": "r1", "step_id": "a", "status": "job-completed", "job_id": pid, "payload_id": pid,
              "replayed": True})
    assert g.h.doc() == d and len(d["steps"]["a"]["attempts"]) == 1


def test_missed_failure_event_replayed():
    g = rig()
    pid, _ = g.h.job_of("a")
    g.status[pid] = "job-failed"
    g.age(700)
    g.sweep()
    g.age(200)
    g.sweep()
    d = g.h.doc()
    assert d["status"] == "failed" and d["error_class"] == "worker"


def test_missed_completion_with_outputs_runs_again_as_lost():
    spec = make_spec(dict(LINEAR, a=_step(ins={"v": _src("$.inputs.x")}, max_retries=1, retry_on=["*"])))
    g = rig(spec)
    settle_a(g)
    g.age(700)
    g.sweep()
    g.age(200)
    g.sweep()
    assert [x["status"] for x in g.h.doc()["steps"]["a"]["attempts"]] == ["failed", "queued"]


def test_job_vanished_fails_offline():
    g = rig()
    pid, _ = g.h.job_of("a")
    g.h.send({"run_id": "r1", "step_id": "a", "status": "job-started", "job_id": pid, "payload_id": pid})
    g.age(700)
    g.sweep()
    g.age(400)
    acts = g.sweep()
    assert any("vanished" in a for _, a in acts)
    d = g.h.doc()
    assert d["status"] == "failed" and d["error_class"] == "offline"


def test_halting_with_nothing_running_is_finished():
    g = rig()
    ws.update_run(g.h.r, "r1", lambda d: _to_halting(d), list_key="logstash")
    g.h.r.zadd(ws.ACTIVE_KEY, {"r1": T0})
    g.age(700)
    # the running job is still active: not finished
    pid, _ = g.h.job_of("a")
    g.status[pid] = "job-started"
    g.h.send({"run_id": "r1", "step_id": "a", "status": "job-started", "job_id": pid, "payload_id": pid})
    assert g.sweep() == [] and g.h.doc()["status"] == "halting"
    # its attempt ends (cancelled) -> nothing running -> finish
    ws.update_run(g.h.r, "r1", lambda d: _end_attempts(d), list_key="logstash")
    g.h.r.zadd(ws.ACTIVE_KEY, {"r1": T0})
    acts = g.sweep()
    assert any("halting recovery" in a for _, a in acts)
    d = g.h.doc()
    assert d["status"] == "failed" and d["error_class"] == "worker" and d["steps"]["c"]["state"] == "cancelled"
    assert ws.n_done(g.h.r, "r1") == 3


def _to_halting(d):
    import copy
    n = copy.deepcopy(d)
    n["status"], n["error_class"], n["reason"], n["failed_step"] = "halting", "worker", "boom", "a"
    n["version"] = d["version"] + 1
    return n, None


def _end_attempts(d):
    from hysds_commons.workflow_run2 import end_attempt, mark_step_failed
    n = end_attempt(d, "a", "failed", error="boom")
    return mark_step_failed(n, "a", error="boom", error_class="worker"), None, lambda p: ws.record_done(p, "r1", "a")


def test_finalize_missed():
    spec = make_spec({"a": _step(ins={"v": _src("$.inputs.x")}, out=())}, outputs={})
    g = rig(spec)
    h = g.h
    h.eng._after_settle = lambda *a, **k: None  # crash right after the settle commit
    pid, _ = h.job_of("a")
    h.send({"run_id": "r1", "step_id": "a", "status": "job-completed", "job_id": pid, "payload_id": pid})
    del h.eng._after_settle
    assert h.doc()["status"] == "running"
    g.age(700)
    acts = g.sweep()
    assert any("finalize missed" in a for _, a in acts)
    assert h.doc()["status"] == "completed"


def test_run_timeout_cancels_like_user_cancel():
    g = rig()
    g.age(49 * 3600)
    g.h.r.zadd(ws.ACTIVE_KEY, {"r1": g.now})  # even with a fresh heartbeat
    # the harness run was created "now" in wall time; make it old
    def old(d):
        import copy
        n = copy.deepcopy(d)
        n["created_at"], n["version"] = "2020-01-01T00:00:00.000000Z", d["version"] + 1
        return n, None
    ws.update_run(g.h.r, "r1", old, list_key="logstash")
    g.reaper.now_fn = lambda: T0 + 100 * 365 * 86400.0
    acts = g.sweep()
    assert any("timeout" in a for _, a in acts)
    d = g.h.doc()
    assert d["status"] == "cancelled" and "timed out" in d["reason"] and d["cancelled_by"] == "workflow-reaper"
    assert g.h.revoked and g.h.r.zrange(ws.ACTIVE_KEY, 0, -1) == []


def test_lock_prevents_double_action():
    g = rig()
    g.age(700)
    g.status.clear()
    g.h.r.set(wr.k_lock("r1"), "other-reaper", ex=100)
    g.age(5000)
    assert g.sweep() == [] and g.h.doc()["steps"]["a"]["attempts"][-1]["status"] == "queued"
    g.h.r.delete(wr.k_lock("r1"))
    g.sweep()
    g.age(1000)
    g.sweep()
    assert g.h.doc()["status"] == "failed"
    assert g.h.r.get(wr.k_lock("r1")) is None  # released


def test_no_opensearch_or_v1_imports():
    src = open(wr.__file__).read()
    assert "elasticsearch" not in src.lower() and "opensearch" not in src.lower() and "es_util" not in src


def test_halting_from_a_completed_halt_finishes_completed():
    """finish_halting goes through the engine's _final_doc: a completed halt ends completed, not failed."""
    steps = {"g": _step(out=("o",)), "r": _step(out=("o",)),
             "h": {"halt": {"status": "completed", "reason": "enough"}, "deps": ["g"], "when": "$.g.bad"}}
    steps["g"] = _step(out=("o", "bad"))
    spec = make_spec(steps, outputs={"res": {"type": "string?", "source": ["$.g.o"]}}, on_error="drain")
    g = rig(spec)
    pid, _ = g.h.job_of("r")
    g.h.send({"run_id": "r1", "step_id": "r", "status": "job-started", "job_id": pid, "payload_id": pid})
    g.h.finish("g", {"bad": True, "o": "G"})
    assert g.h.doc()["status"] == "halting"
    # the running job's end is lost: its attempt ends without the engine hearing about it
    ws.update_run(g.h.r, "r1", lambda d: _end_r(d), list_key="logstash")
    g.h.r.zadd(ws.ACTIVE_KEY, {"r1": T0})
    g.age(700)
    acts = g.sweep()
    assert any("halting recovery" in a for _, a in acts)
    d = g.h.doc()
    assert d["status"] == "completed" and d["reason"] == "enough" and d["outputs"] == {"res": "G"}
    assert d["steps"]["r"]["state"] == "cancelled"


def _end_r(d):
    from hysds_commons.workflow_run2 import end_attempt
    return end_attempt(d, "r", "cancelled"), None


def _outputs_spec():
    return make_spec(dict(LINEAR, a=_step(ins={"v": _src("$.inputs.x")}, out=("o",))))


def test_missed_completion_with_outputs_replays_the_stored_event():
    import json
    g = rig(_outputs_spec())
    pid = settle_a(g)
    ev = {"run_id": "r1", "step_id": "a", "job_id": pid, "payload_id": pid, "status": "job-completed",
          "workflow_outputs": [{"name": "o", "value_s": "OUT"}], "error_class": None, "child_index": None}
    g.h.r.set(ws.k_event(pid, "job-completed"), json.dumps(ev))
    g.age(700)
    g.sweep()
    g.age(200)
    acts = g.sweep()
    assert any("stored event replayed" in a for _, a in acts)
    d = g.h.doc()
    assert d["steps"]["a"]["state"] == "completed" and d["steps"]["b"]["state"] in ("queued", "running")
    assert ws.settled_map(g.h.r, "r1")["a"]["outputs"] == {"o": "OUT"}


def test_missed_completion_with_outputs_and_no_stored_event_is_lost():
    g = rig(_outputs_spec())
    settle_a(g)
    g.age(700)
    g.sweep()
    g.age(200)
    acts = g.sweep()
    assert any("stored event missing" in a for _, a in acts)
    d = g.h.doc()
    assert d["status"] == "failed" and d["error_class"] == "lost"


def test_engine_deletes_the_stored_event_once_applied_and_a_replay_is_a_noop():
    import json
    g = rig(_outputs_spec())
    pid, _ = g.h.job_of("a")
    ev = {"run_id": "r1", "step_id": "a", "job_id": pid, "payload_id": pid, "status": "job-completed",
          "workflow_outputs": [{"name": "o", "value_s": "OUT"}], "error_class": None, "child_index": None}
    key = ws.k_event(pid, "job-completed")
    g.h.r.set(key, json.dumps(ev))
    g.h.send(ev)
    g.h.pump()
    assert g.h.r.get(key) is None and g.h.doc()["steps"]["a"]["state"] == "completed"
    before = g.h.doc()
    g.h.send(ev)  # duplicate / replay
    g.h.pump()
    assert g.h.doc()["steps"]["b"] == before["steps"]["b"] and g.h.doc()["steps"]["a"]["state"] == "completed"
    # a non-terminal event never touches it
    g.h.r.set(key, "x")
    g.h.send(dict(ev, status="job-started"))
    assert g.h.r.get(key) == b"x"


def test_job_status_is_looked_up_by_the_recorded_task_id_not_the_payload_id():
    """Redis job status keys are the run_job task id (what job-queued/job-started carry), not the payload id."""
    g = rig()
    pid, _ = g.h.job_of("a")
    ws.record_task_id(g.h.r, "r1", pid, "task-of-a")
    g.status["task-of-a"] = "job-failed"  # keyed by the task id only; the payload id has no status
    g.age(700)
    g.sweep()
    g.age(200)
    acts = g.sweep()
    assert any("missed terminal event (job-failed)" in a for _, a in acts)
    assert g.h.doc()["steps"]["a"]["state"] == "failed"
