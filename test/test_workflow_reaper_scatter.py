"""workflow v2 reaper, scatter-item recovery: a child whose terminal event was missed, that is stuck queued or
vanished, or a fan-out / retry / finish the engine lost. fakeredis, no broker."""
import json
import time

from test_workflow_reaper_v2 import SET
from test_workflow_scatter import SH, spec_for

from hysds import workflow_reaper as wr
from hysds import workflow_state as ws

T0 = time.time()


class SRig:
    def __init__(self, items, **kw):
        spec = spec_for(**kw)
        if kw.get("retries"):  # the reaper's synthetic errors are classed lost/offline
            spec["steps"]["s"]["retry_on"] = ["*"]
        self.h = SH(spec, inputs={"items": items})
        self.status, self.now = {}, T0
        self.reaper = wr.Reaper(self.h.eng, lambda jid: self.status.get(jid), SET, now_fn=lambda: self.now)
        self.h.start()
        self.h.r.zadd(ws.ACTIVE_KEY, {"r1": T0})

    def age(self, s):
        self.now += s

    def sweep(self):
        acts = self.reaper.sweep()
        self.h.pump()
        return acts

    def pid(self, i):
        return self.h.kids()[i][0]

    def step(self):
        return self.h.doc()["steps"]["s"]


def srig(items, **kw):
    return SRig(items, **kw)


def test_healthy_children_untouched():
    g = srig(["a", "b"])
    for i in (0, 1):
        g.status[g.pid(i)] = "job-started"
        g.h.child(i, "job-started")
    g.age(10_000)
    assert g.sweep() == []
    assert g.h.counts()["completed"] == 0 and len(g.h.jobs) == 2


def test_missed_terminal_event_replays_the_stored_event_and_the_step_finishes():
    g = srig(["a", "b"])
    g.h.ok(0)
    pid = g.pid(1)
    g.status[pid] = "job-completed"
    ev = g.h.child_ev(1, outputs={"o": "o1", "p": "p1"})
    g.h.r.set(ws.k_event(pid, "job-completed"), json.dumps(ev))
    g.age(700)
    g.sweep()  # grace clock starts
    g.age(200)
    acts = g.sweep()
    assert any("missed terminal event (job-completed)" in a and "s[1]" in a for _, a in acts)
    d = g.h.doc()
    assert d["status"] == "completed" and d["outputs"]["res"] == ["o0", "o1"]
    assert g.h.r.get(ws.k_event(pid, "job-completed")) is None  # engine consumed it


def test_missed_completion_without_stored_event_is_failed_lost_and_retried():
    g = srig(["a", "b"], retries=1)
    g.h.ok(0)
    old = g.pid(1)
    g.status[old] = "job-completed"
    g.age(700)
    g.sweep()
    g.age(200)
    acts = g.sweep()
    assert any("failed as lost" in a for _, a in acts)
    new = g.pid(1)
    assert new != old and g.h.counts()["retries"] == 1 and g.step()["state"] == "running"


def test_missed_failure_replayed_and_counts_toward_the_threshold():
    g = srig(["a", "b"], threshold=0.5)
    g.h.ok(0)
    g.status[g.pid(1)] = "job-failed"
    g.age(700)
    g.sweep()
    g.age(200)
    g.sweep()
    d = g.h.doc()
    assert d["status"] == "completed" and d["outputs"]["res"] == ["o0", None]
    assert (g.h.counts()["completed"], g.h.counts()["failed"]) == (1, 1)


def test_stuck_queued_child_is_lost_and_below_threshold_fails_the_step():
    g = srig(["a", "b"])  # threshold 1.0, no retries
    g.h.ok(0)
    g.age(700)
    assert g.sweep() == []  # the clock on the missing status starts at first sight
    g.age(700)
    acts = g.sweep()
    assert any("stuck queued" in a and "s[1]" in a for _, a in acts)
    d = g.h.doc()
    assert d["status"] == "failed" and d["error_class"] == "scatter_threshold"


def test_stuck_queued_child_is_resubmitted_within_retries():
    g = srig(["a"], retries=1)
    g.age(700)
    g.sweep()
    g.age(700)
    g.sweep()
    assert len(g.h.jobs) == 2 and g.h.kids()[0][1]["params"] == {"v": "a"}
    assert g.h.counts()["retries"] == 1 and g.h.doc()["status"] == "running"


def test_vanished_running_child_is_offline():
    g = srig(["a", "b"], threshold=0.5)
    g.h.ok(0)
    g.h.child(1, "job-started")
    g.age(700)
    g.sweep()
    g.age(400)
    acts = g.sweep()
    assert any("job vanished" in a and "s[1]" in a for _, a in acts)
    d = g.h.doc()
    assert d["status"] == "completed" and d["outputs"]["res"] == ["o0", None]


def test_missed_job_started_replayed():
    g = srig(["a"])
    g.status[g.pid(0)] = "job-started"
    g.age(700)
    g.sweep()
    g.age(200)
    acts = g.sweep()
    assert any("missed job-started" in a for _, a in acts)
    assert ws.scatter_child(g.h.r, "r1", "s", 0)["state"] == "running"


def test_status_is_looked_up_by_the_recorded_task_id():
    g = srig(["a"])
    pid = g.pid(0)
    ws.record_task_id(g.h.r, "r1", pid, "task-0")
    g.status["task-0"] = "job-failed"
    g.age(700)
    g.sweep()
    g.age(200)
    acts = g.sweep()
    assert any("missed terminal event (job-failed)" in a for _, a in acts)
    assert g.h.doc()["status"] == "failed"


def test_lost_scatter_finish_is_finished():
    g = srig(["a", "b"])
    g.h.eng._scatter_after = lambda *a, **k: "skipped"  # crash after the last child settled
    g.h.ok(0)
    g.h.ok(1)
    del g.h.eng._scatter_after
    assert g.step()["state"] == "running" and g.h.counts()["completed"] == 2
    g.age(700)
    acts = g.sweep()
    assert any("scatter finish missed s" in a for _, a in acts)
    assert g.h.doc()["status"] == "completed"


def test_child_stuck_in_retry_wait_is_resubmitted():
    g = srig(["a"], retries=1)
    old = g.pid(0)
    assert ws.scatter_op(g.h.r, "r1", "s", "retry_mark", 0, old)[0]  # crash right after the retry decision
    g.age(700)
    g.sweep()
    g.age(200)
    acts = g.sweep()
    assert any("child retry lost s[0]" in a for _, a in acts)
    assert g.pid(0) != old and ws.scatter_child(g.h.r, "r1", "s", 0)["state"] == "queued"


def test_interrupted_fanout_is_resumed():
    g = srig(["a", "b", "c"])
    # simulate a crash after child 0 was submitted: drop the other claims and jobs
    for i in (1, 2):
        g.h.r.hdel(ws.k_sc("r1", "s"), str(i))
    g.h.jobs[:] = g.h.jobs[:1]
    g.age(700)
    g.sweep()
    g.age(700)
    acts = g.sweep()
    assert any("fan-out resumed" in a for _, a in acts)
    assert sorted(g.h.kids()) == [0, 1, 2]


def test_halting_drain_cancels_queued_children_and_recovers_the_running_one():
    g = srig(["a", "b", "c"], threshold=0.3)
    g.h.child(0, "job-started")
    g.status[g.pid(0)] = "job-completed"  # child 0 finished, its event is lost
    ev = g.h.child_ev(0, outputs={"o": "o0", "p": "p0"})
    g.h.r.set(ws.k_event(g.pid(0), "job-completed"), json.dumps(ev))
    # another step halts the run (drain) -- put the run into halting directly, as the engine would
    def halt(d):
        d = dict(d, status="halting", reason="x", failed_step="s", version=d["version"] + 1)
        return d, None
    g.h.eng._update("r1", halt)
    g.age(700)
    g.sweep()  # drain re-run: queued children 1, 2 cancelled; grace clock starts for child 0
    c = g.h.counts()
    assert c["failed"] == 2 and c["completed"] == 0
    g.age(200)
    acts = g.sweep()
    assert any("missed terminal event" in a and "s[0]" in a for _, a in acts)
    assert g.step()["state"] == "completed" and g.h.counts()["completed"] == 1


def test_lock_respected_for_scatter_recovery():
    g = srig(["a"])
    g.h.r.set(wr.k_lock("r1"), "other", ex=60)
    g.age(700)
    g.sweep()
    g.age(700)
    assert g.sweep() == [] and g.step()["state"] == "running"
