"""workflow v2, S6: on-error modes (kill/drain/continue), halts under each mode, ``halting`` -> final via the
settled set, user cancel, duplicates, and the direct (OpenSearch-free) revoke. fakeredis, no broker."""
from unittest import mock

import fakeredis
import pytest
from test_workflow_engine import Harness, _src, _step, make_spec

from hysds import workflow_engine as we
from hysds import workflow_rules
from hysds import workflow_state as ws
from hysds_commons.workflow_run2 import validate_run_doc

MODES = ("kill", "drain", "continue")


# f fails; r runs; q is only queued; n waits for r; w waits for f
def table_steps():
    return {
        "f": _step(out=("o",)),
        "r": _step(out=("o",)),
        "q": _step(out=("o",)),
        "w": _step(deps=["f"], ins={"v": _src("$.f.o")}),
        "n": _step(deps=["r"], ins={"v": _src("$.r.o")}),
    }


def harness(steps, mode, outputs=None, loosen=None, **kw):
    outputs = {k: dict(v) for k, v in (outputs or {}).items()}
    spec = make_spec(steps, outputs=outputs, on_error=mode, **kw)
    for name, typ in (loosen or {}).items():  # bypass the registration check (B21) to test the run-time one
        spec["outputs"][name]["type"] = typ
    h = Harness(spec)
    h.start()
    return h


def started(h, step):
    h.finish(step, status="job-started")


def fail(h, step, ec="worker", status="job-failed"):
    return h.finish(step, status=status, error_class=ec, short_error="boom")


def states(h):
    return {s: st["state"] for s, st in h.doc()["steps"].items()}


def submitted(h):
    return sorted({j["workflow"]["step_id"] for _, j in h.jobs})


def pid(h, step):
    return h.job_of(step)[0]


def check_valid(h):
    assert validate_run_doc(h.doc()) == []


# ---------------------------------------------------------------- the 6.6 table: failure trigger


def test_table_kill_failure():
    h = harness(table_steps(), "kill")
    started(h, "r")
    fail(h, "f")
    d = h.doc()
    assert d["status"] == "failed" and d["failed_step"] == "f" and d["error_class"] == "worker"
    assert states(h) == {"f": "failed", "r": "cancelled", "q": "cancelled", "w": "cancelled", "n": "cancelled"}
    assert sorted(h.revoked[-1][1]) == sorted([pid(h, "r"), pid(h, "q")])  # queued AND running
    assert d["steps"]["w"]["cancelled_by"]["kind"] == "failure"
    assert ws.n_done(h.r, "r1") == 5
    check_valid(h)


def test_table_drain_failure_running_job_completes():
    h = harness(table_steps(), "drain", outputs={"res": {"type": "string?", "source": ["$.r.o"]}})
    started(h, "r")
    fail(h, "f")
    d = h.doc()
    assert d["status"] == "halting" and d["failed_step"] == "f"
    # queued revoked, running untouched, not-yet-started cancelled
    assert h.revoked[-1][1] == [pid(h, "q")]
    assert states(h) == {"f": "failed", "r": "running", "q": "cancelled", "w": "cancelled", "n": "cancelled"}
    assert d["steps"]["n"]["cancelled_by"]["kind"] == "failure"
    check_valid(h)
    h.finish("r", {"o": "R"})  # the running job completes: its output is kept
    d = h.doc()
    assert d["status"] == "failed" and d["failed_step"] == "f" and d["ended_at"]
    assert ws.settled_map(h.r, "r1")["r"]["outputs"] == {"o": "R"}
    assert "n" not in submitted(h)  # n never started: drain does not release anything
    assert ws.n_done(h.r, "r1") == 5
    assert h.r.zrange(ws.ACTIVE_KEY, 0, -1) == []
    check_valid(h)


def test_table_drain_failure_running_job_also_fails():
    h = harness(table_steps(), "drain")
    started(h, "r")
    fail(h, "f", ec="worker")
    assert h.doc()["status"] == "halting"
    fail(h, "r", ec="offline", status="job-offline")
    d = h.doc()
    assert d["status"] == "failed" and d["failed_step"] == "f" and d["error_class"] == "worker"  # first failure wins
    assert d["steps"]["r"]["state"] == "failed"
    check_valid(h)


def test_drain_no_retry_while_halting():
    steps = table_steps()
    steps["r"] = _step(out=("o",), max_retries=2, retry_on=["worker"])
    h = harness(steps, "drain")
    started(h, "r")
    fail(h, "f")
    n_jobs = len(h.jobs)
    fail(h, "r")  # retryable, but the run is halting
    assert len(h.jobs) == n_jobs and h.doc()["status"] == "failed"


def test_drain_nothing_running_ends_immediately():
    h = harness({"f": _step(), "w": _step(deps=["f"], ins={"v": _src("$.f.o")})}, "drain")
    fail(h, "f")
    d = h.doc()
    assert d["status"] == "failed" and states(h) == {"f": "failed", "w": "cancelled"}
    check_valid(h)


def test_table_continue_failure():
    h = harness(table_steps(), "continue")
    started(h, "r")
    fail(h, "f")
    d = h.doc()
    assert d["status"] == "running" and d["failed_step"] == "f"  # the first failure is recorded already
    assert h.revoked == []  # nothing revoked
    assert states(h) == {"f": "failed", "r": "running", "q": "queued", "w": "cancelled", "n": "waiting"}
    assert d["steps"]["w"]["cancelled_by"] == {"kind": "upstream_failed", "step": "f", "reason": "worker"}
    h.finish("q", {"o": "Q"})
    assert h.doc()["status"] == "running"
    h.finish("r", {"o": "R"})  # independent branch goes on: n is released
    assert states(h)["n"] == "queued" and "n" in submitted(h)
    h.finish("n", {"o": "N"})
    d = h.doc()
    assert d["status"] == "failed" and d["failed_step"] == "f" and d["error_class"] == "worker"
    assert "w" not in submitted(h)
    assert ws.n_done(h.r, "r1") == 5
    check_valid(h)


def test_continue_cancellation_is_transitive_and_counts_as_settled():
    steps = {"f": _step(), "a": _step(deps=["f"], ins={"v": _src("$.f.o")}),
             "b": _step(deps=["a"], ins={"v": _src("$.a.o")}), "i": _step()}
    h = harness(steps, "continue")
    fail(h, "f")
    assert states(h) == {"f": "failed", "a": "cancelled", "b": "cancelled", "i": "queued"}
    assert ws.n_done(h.r, "r1") == 3
    h.finish("i", {"o": "I"})
    d = h.doc()
    assert d["status"] == "failed" and ws.n_done(h.r, "r1") == 4
    assert d["steps"]["b"]["cancelled_by"]["step"] == "f"


def test_continue_failed_step_never_null_for_first_non_null():
    steps = {"a": _step(), "b": _step(),
             "k": _step(deps=["a", "b"], ins={"v": _src("$.a.o", "$.b.o", pick="first_non_null")})}
    h = harness(steps, "continue")
    fail(h, "a")
    h.finish("b", {"o": "B"})
    assert states(h)["k"] == "cancelled"  # NOT run with b's value, and not null
    assert "k" not in submitted(h)
    assert h.doc()["status"] == "failed"


def test_continue_all_independent_failures_end_failed_once():
    h = harness({"a": _step(), "b": _step()}, "continue")
    fail(h, "a")
    assert h.doc()["status"] == "running"
    fail(h, "b")
    d = h.doc()
    assert d["status"] == "failed" and d["failed_step"] == "a"


# ---------------------------------------------------------------- halts under each mode


def halt_steps(status):
    s = table_steps()
    del s["f"], s["w"]
    s["g"] = _step(out=("bad", "o"))
    s["h"] = {"halt": {"status": status, "reason": "qa says stop"}, "deps": ["g"], "when": "$.g.bad"}
    return s


OUT = {"res": {"type": "string?", "source": ["$.r.o"]}}


@pytest.mark.parametrize("mode", MODES)
def test_halt_failed_each_mode(mode):
    h = harness(halt_steps("failed"), mode, outputs=OUT)
    started(h, "r")
    h.finish("g", {"bad": True, "o": "G"})
    d = h.doc()
    assert d["steps"]["h"]["halt"]["fired"] is True and d["steps"]["h"]["state"] == "completed"
    if mode == "kill":
        assert d["status"] == "failed"
        assert states(h)["r"] == "cancelled" and sorted(h.revoked[-1][1]) == sorted([pid(h, "r"), pid(h, "q")])
    else:  # continue behaves like drain for a halt (B19)
        assert d["status"] == "halting" and h.revoked[-1][1] == [pid(h, "q")]
        assert states(h)["r"] == "running" and states(h)["q"] == "cancelled" and states(h)["n"] == "cancelled"
        h.finish("r", {"o": "R"})
        d = h.doc()
        assert d["status"] == "failed"
    assert d["error_class"] == "halted" and d["reason"] == "qa says stop" and d["failed_step"] == "h"
    assert d["steps"]["n"]["cancelled_by"]["kind"] == "halt"
    assert ws.n_done(h.r, "r1") == 5
    check_valid(h)


@pytest.mark.parametrize("mode", MODES)
def test_halt_completed_each_mode(mode):
    h = harness(halt_steps("completed"), mode, outputs=OUT)
    started(h, "r")
    h.finish("g", {"bad": True, "o": "G"})
    if mode == "kill":
        d = h.doc()
        assert d["status"] == "completed" and d["outputs"] == {"res": None}  # r was killed: optional output null
    else:
        assert h.doc()["status"] == "halting"
        h.finish("r", {"o": "R"})
        d = h.doc()
        assert d["status"] == "completed" and d["outputs"] == {"res": "R"}  # drained job's output is kept
    assert d["reason"] == "qa says stop" and not d.get("error_class")
    assert d["steps"]["h"]["halt"]["fired"] is True
    check_valid(h)


@pytest.mark.parametrize("mode", MODES)
def test_halt_completed_required_output_must_resolve(mode):
    # B21 at run time: a required output that cannot resolve fails the run with output_resolution
    h = harness(halt_steps("completed"), mode, outputs={"res": {"type": "string?", "source": ["$.n.o"]}},
                loosen={"res": "string"})
    h.finish("g", {"bad": True, "o": "G"})  # r, q only queued: revoked (drain) or killed, n cancelled -> null
    d = h.doc()
    assert d["status"] == "failed" and d["error_class"] == "output_resolution"
    check_valid(h)


def test_halt_completed_under_kill_still_resolves_present_required_output():
    h = harness(halt_steps("completed"), "kill", outputs={"res": {"type": "string?", "source": ["$.g.o"]}},
                loosen={"res": "string"})
    h.finish("g", {"bad": True, "o": "G"})
    d = h.doc()
    assert d["status"] == "completed" and d["outputs"] == {"res": "G"}


def test_halt_not_fired_when_flag_false_run_completes():
    h = harness({"g": _step(out=("bad",)),
                 "h": {"halt": {"status": "failed", "reason": "x"}, "deps": ["g"], "when": "$.g.bad"}}, "drain")
    h.finish("g", {"bad": False})
    d = h.doc()
    assert d["status"] == "completed" and d["steps"]["h"]["state"] == "skipped"


def test_halt_under_continue_after_a_failure_run_is_failed():
    steps = {"f": _step(), "g": _step(out=("bad",)),
             "h": {"halt": {"status": "completed", "reason": "stop"}, "deps": ["g"], "when": "$.g.bad"}}
    h = harness(steps, "continue", outputs={"res": {"type": "string?", "source": ["$.f.o"]}})
    fail(h, "f")
    assert h.doc()["status"] == "running"
    h.finish("g", {"bad": True})
    d = h.doc()
    assert d["status"] == "failed" and d["failed_step"] == "f"  # a failure outranks a completed halt
    check_valid(h)


def test_running_job_failing_under_completed_halt_drain_fails_the_run():
    h = harness(halt_steps("completed"), "drain", outputs=OUT)
    started(h, "r")
    h.finish("g", {"bad": True, "o": "G"})
    assert h.doc()["status"] == "halting"
    fail(h, "r")
    d = h.doc()
    assert d["status"] == "failed" and d["failed_step"] == "r"


# ---------------------------------------------------------------- user cancel is always kill


@pytest.mark.parametrize("mode", ("drain", "continue"))
def test_user_cancel_is_kill_whatever_the_mode(mode):
    h = harness(table_steps(), mode)
    started(h, "r")
    fail(h, "f")  # drain: halting; continue: running with r running and q queued
    h.send({"kind": "cancel_requested", "run_id": "r1", "username": "alice"})
    d = h.doc()
    assert d["status"] == "cancelled" and d["cancelled_by"] == "alice"
    assert pid(h, "r") in h.revoked[-1][1]  # the running job is killed too
    assert not [s for s, st in d["steps"].items() if st["state"] in ("queued", "running", "waiting")]
    check_valid(h)
    h.send({"kind": "cancel_requested", "run_id": "r1", "username": "bob"})  # idempotent
    assert h.doc()["cancelled_by"] == "alice"


# ---------------------------------------------------------------- duplicates, atomicity, snapshots


@pytest.mark.parametrize("mode", MODES)
def test_duplicate_failure_event_is_a_noop(mode):
    h = harness(table_steps(), mode)
    started(h, "r")
    ev = fail(h, "f")
    v, n_rev = h.doc()["version"], len(h.revoked)
    h.send(ev)
    h.send(ev)
    assert h.doc()["version"] == v and len(h.revoked) == n_rev


def test_duplicate_completion_in_halting_does_not_double_settle_or_finalize_early():
    h = harness(table_steps(), "drain")
    started(h, "r")
    started(h, "q")
    fail(h, "f")
    assert h.doc()["status"] == "halting"
    ev = h.finish("q", {"o": "Q"})  # q was still queued (revoked): its late completion is ignored
    v = h.doc()["version"]
    assert h.doc()["status"] == "halting"
    h.send(ev)
    assert h.doc()["version"] == v and h.doc()["status"] == "halting"
    ev_r = h.finish("r", {"o": "R"})
    assert h.doc()["status"] == "failed"
    v = h.doc()["version"]
    h.send(ev_r)  # duplicate after the run ended
    assert h.doc()["version"] == v


def test_halting_finalize_replay_after_settle_heals():
    # a crash between the last settle and finalize: a duplicate completion event finishes the run
    h = harness(table_steps(), "drain")
    started(h, "r")
    fail(h, "f")
    with mock.patch.object(we.Engine, "_finalize", lambda self, run_id: None):
        ev = h.finish("r", {"o": "R"})
    assert h.doc()["status"] == "halting" and ws.n_done(h.r, "r1") == 5
    h.send(ev)  # the duplicate completion event heals it
    assert h.doc()["status"] == "failed"
    check_valid(h)


@pytest.mark.parametrize("mode", MODES)
def test_snapshot_after_every_change_incl_halting(mode):
    h = harness(table_steps(), mode)
    started(h, "r")
    fail(h, "f")
    h.finish("q", {"o": "Q"})
    h.finish("r", {"o": "R"})
    if mode == "continue":
        h.finish("n", {"o": "N"})
    snaps = h.snapshots()
    vs = [s["version"] for s in snaps]
    assert vs == sorted(vs) and len(set(vs)) == len(vs) and vs[-1] == h.doc()["version"]
    assert int(h.r.get("wf:ver:r1")) == vs[-1]
    if mode == "drain":
        assert "halting" in [s["status"] for s in snaps]
    assert snaps[-1]["status"] == "failed"


def test_failure_of_an_already_cancelled_step_is_ignored():
    h = harness({"a": _step(), "b": _step()}, "drain")
    fail(h, "a")
    d = h.doc()
    assert d["status"] == "failed" and d["steps"]["b"]["state"] == "cancelled"
    v = d["version"]
    fail(h, "b")
    assert h.doc()["version"] == v


# ---------------------------------------------------------------- late revoke + task ids


def test_job_queued_event_records_task_id():
    h = harness({"a": _step()}, "kill")
    h.send({"run_id": "r1", "step_id": "a", "status": "job-queued", "payload_id": pid(h, "a"),
            "job_id": "x", "task_id": "task-A"})
    assert ws.get_task_id(h.r, "r1", pid(h, "a")) == "task-A"
    assert h.doc()["steps"]["a"]["state"] == "queued"


def test_cancelled_attempt_revoked_again_when_its_task_id_arrives():
    h = harness(table_steps(), "drain")
    fail(h, "f")  # q (queued, task id still unknown) is revoked by payload id only
    assert pid(h, "q") in h.revoked[-1][1]
    n = len(h.revoked)
    note = h.send({"run_id": "r1", "step_id": "q", "status": "job-queued", "payload_id": pid(h, "q"),
                   "job_id": "x", "task_id": "task-Q"})
    assert "late revoke" in note and len(h.revoked) == n + 1 and h.revoked[-1][1] == [pid(h, "q")]
    assert ws.get_task_id(h.r, "r1", pid(h, "q")) == "task-Q"
    # a job that started anyway (the kill raced) is revoked too, also after the run ended
    h.send({"run_id": "r1", "step_id": "q", "status": "job-started", "payload_id": pid(h, "q"),
            "job_id": "x", "task_id": "task-Q"})
    assert len(h.revoked) == n + 2


# ---------------------------------------------------------------- the direct revoke (celery mocked)


class FakeControl:
    def __init__(self):
        self.calls = []

    def revoke(self, task_id, **kw):
        self.calls.append((task_id, kw))


def test_make_revoke_fn_revokes_task_ids_directly_with_marker():
    r = fakeredis.FakeStrictRedis()
    ws.record_task_id(r, "r1", "p1", "task-1")
    ws.record_task_id(r, "r1", "p2", "task-2")
    ctl = FakeControl()
    fn = workflow_rules.make_revoke_fn(r, ctl, marker_ttl=77, marker_tmpl="hysds-revoked-task-%s")
    fn("r1", ["p1", "p2", "p3"], "alice")
    assert ctl.calls == [
        ("p1", {}), ("task-1", {"terminate": True, "signal": "SIGTERM"}),
        ("p2", {}), ("task-2", {"terminate": True, "signal": "SIGTERM"}),
        ("p3", {}),  # no task id yet: only the orchestrator task id
    ]
    assert r.get("hysds-revoked-task-task-1") is not None and 0 < r.ttl("hysds-revoked-task-task-2") <= 77
    assert r.get("hysds-revoked-task-p1") is None  # the marker is keyed by the run_job task id


def test_revoke_marker_template_matches_job_worker():
    from hysds.log_utils import REVOKED_TASK_TMPL
    assert REVOKED_TASK_TMPL == "hysds-revoked-task-%s"


def test_make_revoke_fn_one_failure_does_not_stop_the_rest():
    r = fakeredis.FakeStrictRedis()
    ws.record_task_id(r, "r1", "p1", "t1")
    ws.record_task_id(r, "r1", "p2", "t2")
    ctl = mock.MagicMock()
    ctl.revoke.side_effect = [RuntimeError("broker"), None, None, None]
    fn = workflow_rules.make_revoke_fn(r, ctl, marker_ttl=5, marker_tmpl="m-%s")
    fn("r1", ["p1", "p2"], None)  # does not raise
    assert mock.call("t2", terminate=True, signal="SIGTERM") in ctl.revoke.call_args_list


def test_engine_kill_and_user_cancel_call_direct_revoke_with_recorded_task_ids():
    ctl = FakeControl()
    h = harness(table_steps(), "kill")
    h.eng.revoke_fn = workflow_rules.make_revoke_fn(h.r, ctl, marker_ttl=60, marker_tmpl="m-%s")
    for step, tid in (("r", "task-r"), ("q", "task-q")):
        h.send({"run_id": "r1", "step_id": step, "status": "job-queued", "payload_id": pid(h, step),
                "job_id": "x", "task_id": tid})
    started(h, "r")
    fail(h, "f")
    assert sorted(c[0] for c in ctl.calls if c[1].get("terminate")) == ["task-q", "task-r"]
    assert h.r.get("m-task-r") is not None

    h2 = harness({"a": _step()}, "drain")
    h2.eng.revoke_fn = workflow_rules.make_revoke_fn(h2.r, ctl, marker_ttl=60, marker_tmpl="m-%s")
    h2.send({"run_id": "r1", "step_id": "a", "status": "job-queued", "payload_id": pid(h2, "a"),
             "job_id": "x", "task_id": "task-a"})
    h2.send({"kind": "cancel_requested", "run_id": "r1", "username": "u"})
    assert ("task-a", {"terminate": True, "signal": "SIGTERM"}) in ctl.calls
