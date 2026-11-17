"""workflow v2 engine, scatter (S5): one step, N children tracked in Redis, index-aligned gather. fakeredis, no broker."""
import fakeredis  # noqa: F401  (the Harness uses it)
import pytest
from test_workflow_engine import FROZEN, Harness, _src, _step, make_spec  # noqa: F401

from hysds import workflow_state as ws
from hysds_commons.workflow_run2 import validate_run_doc

def spec_for(threshold=1.0, downstream=False, retries=None, out=("o", "p")):
    sc = {"input": "v", "threshold": threshold}
    extra = dict(max_retries=retries, retry_on=["worker"]) if retries else {}
    steps = {"s": _step(ins={"v": _src("$.inputs.items")}, out=out, scatter=sc, **extra)}
    outputs = {"res": {"type": "string[]?", "source": ["$.s.o"]}}
    if downstream:
        steps["d"] = _step(deps=["s"], ins={"w": _src("$.s.o", link_merge="merge_flattened", pick="all_non_null")})
        outputs = {"res": {"type": "string[]", "source": ["$.d.o"]}}
    return make_spec(steps, outputs=outputs, inputs={"items": {"type": "Any?"}})


class SH(Harness):
    """Scatter harness: children addressed by index."""

    def kids(self, step="s"):
        """{child_index: (payload_id, job)} of the latest job of each child."""
        out = {}
        for pid, j in self.jobs:
            w = j["workflow"]
            if w["step_id"] == step and w["child_index"] is not None:
                out[w["child_index"]] = (pid, j)
        return out

    def child_ev(self, i, status="job-completed", outputs=None, step="s", pid=None, **kw):
        pid = pid or self.kids(step)[i][0]
        ev = {"run_id": "r1", "step_id": step, "child_index": i, "status": status, "job_id": pid, "payload_id": pid,
              "workflow_outputs": [{"name": k, "value_s": v} for k, v in (outputs or {}).items()],
              "error_class": kw.pop("error_class", None)}
        ev.update(kw)
        return ev

    def child(self, i, status="job-completed", outputs=None, **kw):
        ev = self.child_ev(i, status, outputs, **kw)
        self.send(ev)
        self.pump()
        return ev

    def ok(self, i):
        return self.child(i, outputs={"o": "o%d" % i, "p": "p%d" % i})

    def bad(self, i, ec="worker"):
        return self.child(i, "job-failed", error_class=ec, short_error="boom %d" % i)

    def counts(self, step="s"):
        return ws.scatter_counts(self.r, "r1", step)


def run_scatter(items, **kw):
    h = SH(spec_for(**kw), inputs={"items": items})
    h.start()
    return h


# ---------------------------------------------------------------------------------------- sizes


def test_zero_items_completes_immediately_with_empty_arrays():
    h = run_scatter([])
    d = h.doc()
    assert h.jobs == [] and d["status"] == "completed"
    assert d["outputs"] == {"res": []}
    assert d["steps"]["s"]["state"] == "completed" and d["steps"]["s"]["scatter"]["expected"] == 0
    assert ws.settled_map(h.r, "r1")["s"]["outputs"] == {"o": [], "p": []}
    assert validate_run_doc(d) == []


def test_one_item():
    h = run_scatter(["a"])
    (pid, job), = h.kids().values()
    assert job["workflow"] == {"run_id": "r1", "step_id": "s", "spec_id": "t", "child_index": 0, "child_total": 1}
    assert job["params"] == {"v": "a"} and job["queue"] == "q1"
    h.ok(0)
    d = h.doc()
    assert d["status"] == "completed" and d["outputs"] == {"res": ["o0"]}


def test_n_items_all_succeed_and_outputs_are_index_aligned():
    items = ["i%d" % k for k in range(7)]
    h = run_scatter(items)
    kids = h.kids()
    assert sorted(kids) == list(range(7))
    assert [kids[i][1]["params"]["v"] for i in range(7)] == items
    assert {j["workflow"]["child_total"] for _, j in kids.values()} == {7}
    assert h.doc()["steps"]["s"]["state"] == "running" and h.doc()["steps"]["s"]["scatter"]["expected"] == 7
    for i in (3, 0, 6, 1, 5, 2):  # completion order differs from index order
        h.ok(i)
    assert h.doc()["status"] == "running"
    h.ok(4)
    d = h.doc()
    assert d["status"] == "completed"
    assert d["outputs"]["res"] == ["o%d" % i for i in range(7)]
    assert ws.settled_map(h.r, "r1")["s"]["outputs"]["p"] == ["p%d" % i for i in range(7)]
    sc = d["steps"]["s"]["scatter"]
    assert (sc["expected"], sc["n_completed"], sc["n_failed"]) == (7, 7, 0)
    assert sc["children"] == [kids[i][0] for i in range(7)]
    assert d["steps"]["s"]["attempts"] == [] and validate_run_doc(d) == []


# ---------------------------------------------------------------------------------------- threshold


def test_failures_within_threshold_complete_with_null_at_failed_indices():
    h = run_scatter(list("abcde"), threshold=0.6)
    h.ok(0), h.bad(1), h.ok(2), h.bad(3), h.ok(4)
    d = h.doc()
    assert d["status"] == "completed", d
    assert ws.settled_map(h.r, "r1")["s"]["outputs"]["o"] == ["o0", None, "o2", None, "o4"]
    assert d["outputs"]["res"] == ["o0", None, "o2", None, "o4"]
    assert (d["steps"]["s"]["scatter"]["n_completed"], d["steps"]["s"]["scatter"]["n_failed"]) == (3, 2)


def test_failures_below_threshold_fail_the_step_with_scatter_threshold():
    h = run_scatter(list("abcde"), threshold=0.6)
    h.ok(0), h.bad(1), h.bad(2), h.bad(3), h.ok(4)
    d = h.doc()
    assert d["status"] == "failed" and d["error_class"] == "scatter_threshold" and d["failed_step"] == "s"
    assert d["steps"]["s"]["state"] == "failed" and "2 of 5 children completed" in d["steps"]["s"]["error"]
    assert "boom 1" in d["steps"]["s"]["error"]
    assert validate_run_doc(d) == []


def test_a_failed_child_never_kills_the_run_before_the_threshold_decides():
    h = run_scatter(list("abc"), threshold=0.3)
    h.bad(0)
    d = h.doc()
    assert d["status"] == "running" and d["steps"]["s"]["state"] == "running"
    assert h.revoked == []
    h.bad(1)  # 1 of 3 may still complete: the decision waits for the last child
    assert h.doc()["status"] == "running"
    h.ok(2)
    assert h.doc()["status"] == "completed"  # 1/3 >= 0.3


def test_default_threshold_one_fails_on_any_failure():
    h = run_scatter(list("ab"))
    h.ok(0), h.bad(1)
    assert h.doc()["error_class"] == "scatter_threshold"


def test_all_children_fail():
    h = run_scatter(list("ab"), threshold=0.5)
    h.bad(0), h.bad(1)
    assert h.doc()["error_class"] == "scatter_threshold"


# ---------------------------------------------------------------------------------------- retries


def test_child_retry_then_success_keeps_the_index():
    h = run_scatter(list("ab"), retries=1)
    first = h.kids()[1][0]
    h.bad(1)
    pid, job = h.kids()[1]
    assert pid != first and job["workflow"]["child_index"] == 1 and job["params"] == {"v": "b"}
    assert job["retry_count"] == 1
    assert h.doc()["status"] == "running" and h.counts()["retries"] == 1
    h.ok(0), h.ok(1)
    d = h.doc()
    assert d["status"] == "completed" and d["outputs"]["res"] == ["o0", "o1"]
    assert d["steps"]["s"]["scatter"]["children"][1] == pid  # the doc names the latest job of the child


def test_child_retries_are_exhausted_then_the_child_fails():
    h = run_scatter(["a"], retries=1)
    h.bad(0)
    h.bad(0)
    assert len(h.jobs) == 2 and h.counts()["failed"] == 1
    assert h.doc()["error_class"] == "scatter_threshold"


def test_non_matching_error_class_is_not_retried():
    h = run_scatter(["a"], retries=3)
    h.bad(0, ec="offline")
    assert len(h.jobs) == 1


def test_retry_submit_failure_reverts_and_redelivery_retries():
    h = run_scatter(["a"], retries=1)
    ev = h.child_ev(0, "job-failed", error_class="worker")
    h.fail_submit = 1
    with pytest.raises(RuntimeError):
        h.send(ev)
    assert len(h.jobs) == 1 and ws.scatter_child(h.r, "r1", "s", 0)["state"] == "retry_wait"
    h.send(ev)  # redelivered
    assert len(h.jobs) == 2 and ws.scatter_child(h.r, "r1", "s", 0)["attempt"] == 2


# ---------------------------------------------------------------------------------------- duplicates


def test_duplicate_child_events_are_harmless():
    h = run_scatter(list("abc"))
    e0 = h.ok(0)
    h.send(e0), h.send(e0)
    assert h.counts()["completed"] == 1
    h.bad(1)
    h.send(h.child_ev(1, "job-failed", error_class="worker"))
    assert h.counts()["failed"] == 1
    h.ok(2)
    d = h.doc()
    assert d["status"] == "failed"
    h.send(e0)  # after the end
    h.send(h.child_ev(2, outputs={"o": "x"}))
    assert h.doc() == d


def test_duplicate_completion_after_the_step_settled_replays_the_join():
    h = SH(spec_for(downstream=True), inputs={"items": ["a"]})
    h.start()
    ev = h.ok(0)
    assert len([1 for _, j in h.jobs if j["workflow"]["step_id"] == "d"]) == 1
    h.send(ev)
    assert len([1 for _, j in h.jobs if j["workflow"]["step_id"] == "d"]) == 1


def test_stale_event_of_a_retried_attempt_and_unknown_child_are_ignored():
    h = run_scatter(["a", "b"], retries=1)
    old = h.kids()[0][0]
    h.bad(0)
    n = len(h.jobs)
    h.send(h.child_ev(0, "job-failed", pid=old, error_class="worker"))  # the old attempt again
    h.send(h.child_ev(0, outputs={"o": "z"}, pid=old))
    h.send(h.child_ev(1, outputs={"o": "z"}, pid="foreign"))
    assert len(h.jobs) == n and h.counts()["completed"] == 0 and h.counts()["failed"] == 0


def test_started_event_marks_the_child_running_without_a_snapshot():
    h = run_scatter(["a"])
    before = len(h.snapshots())
    h.child(0, "job-started")
    assert ws.scatter_child(h.r, "r1", "s", 0)["state"] == "running" and len(h.snapshots()) == before


# ---------------------------------------------------------------------------------------- downstream


def test_downstream_merge_flattened_all_non_null_drops_failed_children():
    h = SH(spec_for(threshold=0.5, downstream=True), inputs={"items": list("abcd")})
    h.start()
    h.ok(0), h.bad(1), h.ok(2), h.ok(3)
    pid, job = [(p, j) for p, j in h.jobs if j["workflow"]["step_id"] == "d"][-1]
    assert job["params"] == {"w": ["o0", "o2", "o3"]}
    h.finish("d", {"o": "done"})
    assert h.doc()["status"] == "completed"


def test_downstream_of_an_empty_scatter_gets_an_empty_list():
    h = SH(spec_for(downstream=True), inputs={"items": []})
    h.start()
    assert h.job_of("d")[1]["params"] == {"w": []}


# ---------------------------------------------------------------------------------------- B20


@pytest.mark.parametrize("value", [None, "abc", {"a": 1}])
def test_null_or_non_list_scatter_input_is_a_run_time_error(value):
    h = run_scatter(value)
    d = h.doc()
    assert d["status"] == "failed" and d["error_class"] == "input_resolution" and h.jobs == []


# ---------------------------------------------------------------------------------------- lister step


def lister_spec(retries=None):
    """A mission lister (an ordinary job step whose ``items`` output is the list) feeding the scatter."""
    extra = dict(max_retries=retries, retry_on=["worker"]) if retries else {}
    steps = {"L": _step(ins={}, out=("items",)),
             "s": _step(deps=["L"], ins={"v": _src("$.L.items")}, out=("o", "p"),
                        scatter={"input": "v", "threshold": 1.0}, **extra)}
    return make_spec(steps, outputs={"res": {"type": "string[]?", "source": ["$.s.o"]}})


def test_lister_step_output_is_the_scatter_list():
    h = SH(lister_spec())
    h.start()
    assert [j["workflow"]["step_id"] for _, j in h.jobs] == ["L"]
    h.finish("L", {"items": ["x", "y", "z"]})
    kids = h.kids()
    assert sorted(kids) == [0, 1, 2] and [kids[i][1]["params"]["v"] for i in kids] == ["x", "y", "z"]
    d = h.doc()
    assert d["steps"]["s"]["scatter"]["phase"] == "children" and d["steps"]["s"]["attempts"] == []
    assert validate_run_doc(d) == []
    for i in range(3):
        h.ok(i)
    assert h.doc()["status"] == "completed" and h.doc()["outputs"]["res"] == ["o0", "o1", "o2"]


def test_lister_with_an_empty_list_completes_the_step():
    h = SH(lister_spec())
    h.start()
    h.finish("L", {"items": []})
    assert h.doc()["status"] == "completed" and h.doc()["outputs"] == {"res": []}


def test_lister_failure_fails_the_run_and_the_scatter_never_starts():
    h = SH(lister_spec())
    h.start()
    h.finish("L", None, "job-failed", error_class="worker")
    assert h.doc()["status"] == "failed" and [j["workflow"]["step_id"] for _, j in h.jobs] == ["L"]


def test_children_retry_with_their_item_from_the_lister_output():
    h = SH(lister_spec(retries=1))
    h.start()
    h.finish("L", {"items": ["x", "y"]})
    h.bad(1)
    assert h.kids()[1][1]["params"]["v"] == "y"


# ---------------------------------------------------------------------------------------- snapshots


def test_progress_snapshots_are_throttled_on_a_large_scatter():
    n = 600
    h = run_scatter(list(range(n)))
    base = len(h.snapshots())
    for i in range(n):
        h.ok(i)
    extra = len(h.snapshots()) - base
    assert h.doc()["status"] == "completed"
    assert extra <= 20 + 4, extra  # <= max progress snapshots + finish + settle + finalize, not one per child
    assert h.doc()["steps"]["s"]["scatter"]["children"] == []  # above WORKFLOW_SCATTER_DOC_CHILDREN_MAX: ids stay in Redis
    assert len(ws.scatter_children(h.r, "r1", "s")) == n


def test_small_scatter_snapshots_after_each_child_and_versions_increase():
    h = run_scatter(list("abc"))
    base = len(h.snapshots())
    h.ok(0), h.ok(1)
    vs = [s["version"] for s in h.snapshots()]
    assert len(h.snapshots()) - base == 2 and vs == sorted(vs) and len(set(vs)) == len(vs)


# ---------------------------------------------------------------------------------------- kill / cancel / resume


def test_kill_from_another_step_revokes_live_children():
    steps = {"s": _step(ins={"v": _src("$.inputs.items")}, scatter={"input": "v"}),
             "f": _step(ins={"v": _src("$.inputs.x")})}
    h = SH(make_spec(steps, inputs={"items": {"type": "Any?"}, "x": {"type": "string", "default": "d"}}),
           inputs={"items": list("abc")})
    h.start()
    h.ok(0)
    done = h.kids()[0][0]
    h.finish("f", status="job-failed", error_class="worker")
    d = h.doc()
    assert d["status"] == "failed" and d["steps"]["s"]["state"] == "cancelled"
    live = {h.kids()[1][0], h.kids()[2][0]}
    assert set(h.revoked[-1][1]) == live and done not in h.revoked[-1][1]
    h.send(h.child_ev(1, outputs={"o": "x"}))  # a late event of a cancelled step
    assert h.doc() == d


def test_user_cancel_revokes_live_children():
    h = run_scatter(list("ab"))
    h.ok(0)
    h.send({"kind": "cancel_requested", "run_id": "r1", "username": "u"})
    assert h.doc()["status"] == "cancelled"
    assert set(h.revoked[-1][1]) == {h.kids()[1][0]}


def test_fanout_resumes_after_a_submit_failure_without_duplicates():
    h = SH(spec_for(), inputs={"items": list("abcd")})
    real = h.eng.submit_fn
    calls = {"n": 0}

    def flaky(job, pid):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("broker down")
        return real(job, pid)
    h.eng.submit_fn = flaky
    with pytest.raises(RuntimeError):
        h.start()
    assert sorted(h.kids()) == [0, 1] and h.doc()["steps"]["s"]["state"] == "running"
    h.eng._release("r1", "s")  # the redelivered/replayed release
    assert sorted(h.kids()) == [0, 1, 2, 3] and len(h.jobs) == 4
    h.eng._release("r1", "s")
    assert len(h.jobs) == 4


def test_keys_get_ttl_when_the_run_ends_and_are_listed():
    h = run_scatter(["a"])
    for k in ("sci", "sc", "sco", "scm"):
        assert "wf:%s:r1:s" % k in ws.run_keys("r1", h.spec)
    h.ok(0)
    assert h.doc()["status"] == "completed"
    for k in ws.run_keys("r1", h.spec):
        if h.r.exists(k):
            assert 0 < h.r.ttl(k) <= 24 * 3600, k


def test_two_runs_scatter_independently():
    h = run_scatter(["a"])
    assert ws.scatter_counts(h.r, "other", "s") == {"n": 0, "completed": 0, "failed": 0, "retries": 0}


# ---------------------------------------------------------------------------------------- drain / halt (S6 x S5)


def drain_run(n=4, threshold=0.5, retries=None, mode="drain", started=(0, 1)):
    extra = dict(max_retries=retries, retry_on=["worker"]) if retries else {}
    steps = {"s": _step(ins={"v": _src("$.inputs.items")}, out=("o",), scatter={"input": "v", "threshold": threshold},
                        **extra),
             "f": _step(ins={"v": _src("$.inputs.x")})}
    spec = make_spec(steps, inputs={"items": {"type": "Any?"}, "x": {"type": "string", "default": "d"}},
                     on_error=mode)
    h = SH(spec, inputs={"items": list("abcdefgh")[:n]})
    h.start()
    for i in started:
        h.child(i, "job-started")
    return h


def fail_f(h):
    h.finish("f", status="job-failed", error_class="worker", short_error="boom")


def test_drain_revokes_queued_children_and_lets_running_ones_finish():
    h = drain_run()
    kids = h.kids()
    fail_f(h)
    d = h.doc()
    assert d["status"] == "halting" and d["steps"]["s"]["state"] == "running"
    assert set(h.revoked[-1][1]) == {kids[2][0], kids[3][0]}  # queued only; the running ones are left alone
    assert h.counts()["failed"] == 2 and h.counts()["completed"] == 0
    assert {i: c["state"] for i, c in ws.scatter_children(h.r, "r1", "s").items()} == {
        0: "running", 1: "running", 2: "failed", 3: "failed"}
    h.ok(0)
    assert h.doc()["status"] == "halting"
    h.ok(1)  # every child settled: 2 of 4 >= threshold 0.5
    d = h.doc()
    assert d["steps"]["s"]["state"] == "completed"
    assert ws.settled_map(h.r, "r1")["s"]["outputs"] == {"o": ["o0", "o1", None, None]}
    assert d["status"] == "failed" and d["failed_step"] == "f"
    assert validate_run_doc(d) == []
    res = ws.scatter_results(h.r, "r1", "s")
    assert res[2]["error_class"] == "cancelled" and res[3]["error_class"] == "cancelled"


def test_drain_scatter_below_threshold_fails_after_the_running_children_finish():
    h = drain_run(threshold=1.0)
    fail_f(h)
    h.ok(0)
    h.ok(1)
    d = h.doc()
    assert d["steps"]["s"]["state"] == "failed" and d["steps"]["s"]["error_class"] == "scatter_threshold"
    assert d["status"] == "failed"
    assert validate_run_doc(d) == []


def test_drain_does_not_retry_a_running_child_that_fails():
    h = drain_run(retries=2)
    n_jobs = len(h.jobs)
    fail_f(h)
    h.bad(0)
    assert len(h.jobs) == n_jobs  # no resubmission while halting
    assert ws.scatter_child(h.r, "r1", "s", 0)["state"] == "failed"
    h.ok(1)
    assert h.doc()["steps"]["s"]["error_class"] == "scatter_threshold"  # 1 of 4 completed < 0.5
    assert h.doc()["status"] == "failed"


def test_drain_with_no_child_running_settles_the_scatter_at_once():
    h = drain_run(threshold=0.5, started=())
    fail_f(h)
    d = h.doc()
    assert len(h.revoked[-1][1]) == 4
    assert d["steps"]["s"]["state"] == "failed" and d["steps"]["s"]["error_class"] == "scatter_threshold"
    assert d["status"] == "failed" and d["failed_step"] == "f"  # the first failure stays the run's
    assert ws.n_done(h.r, "r1") == 2


def test_drain_cancels_children_the_fanout_never_submitted():
    h = drain_run(started=())
    # a fan-out interrupted before the last two children were claimed
    for i in (2, 3):
        h.r.hdel(ws.k_sc("r1", "s"), str(i))
    fail_f(h)
    d = h.doc()
    assert d["steps"]["s"]["state"] == "failed" and h.counts()["failed"] == 4
    assert len(h.revoked[-1][1]) == 2  # only the two submitted ones need revoking


def test_halt_under_continue_drains_the_scatter_the_same_way():
    steps = {"s": _step(ins={"v": _src("$.inputs.items")}, out=("o",), scatter={"input": "v", "threshold": 0.5}),
             "g": _step(ins={"v": _src("$.inputs.x")}, out=("bad",)),
             "h": {"halt": {"status": "completed", "reason": "enough"}, "deps": ["g"], "when": "$.g.bad"}}
    spec = make_spec(steps, inputs={"items": {"type": "Any?"}, "x": {"type": "string", "default": "d"}},
                     outputs={"res": {"type": "string[]?", "source": ["$.s.o"]}}, on_error="continue")
    h = SH(spec, inputs={"items": list("abcd")})
    h.start()
    h.child(0, "job-started")
    kids = h.kids()
    h.finish("g", {"bad": True})
    d = h.doc()
    assert d["status"] == "halting" and d["steps"]["s"]["state"] == "running"
    assert set(h.revoked[-1][1]) == {kids[1][0], kids[2][0], kids[3][0]}
    h.ok(0)  # 1 of 4 < 0.5: the scatter fails, which outranks the completed halt
    assert h.doc()["status"] == "failed"
