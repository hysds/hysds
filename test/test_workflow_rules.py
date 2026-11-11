"""workflow_rules: pure decide() table tests and the kombu consumer shell (mocked I/O)."""
import json
import unittest.mock as umock

import pytest

from hysds_commons.workflow_examples import linear_spec, scatter_gather_spec
from hysds_commons.workflow_status import WorkflowStatusNotFound
from hysds.workflow_rules import (
    DeadLetter, Ignore, RetryAction, Submit, WorkflowRulesConsumer, decide,
)

RUN = "run-1"
SPEC_ID = "demo-linear:1.0.0"


def ev(step, status="job-completed", **kw):
    e = {"run_id": RUN, "step_id": step, "spec_id": SPEC_ID, "child_index": None, "child_total": None,
         "job_id": "j-" + step, "payload_id": "p-" + step, "status": status, "short_error": None,
         "workflow_outputs": None}
    e.update(kw)
    return e


def typed(**kv):
    return [{"name": k, "value_s": v} if isinstance(v, str) else {"name": k, "value_d": v} for k, v in kv.items()]


def status_doc(**states):
    nodes = {n: {"state": "pending"} for n in ("A", "pass_1", "B")}
    for n, s in states.items():
        nodes[n] = s if isinstance(s, dict) else {"state": s}
    return {"run_id": RUN, "spec_id": SPEC_ID, "status": "running", "inputs": {"greeting": "hi", "n": 3},
            "nodes": nodes}


def test_entry_completed_submits_pass():
    st = status_doc(A="completed")
    [a] = decide(ev("A", workflow_outputs=typed(count=3)), linear_spec(), st)
    assert isinstance(a, Submit)
    assert a.job_type == "job-workflow-pass:v1.0.0"
    assert a.queue == "workflow-connector-pass"
    assert a.workflow == {"run_id": RUN, "step_id": "pass_1", "spec_id": SPEC_ID,
                          "child_index": None, "child_total": None}
    assert a.params["run_id"] == RUN and a.params["step_id"] == "pass_1" and a.params["spec_id"] == SPEC_ID
    assert json.loads(a.params["workflow_event"])["step_id"] == "A"


def test_pass_completed_submits_B_with_resolved_inputs():
    st = status_doc(A={"state": "completed", "outputs": {"items": ["a", "b"]}}, pass_1="completed")
    e = ev("pass_1", workflow_outputs=[{"name": "outputs", "value_s": "x"}])  # real events carry the typed list
    [a] = decide(e, linear_spec(), st)
    assert isinstance(a, Submit)
    assert a.job_type == "job-wf_test:v1.0"
    assert a.queue == "hello_world_worker"  # defaults.queue
    assert a.params == {"outputs": "x"}  # event outputs overlay status outputs
    assert a.workflow["step_id"] == "B" and a.workflow["spec_id"] == SPEC_ID


def test_pass_uses_event_outputs_when_status_lacks_them():
    st = status_doc(A="completed")
    [a] = decide(ev("pass_1", workflow_outputs=typed(outputs="zz")), linear_spec(), st)
    assert a.params == {"outputs": "zz"}


def test_queue_precedence():
    spec = linear_spec()
    st = status_doc(pass_1="completed")
    e = ev("pass_1", workflow_outputs={"outputs": 1})
    spec["nodes"]["B"]["queue"] = "node_q"
    assert decide(e, spec, st)[0].queue == "node_q"
    del spec["nodes"]["B"]["queue"]
    del spec["defaults"]["queue"]
    assert decide(e, spec, st, lambda jt: "rec_q")[0].queue == "rec_q"
    [a] = decide(e, spec, st)  # nothing anywhere -> halt, not crash
    assert a.job_type == "job-workflow-halt:v1.0.0"
    assert "no queue" in json.loads(a.params["workflow_event"])["short_error"]


def test_last_job_completed_submits_finalize():
    st = status_doc(A="completed", pass_1="completed", B="running")
    [a] = decide(ev("B", workflow_outputs=typed(result="r")), linear_spec(), st)
    assert a.job_type == "job-workflow-finalize:v1.0.0"
    assert a.queue == "workflow-connector-finalize"
    assert a.workflow["step_id"] == "_finalize"
    assert a.params["step_id"] == "_finalize"


@pytest.mark.parametrize("s", ["job-failed", "job-offline"])
def test_failure_halts(s):
    st = status_doc(A="running")
    [a] = decide(ev("A", s, short_error="boom"), linear_spec(), st)
    assert a.job_type == "job-workflow-halt:v1.0.0"
    assert a.queue == "workflow-connector-halt"
    assert a.workflow["step_id"] == "_halt"
    we = json.loads(a.params["workflow_event"])
    assert we["step_id"] == "A" and we["status"] == s


def test_duplicate_event_successor_not_pending():
    st = status_doc(A="completed", pass_1="running")
    [a] = decide(ev("A"), linear_spec(), st)
    assert isinstance(a, Ignore) and "not pending" in a.reason
    st = status_doc(pass_1="completed", B="running")
    [a] = decide(ev("pass_1", workflow_outputs={"outputs": 1}), linear_spec(), st)
    assert isinstance(a, Ignore)


@pytest.mark.parametrize("rs", ["completed", "failed", "cancelled"])
def test_terminal_run_ignored(rs):
    st = status_doc()
    st["status"] = rs
    for e in (ev("A"), ev("A", "job-failed"), ev("B"), ev("_halt", "job-failed")):
        [a] = decide(e, linear_spec(), st)
        assert isinstance(a, Ignore)


def test_internal_events():
    st = status_doc()
    for step in ("_finalize", "_halt"):
        [a] = decide(ev(step), linear_spec(), st)
        assert isinstance(a, Ignore) and not a.loud
        [a] = decide(ev(step, "job-failed"), linear_spec(), st)
        assert isinstance(a, Ignore) and a.loud


def test_ref_resolution_failure_halts():
    st = status_doc(pass_1="completed")
    [a] = decide(ev("pass_1"), linear_spec(), st)  # no outputs anywhere
    assert a.job_type == "job-workflow-halt:v1.0.0"
    we = json.loads(a.params["workflow_event"])
    assert we["step_id"] == "B" and we["status"] == "job-failed"
    assert "pass_1" in we["short_error"] and "ref" in we["short_error"]


def test_job_with_scatter_successor_submits_scatter_connector():
    sg = scatter_gather_spec()
    st = {"run_id": RUN, "spec_id": "demo-scatter:1.0.0", "status": "running", "inputs": {},
          "nodes": {n: {"state": "pending"} for n in sg["nodes"]}}
    [a] = decide(ev("A"), sg, st)
    assert a.job_type == "job-workflow-scatter:v1.0.0" and a.workflow["step_id"] == "scatter_1"


def test_unknown_step_and_missing_run_and_spec():
    [a] = decide(ev("nope"), linear_spec(), status_doc())
    assert isinstance(a, DeadLetter) and "nope" in a.reason
    [a] = decide(ev("A"), None, None)
    assert isinstance(a, DeadLetter) and "run not found" in a.reason
    [a] = decide(ev("A"), None, status_doc())
    assert isinstance(a, DeadLetter) and "spec" in a.reason


# ------------------------------------------------------------------ consumer shell
def make_consumer(status=None, spec=None, submit=None, get_status_fn=None, **kw):
    es = umock.MagicMock()
    es.get.return_value = {"found": True, "_source": spec if spec is not None else linear_spec()}
    if get_status_fn is None:
        def get_status_fn(client, index, run_id):
            if status is None:
                raise WorkflowStatusNotFound(run_id)
            return status, 1, 1
    import fakeredis
    kw.setdefault("job_doc_fn", lambda event: None)
    kw.setdefault("job_exists_fn", lambda run_id, step_id, child_index: False)
    c = WorkflowRulesConsumer(umock.MagicMock(), "workflow_queue", es=es, get_status_fn=get_status_fn,
                              submit_fn=submit or umock.MagicMock(), redis_client=fakeredis.FakeStrictRedis(),
                              **kw)
    return c, es


def msg(redelivered=False):
    m = umock.MagicMock()
    m.delivery_info = {"redelivered": redelivered}
    return m


def test_shell_acks_after_submit():
    order = []
    submit = umock.MagicMock(side_effect=lambda *a, **k: order.append("submit"))
    c, es = make_consumer(status_doc(A="completed"), submit=submit)
    m = msg()
    m.ack.side_effect = lambda: order.append("ack")
    c.on_message(ev("A"), m)
    assert order == ["submit", "ack"]
    jt, q, params, wf = submit.call_args[0]
    assert jt == "job-workflow-pass:v1.0.0" and wf["step_id"] == "pass_1"
    c.on_message(ev("A"), msg())
    assert es.get.call_count == 1  # spec cached


def test_shell_missing_run_acks_without_submit():
    submit = umock.MagicMock()
    c, _ = make_consumer(status=None, submit=submit)
    m = msg()
    c.on_message(ev("A", run_id="run-s17"), m)
    m.ack.assert_called_once()
    submit.assert_not_called()


def test_shell_exception_requeue_once_then_poison():
    submit = umock.MagicMock(side_effect=RuntimeError("es down"))
    c, _ = make_consumer(status_doc(A="completed"), submit=submit)
    with umock.patch("hysds.workflow_rules.time.sleep"):
        m1 = msg(redelivered=False)
        c.on_message(ev("A"), m1)
        m1.requeue.assert_called_once()
        m1.ack.assert_not_called()
        m2 = msg(redelivered=True)
        c.on_message(ev("A"), m2)
        m2.ack.assert_called_once()
        m2.requeue.assert_not_called()


def test_shell_bad_body_is_handled():
    c, _ = make_consumer(status_doc())
    with umock.patch("hysds.workflow_rules.time.sleep"):
        m = msg(redelivered=True)
        c.on_message("garbage", m)
        m.ack.assert_called_once()


def test_pass_completed_submits_when_successor_queued_by_pass_connector():
    """Regression (slice 2.7 E2E): the pass connector marks its successor
    'queued' before its own completion event; that must still be submitted."""
    spec = linear_spec()
    status = {"run_id": "run-1", "spec_id": "demo-linear:1.0.0", "status": "running",
              "inputs": {"greeting": "hi"},
              "nodes": {"A": {"state": "completed", "outputs": {"count": 3, "items": ["a"]}},
                        "pass_1": {"state": "completed", "outputs": {"outputs": "x"}},
                        "B": {"state": "queued"}}}
    e = ev("pass_1", workflow_outputs=[{"name": "outputs", "value_s": "x"}])
    actions = decide(e, spec, status, recommended_queue=lambda jt: "q")
    assert len(actions) == 1 and isinstance(actions[0], Submit), actions
    status["nodes"]["B"]["state"] = "completed"
    actions = decide(e, spec, status, recommended_queue=lambda jt: "q")
    assert isinstance(actions[0], Ignore)


def test_duplicate_event_submits_once_via_redis_claim():
    """Regression (slice 2.7): doubled job-failed event submitted _halt twice."""
    st = {"run_id": "run-1", "spec_id": "demo-linear:1.0.0", "status": "running", "inputs": {"greeting": "hi"},
          "nodes": {"A": {"state": "running"}, "pass_1": {"state": "pending"}, "B": {"state": "pending"}}}
    submit = umock.MagicMock()
    c, _ = make_consumer(status=st, submit=submit)
    e = ev("A", status="job-failed", short_error="boom")
    c.process_event(e)
    actions = c.process_event(dict(e, timestamp="later"))
    assert submit.call_count == 1
    assert "already claimed" in actions[0].summary()


def test_claim_released_when_submit_fails():
    st = {"run_id": "run-1", "spec_id": "demo-linear:1.0.0", "status": "running", "inputs": {"greeting": "hi"},
          "nodes": {"A": {"state": "running"}, "pass_1": {"state": "pending"}, "B": {"state": "pending"}}}
    submit = umock.MagicMock(side_effect=[RuntimeError("broker down"), "pid"])
    c, _ = make_consumer(status=st, submit=submit)
    e = ev("A", status="job-failed", short_error="boom")
    with pytest.raises(RuntimeError):
        c.process_event(e)
    c.process_event(e)
    assert submit.call_count == 2


# ------------------------------------------------------------------ phase 3: scatter/gather
from hysds import workflow_barrier as wb  # noqa: E402
from hysds.workflow_rules import BarrierRecord  # noqa: E402

SG_ID = "demo-scatter:1.0.0"


def sg_status(**states):
    sg = scatter_gather_spec()
    nodes = {n: {"state": "pending"} for n in sg["nodes"]}
    for n, s in states.items():
        nodes[n] = s if isinstance(s, dict) else {"state": s}
    return {"run_id": RUN, "spec_id": SG_ID, "status": "running", "inputs": {"x": "x0"}, "nodes": nodes}


def child(i, status="job-completed", n=5, **kw):
    return ev("P", status, child_index=i, child_total=n, job_id=f"jc-{i}", spec_id=SG_ID, **kw)


def barrier_result(c, f, n, winner=True):
    return wb.BarrierResult(c, f, n, winner)


def test_child_first_pass_returns_barrier_record():
    st = sg_status(P={"state": "running", "expected": 5, "threshold": 0.6})
    [a] = decide(child(2), scatter_gather_spec(), st)
    assert a == BarrierRecord(RUN, "P", "jc-2", "job-completed", 5)


@pytest.mark.parametrize("bad", [dict(child_total=None), dict(job_id=None), dict(status="job-started")])
def test_unrecordable_child_loud_ignore(bad):
    e = child(1)
    e.update(bad)
    [a] = decide(e, scatter_gather_spec(), sg_status(P="running"))
    assert isinstance(a, Ignore) and a.loud


def test_non_winner_ignored_with_counts():
    st = sg_status(P="running")
    [a] = decide(child(0), scatter_gather_spec(), st, barrier=barrier_result(1, 0, 5, False))
    assert isinstance(a, Ignore) and "1 completed, 0 failed of 5" in a.reason and not a.loud


def test_winner_threshold_pass_submits_gather_with_synthetic_event():
    st = sg_status(P={"state": "running", "expected": 5})
    [a] = decide(child(4), scatter_gather_spec(), st, barrier=barrier_result(3, 2, 5))  # 0.6 >= 0.6
    assert a.job_type == "job-workflow-gather:v1.0.0" and a.queue == "workflow-connector-gather"
    assert a.workflow == {"run_id": RUN, "step_id": "gather_1", "spec_id": SG_ID,
                          "child_index": None, "child_total": None}
    we = json.loads(a.params["workflow_event"])
    assert we == {"run_id": RUN, "step_id": "P", "spec_id": SG_ID, "status": "barrier-released",
                  "barrier": {"expected": 5, "completed": 3, "failed": 2}}


def test_winner_threshold_fail_halts():
    st = sg_status(P="running")
    [a] = decide(child(4), scatter_gather_spec(), st, barrier=barrier_result(2, 3, 5))
    assert a.job_type == "job-workflow-halt:v1.0.0" and a.workflow["step_id"] == "_halt"
    we = json.loads(a.params["workflow_event"])
    assert we["step_id"] == "P" and we["status"] == "job-failed" and we["child_index"] is None
    assert we["short_error"] == "scatter P: 2/5 completed < threshold 0.6"


def test_winner_gather_not_pending_ignored():
    st = sg_status(P="running", gather_1="completed")
    [a] = decide(child(4), scatter_gather_spec(), st, barrier=barrier_result(5, 0, 5))
    assert isinstance(a, Ignore) and "not pending" in a.reason


@pytest.mark.parametrize("rs", ["completed", "failed", "cancelled"])
def test_child_event_on_terminal_run_recorded_then_no_action(rs):
    st = sg_status(P="running")
    st["status"] = rs
    [a] = decide(child(0), scatter_gather_spec(), st)
    assert isinstance(a, BarrierRecord)  # still recorded
    for win in (True, False):
        [a] = decide(child(0), scatter_gather_spec(), st, barrier=barrier_result(5, 0, 5, win))
        assert isinstance(a, Ignore) and "already" in a.reason


def test_scatter_completion_n0_submits_gather_else_ignores():
    sg = scatter_gather_spec()
    st = sg_status(scatter_1="completed", P={"state": "running", "expected": 0})
    e = ev("scatter_1", spec_id=SG_ID, workflow_outputs=typed(n=0))
    [a] = decide(e, sg, st)
    assert a.job_type == "job-workflow-gather:v1.0.0" and a.workflow["step_id"] == "gather_1"
    assert json.loads(a.params["workflow_event"])["step_id"] == "scatter_1"
    st = sg_status(scatter_1="completed", P={"state": "running", "expected": 5})
    [a] = decide(e, sg, st)
    assert isinstance(a, Ignore) and not a.loud
    st = sg_status(scatter_1="completed", P={"state": "running", "expected": 0}, gather_1="running")
    [a] = decide(e, sg, st)
    assert isinstance(a, Ignore) and "not pending" in a.reason


def test_gather_completion_submits_pass():
    st = sg_status(gather_1="completed")
    [a] = decide(ev("gather_1", spec_id=SG_ID), scatter_gather_spec(), st)
    assert a.job_type == "job-workflow-pass:v1.0.0" and a.workflow["step_id"] == "pass_2"
    st = sg_status(gather_1="completed", pass_2="running")
    [a] = decide(ev("gather_1", spec_id=SG_ID), scatter_gather_spec(), st)
    assert isinstance(a, Ignore)


def test_gather_completion_last_node_finalizes():
    sg = scatter_gather_spec()
    sg["nodes"]["gather_1"]["next"] = []
    del sg["nodes"]["pass_2"], sg["nodes"]["B"]
    sg["outputs"] = {"final": "$.gather_1.result"}
    st = {"run_id": RUN, "spec_id": SG_ID, "status": "running", "inputs": {}, "nodes": {}}
    [a] = decide(ev("gather_1", spec_id=SG_ID), sg, st)
    assert a.job_type == "job-workflow-finalize:v1.0.0" and a.workflow["step_id"] == "_finalize"


@pytest.mark.parametrize("step", ["scatter_1", "gather_1"])
def test_connector_failure_halts(step):
    st = sg_status()
    [a] = decide(ev(step, "job-failed", spec_id=SG_ID, short_error="x"), scatter_gather_spec(), st)
    assert a.job_type == "job-workflow-halt:v1.0.0"
    assert json.loads(a.params["workflow_event"])["step_id"] == step


# -- consumer shell over fakeredis
def make_sg_consumer(status, submit=None):
    c, es = make_consumer(status, spec=scatter_gather_spec(), submit=submit)
    es.search.return_value = {"hits": {"hits": []}}
    return c, es


def run_children(c, statuses, n=None):
    n = n or len(statuses)
    out = []
    for i, s in enumerate(statuses):
        out.append(c.process_event(child(i, s, n=n)))
    return out


def test_shell_threshold_pass_gather_submitted_once():
    submit = umock.MagicMock()
    c, es = make_sg_consumer(sg_status(P={"state": "running", "expected": 5}), submit)
    # 3 ok + 2 failed, with duplicates; redeliveries of the winner collapse via the barrier
    res = run_children(c, ["job-completed", "job-failed", "job-completed", "job-failed", "job-completed"])
    assert all(isinstance(a[0], Ignore) for a in res[:4])
    assert isinstance(res[4][0], Submit)
    assert submit.call_count == 1
    jt, q, params, wf = submit.call_args[0]
    assert jt == "job-workflow-gather:v1.0.0" and wf["step_id"] == "gather_1"
    c.process_event(child(4, "job-completed"))  # redelivered winner event
    assert submit.call_count == 1
    assert es.search.call_count == 1  # only the first child (keys missing, node running) rebuilds


def test_shell_threshold_fail_halts():
    submit = umock.MagicMock()
    c, _ = make_sg_consumer(sg_status(P={"state": "running", "expected": 5}), submit)
    run_children(c, ["job-completed", "job-failed", "job-failed", "job-failed", "job-completed"])
    assert submit.call_count == 1
    assert submit.call_args[0][0] == "job-workflow-halt:v1.0.0"


def test_shell_duplicate_winner_protected_by_claim():
    submit = umock.MagicMock()
    c, _ = make_sg_consumer(sg_status(P={"state": "running", "expected": 1}), submit)
    c.process_event(child(0, n=1))
    # simulate barrier state loss after release -> second "winner" must hit the claim
    c.redis.delete(*wb.barrier_keys(RUN, "P"))
    [a] = c.process_event(child(0, n=1))
    assert "already claimed" in a.summary()
    assert submit.call_count == 1


def test_shell_terminal_run_records_but_no_submit():
    submit = umock.MagicMock()
    st = sg_status(P="running")
    st["status"] = "failed"
    c, _ = make_sg_consumer(st, submit)
    [a] = c.process_event(child(0, n=5))
    assert isinstance(a, Ignore)
    assert wb.get_barrier(c.redis, RUN, "P")["completed"] == 1
    submit.assert_not_called()


def test_shell_rebuild_when_keys_missing_and_running():
    submit = umock.MagicMock()
    c, es = make_sg_consumer(sg_status(P={"state": "running", "expected": 3}), submit)
    es.search.return_value = {"hits": {"hits": [
        {"_id": "1", "_source": {"job_id": "jc-0", "status": "job-completed", "@timestamp": "t1"}, "sort": ["t1", "jc-0"]},
        {"_id": "2", "_source": {"job_id": "jc-1", "status": "job-completed", "@timestamp": "t2"}, "sort": ["t2", "jc-1"]},
    ]}}
    # event for child 2 arrives after Redis was lost: seed (2 done) + this event = 3 -> winner
    [a] = c.process_event(child(2, n=3))
    assert isinstance(a, Submit) and a.job_type == "job-workflow-gather:v1.0.0"
    assert es.search.call_count == 1
    # the redelivered event: keys exist now, no rebuild, no second submit
    c.process_event(child(2, n=3))
    assert es.search.call_count == 1 and submit.call_count == 1


def test_shell_no_rebuild_when_node_not_running():
    c, es = make_sg_consumer(sg_status(P="pending"))
    c.process_event(child(0, n=3))
    es.search.assert_not_called()


def test_shell_n0_via_scatter_completion():
    submit = umock.MagicMock()
    c, _ = make_sg_consumer(sg_status(scatter_1="completed", P={"state": "running", "expected": 0}), submit)
    c.process_event(ev("scatter_1", spec_id=SG_ID))
    c.process_event(ev("scatter_1", spec_id=SG_ID))
    assert submit.call_count == 1 and submit.call_args[0][3]["step_id"] == "gather_1"


def test_shell_log_line_includes_barrier_counts(caplog):
    import logging
    c, _ = make_sg_consumer(sg_status(P={"state": "running", "expected": 5}))
    with caplog.at_level(logging.INFO, logger="hysds.workflow_rules"):
        c.process_event(child(0))
    assert any("barrier=barrier completed=1 failed=0 expected=5 winner=False" in r.getMessage()
               for r in caplog.records)


# ---- pluggable connectors (contract 11.4)

def _with_connectors(spec, **per_node):
    for nid, (conn, q) in per_node.items():
        spec["nodes"][nid].update(connector=conn, connector_queue=q)
    return spec


def test_pluggable_connector_used_when_present_job_to_pass():
    sp = _with_connectors(linear_spec(), pass_1=("job-m-pass:v2", "m-pass-q"))
    [a] = decide(ev("A", workflow_outputs=typed(count=3)), sp, status_doc(A="completed"))
    assert (a.job_type, a.queue) == ("job-m-pass:v2", "m-pass-q")
    assert a.workflow["step_id"] == "pass_1"


def test_pluggable_connector_job_to_scatter_and_scatter_n0_gather_and_gather_to_pass():
    sp = _with_connectors(scatter_gather_spec(), scatter_1=("job-m-sc:v1", "m-sc-q"),
                          gather_1=("job-m-ga:v1", "m-ga-q"), pass_2=("job-m-pass:v2", "m-pass-q"))
    [a] = decide(ev("A", spec_id=SG_ID), sp, sg_status(A="completed"))
    assert (a.job_type, a.queue) == ("job-m-sc:v1", "m-sc-q")
    e = ev("scatter_1", spec_id=SG_ID, workflow_outputs=typed(n=0))
    [a] = decide(e, sp, sg_status(scatter_1="completed", P={"state": "running", "expected": 0}))
    assert (a.job_type, a.queue) == ("job-m-ga:v1", "m-ga-q")
    [a] = decide(ev("gather_1", spec_id=SG_ID), sp, sg_status(gather_1="completed"))
    assert (a.job_type, a.queue) == ("job-m-pass:v2", "m-pass-q")


def test_pluggable_connector_barrier_winner_gather():
    sp = _with_connectors(scatter_gather_spec(), gather_1=("job-m-ga:v1", "m-ga-q"))
    st = sg_status(P={"state": "running", "expected": 5})
    [a] = decide(child(4), sp, st, barrier=barrier_result(5, 0, 5))
    assert (a.job_type, a.queue) == ("job-m-ga:v1", "m-ga-q")


def test_pluggable_connector_pass_after_pass_chain_and_partial_fallback():
    # only the gather carries a connector; scatter on the same spec falls back
    sp = _with_connectors(scatter_gather_spec(), gather_1=("job-m-ga:v1", "m-ga-q"))
    [a] = decide(ev("A", spec_id=SG_ID), sp, sg_status(A="completed"))
    assert (a.job_type, a.queue) == ("job-workflow-scatter:v1.0.0", "workflow-connector-scatter")


def test_finalize_and_halt_ignore_node_connector_fields():
    sp = _with_connectors(linear_spec(), pass_1=("job-m-pass:v2", "m-pass-q"))
    [a] = decide(ev("B", workflow_outputs=typed(result="r")), sp, status_doc(A="completed", pass_1="completed", B="completed"))
    assert (a.job_type, a.queue) == ("job-workflow-finalize:v1.0.0", "workflow-connector-finalize")
    [a] = decide(ev("A", status="job-failed", short_error="x"), sp, status_doc(A="running"))
    assert (a.job_type, a.queue) == ("job-workflow-halt:v1.0.0", "workflow-connector-halt")


# ------------------------------------------------------------------ phase 4: retry (contract 12.1-12.3)
def retry_spec(max_retries=2, retry_on=("exit_code:*",), where="B"):
    spec = linear_spec()
    tgt = spec["defaults"] if where == "defaults" else spec["nodes"][where]
    tgt["max_retries"] = max_retries
    tgt["retry_on"] = list(retry_on)
    return spec


def fail(step="B", ec="exit_code:1", rc=0, **kw):
    kw.setdefault("job_id", "jid-" + step)
    return ev(step, "job-failed", error_class=ec, retry_count=rc, **kw)


def test_retryable_failure_yields_retry_not_halt():
    st = status_doc(A="completed", pass_1="completed", B="running")
    [a] = decide(fail(), retry_spec(), st)
    assert isinstance(a, RetryAction)
    assert (a.job_id, a.max_retries, a.retry_count, a.queue) == ("jid-B", 2, 0, "system-jobs-queue")
    assert a.claim_key == "wf-claim:run-1:B:None:retry1"
    assert a.params == {"retry_job_id": "jid-B", "retry_count_max": 2, "job_priority_increment": 0,
                        "workflow_retry": True}


def test_retry_job_offline_matches_offline_and_star():
    st = status_doc(B="running")
    e = ev("B", "job-offline", error_class="offline", retry_count=0, job_id="j")
    assert isinstance(decide(e, retry_spec(retry_on=["offline"]), st)[0], RetryAction)
    assert isinstance(decide(e, retry_spec(retry_on=["*"]), st)[0], RetryAction)
    halt = decide(e, retry_spec(retry_on=["exit_code:*"]), st)[0]
    assert isinstance(halt, Submit) and halt.workflow["step_id"] == "_halt"


def test_retry_uses_defaults_and_node_overrides_defaults():
    st = status_doc(B="running")
    spec = retry_spec(where="defaults", max_retries=3, retry_on=["*"])
    assert isinstance(decide(fail(), spec, st)[0], RetryAction)
    spec["nodes"]["B"]["max_retries"] = 0  # node wins -> no retry -> halt
    a = decide(fail(), spec, st)[0]
    assert isinstance(a, Submit) and a.workflow["step_id"] == "_halt"


def test_retry_second_attempt_counts_up():
    st = status_doc(B="running")
    [a] = decide(fail(rc=1), retry_spec(max_retries=2), st)
    assert isinstance(a, RetryAction) and a.claim_key.endswith(":retry2")


def test_exhausted_retries_halt_via_existing_path():
    st = status_doc(B="running")
    [a] = decide(fail(rc=2), retry_spec(max_retries=2), st)
    assert isinstance(a, Submit) and a.workflow["step_id"] == "_halt"
    we = json.loads(a.params["workflow_event"])
    assert we["error_class"] == "exit_code:1"


def test_no_policy_means_halt():
    st = status_doc(B="running")
    a = decide(fail(), linear_spec(), st)[0]
    assert isinstance(a, Submit) and a.workflow["step_id"] == "_halt"


def test_class_not_in_retry_on_halts():
    st = status_doc(B="running")
    a = decide(fail(ec="timeout"), retry_spec(retry_on=["exit_code:*"]), st)[0]
    assert isinstance(a, Submit)
    assert isinstance(decide(fail(ec="pge:transient"), retry_spec(retry_on=["pge:transient"]), st)[0], RetryAction)
    assert isinstance(decide(fail(ec="exit_code:7"), retry_spec(retry_on=["exit_code:7"]), st)[0], RetryAction)
    assert isinstance(decide(fail(ec="exit_code:8"), retry_spec(retry_on=["exit_code:7"]), st)[0], Submit)


def test_event_without_retry_fields_treated_as_count_zero_class_missing():
    st = status_doc(B="running")
    e = ev("B", "job-failed", job_id="j")  # old producer: no error_class / retry_count
    assert isinstance(decide(e, retry_spec(retry_on=["*"]), st)[0], RetryAction)
    assert isinstance(decide(e, retry_spec(retry_on=["exit_code:*"]), st)[0], Submit)


def test_synthetic_failure_and_missing_job_id_never_retry():
    st = status_doc(B="running")
    spec = retry_spec(retry_on=["*"])
    assert isinstance(decide(fail(synthetic=True), spec, st)[0], Submit)
    assert isinstance(decide(fail(job_id=None), spec, st)[0], Submit)


def test_connector_failure_not_retried():
    st = status_doc(A="completed", pass_1="running")
    spec = retry_spec(retry_on=["*"], where="defaults")
    e = ev("pass_1", "job-failed", error_class="worker", retry_count=0)
    assert isinstance(decide(e, spec, st)[0], Submit)


def test_retry_on_terminal_run_ignored():
    st = status_doc(B="running")
    st["status"] = "failed"
    assert isinstance(decide(fail(), retry_spec(), st)[0], Ignore)


def test_internal_failure_not_retried():
    st = status_doc(B="running")
    e = ev("_halt", "job-failed", error_class="worker", retry_count=0)
    a = decide(e, retry_spec(retry_on=["*"], where="defaults"), st)[0]
    assert isinstance(a, Ignore) and a.loud


def sg_retry_spec(max_retries=1, retry_on=("exit_code:*",)):
    spec = scatter_gather_spec()
    spec["nodes"]["P"]["max_retries"] = max_retries
    spec["nodes"]["P"]["retry_on"] = list(retry_on)
    return spec


def sg_fail(i, rc=0, ec="exit_code:1", n=5):
    return child(i, "job-failed", error_class=ec, retry_count=rc)


def test_scatter_child_retryable_failure_not_recorded_in_barrier():
    st = sg_status(P={"state": "running", "expected": 5})
    [a] = decide(sg_fail(3), sg_retry_spec(), st)
    assert isinstance(a, RetryAction)
    assert a.child_index == 3 and a.claim_key == "wf-claim:run-1:P:3:retry1" and a.job_id == "jc-3"


def test_scatter_child_exhausted_failure_goes_to_barrier():
    st = sg_status(P={"state": "running", "expected": 5})
    [a] = decide(sg_fail(3, rc=1), sg_retry_spec(), st)
    assert isinstance(a, BarrierRecord) and a.status == "job-failed"


def test_scatter_child_no_policy_goes_to_barrier():
    st = sg_status(P={"state": "running", "expected": 5})
    assert isinstance(decide(sg_fail(3), scatter_gather_spec(), st)[0], BarrierRecord)


def test_barrier_second_pass_never_retries():
    st = sg_status(P={"state": "running", "expected": 5})
    a = decide(sg_fail(4), sg_retry_spec(), st, barrier=barrier_result(3, 2, 5, winner=False))
    assert isinstance(a[0], Ignore)


def test_decide_retry_is_pure_no_io():
    st = status_doc(B="running")
    spec = retry_spec()
    e = fail()
    before = (json.dumps(st, sort_keys=True), json.dumps(spec, sort_keys=True), json.dumps(e, sort_keys=True))
    decide(e, spec, st)
    assert before == (json.dumps(st, sort_keys=True), json.dumps(spec, sort_keys=True),
                      json.dumps(e, sort_keys=True))


# shell
RT = "job-lw-mozart-retry:v6.1.2"


def test_shell_retry_submits_retry_job_untagged_no_dedup_and_no_halt():
    submit = umock.MagicMock()
    c, _ = make_consumer(status_doc(A="completed", pass_1="completed", B="running"),
                         spec=retry_spec(), submit=submit, retry_job_type=RT)
    [a] = c.process_event(fail())
    assert isinstance(a, RetryAction)
    submit.assert_called_once_with(RT, "system-jobs-queue",
                                   {"retry_job_id": "jid-B", "retry_count_max": 2,
                                    "job_priority_increment": 0, "workflow_retry": True},
                                   None, enable_dedup=False)
    assert c.redis.get("wf-claim:run-1:B:None:retry1") is not None


def test_shell_redelivered_failure_retries_once():
    submit = umock.MagicMock()
    c, _ = make_consumer(status_doc(B="running"), spec=retry_spec(), submit=submit, retry_job_type=RT)
    c.process_event(fail())
    actions = c.process_event(fail(timestamp="later"))
    assert submit.call_count == 1
    assert "already claimed" in actions[0].summary()
    # the next attempt's failure is a different claim
    c.process_event(fail(rc=1))
    assert submit.call_count == 2


def test_shell_retry_claim_released_on_submit_failure():
    submit = umock.MagicMock(side_effect=[RuntimeError("mozart down"), "pid"])
    c, _ = make_consumer(status_doc(B="running"), spec=retry_spec(), submit=submit, retry_job_type=RT)
    with pytest.raises(RuntimeError):
        c.process_event(fail())
    assert c.redis.get("wf-claim:run-1:B:None:retry1") is None
    c.process_event(fail())
    assert submit.call_count == 2


def test_shell_scatter_child_retry_does_not_touch_barrier():
    submit = umock.MagicMock()
    c, es = make_sg_consumer(sg_status(P={"state": "running", "expected": 5}), submit)
    c._es.get.return_value = {"found": True, "_source": sg_retry_spec()}
    c._retry_job_type = RT
    [a] = c.process_event(sg_fail(2))
    assert isinstance(a, RetryAction)
    assert not wb.barrier_exists(c.redis, RUN, "P")
    assert es.search.call_count == 0  # no rebuild either


def test_shell_scatter_retry_then_success_counts_once():
    submit = umock.MagicMock()
    c, _ = make_sg_consumer(sg_status(P={"state": "running", "expected": 2}), submit)
    c._es.get.return_value = {"found": True, "_source": sg_retry_spec()}
    c._retry_job_type = RT
    c.process_event(sg_fail(0, rc=0))
    c.process_event(child(0, n=2, retry_count=1))
    [a] = c.process_event(child(1, n=2))
    assert isinstance(a, Submit) and a.workflow["step_id"] == "gather_1"
    assert wb.get_barrier(c.redis, RUN, "P")["failed"] == 0


def test_shell_retry_job_type_lookup_and_fallback():
    c, es = make_consumer(status_doc(), submit=umock.MagicMock())
    es.search.return_value = {"hits": {"hits": [{"_source": {"id": "job-lw-mozart-retry:v6.1.2"}},
                                                 {"_source": {"id": "job-lw-mozart-retry-other:v1"}}]}}
    assert c.retry_job_type() == "job-lw-mozart-retry:v6.1.2"
    es.search.reset_mock()
    assert c.retry_job_type() == "job-lw-mozart-retry:v6.1.2" and es.search.call_count == 0  # cached
    c2, es2 = make_consumer(status_doc(), submit=umock.MagicMock())
    es2.search.side_effect = RuntimeError("es down")
    assert c2.retry_job_type() == "job-lw-mozart-retry"  # not cached on failure
    es2.search.side_effect = None
    es2.search.return_value = {"hits": {"hits": []}}
    assert c2.retry_job_type() == "job-lw-mozart-retry"


# ------------------------------------------------------------------ phase 4.5: DLQ + barrier TTL
def make_dlq_consumer(status=None, spec=None, submit=None, **kw):
    dlq = []
    c, es = make_consumer(status, spec=spec, submit=submit, dlq_publish_fn=dlq.append, **kw)
    return c, dlq


def test_dlq_malformed_bodies_acked_and_published_immediately():
    c, dlq = make_dlq_consumer(status_doc())
    for body in ("garbage", [1, 2], {"step_id": "A", "status": "job-completed"},
                 {"run_id": "r", "step_id": "A"}, {"run_id": 5, "step_id": "A", "status": "x"}):
        m = msg(redelivered=False)
        c.on_message(body, m)
        m.ack.assert_called_once()
        m.requeue.assert_not_called()
    assert len(dlq) == 5
    assert all(set(d) == {"event", "reason", "consumer_host", "timestamp"} for d in dlq)
    assert dlq[0]["event"] == "garbage" and dlq[0]["reason"].startswith("malformed event")
    assert dlq[0]["timestamp"].endswith("Z") and dlq[0]["consumer_host"]


def test_dlq_unserializable_body_is_repr():
    c, dlq = make_dlq_consumer(status_doc())
    c.on_message(object(), msg())
    assert isinstance(dlq[0]["event"], str)


def test_dlq_poison_after_second_failure_first_requeues():
    submit = umock.MagicMock(side_effect=RuntimeError("es down"))
    c, dlq = make_dlq_consumer(status_doc(A="completed"), submit=submit)
    with umock.patch("hysds.workflow_rules.time.sleep"):
        m1 = msg(redelivered=False)
        c.on_message(ev("A"), m1)
        m1.requeue.assert_called_once()
        m1.ack.assert_not_called()
        assert dlq == []
        m2 = msg(redelivered=True)
        c.on_message(ev("A"), m2)
    m2.ack.assert_called_once()
    m2.requeue.assert_not_called()
    assert len(dlq) == 1
    assert dlq[0]["event"]["step_id"] == "A" and "es down" in dlq[0]["reason"]
    assert dlq[0]["reason"].startswith("poison event")


def test_dlq_publish_failure_requeues_instead_of_losing():
    def boom(payload):
        raise ConnectionError("broker down")
    submit = umock.MagicMock(side_effect=RuntimeError("es down"))
    c, _ = make_consumer(status_doc(A="completed"), submit=submit, dlq_publish_fn=boom)
    with umock.patch("hysds.workflow_rules.time.sleep"):
        m = msg(redelivered=True)
        c.on_message(ev("A"), m)
    m.requeue.assert_called_once()
    m.ack.assert_not_called()


def test_dlq_missing_run_and_unknown_step_and_missing_spec():
    c, dlq = make_dlq_consumer(status=None)
    m = msg()
    c.on_message(ev("A", run_id="gone"), m)
    m.ack.assert_called_once()
    assert "run not found" in dlq[0]["reason"] and dlq[0]["event"]["run_id"] == "gone"
    c2, dlq2 = make_dlq_consumer(status_doc())
    c2.on_message(ev("nope"), msg())
    assert "unknown step_id" in dlq2[0]["reason"]


def test_dlq_default_publish_uses_named_queue():
    conn = umock.MagicMock()
    c = WorkflowRulesConsumer(conn, "workflow_queue", es=umock.MagicMock(), dlq_name="my_dlq")
    c.dead_letter({"run_id": "r"}, "why")
    producer = conn.clone.return_value.__enter__.return_value.Producer.return_value
    kw = producer.publish.call_args.kwargs
    assert kw["routing_key"] == "my_dlq" and kw["exchange"].name == "my_dlq"
    assert producer.publish.call_args[0][0]["reason"] == "why"


def test_healthy_event_not_dead_lettered():
    c, dlq = make_dlq_consumer(status_doc(A="completed"))
    c.on_message(ev("A"), msg())
    assert dlq == []


def _sg_with_barrier(c, ttl_expected=None):
    c.process_event(child(0, n=2))
    assert c.redis.ttl(wb.barrier_keys(RUN, "P")[0]) > 3 * 86400  # creation TTL is long


def test_ttl_set_on_finalize():
    submit = umock.MagicMock()
    c, _ = make_sg_consumer(sg_status(P={"state": "running", "expected": 2}, pass_2="completed"), submit)
    c.barrier_ttl = 777
    _sg_with_barrier(c)
    c.process_event(ev("B", spec_id=SG_ID))  # last job completed -> _finalize
    assert submit.call_args[0][3]["step_id"] == "_finalize"
    for k in wb.barrier_keys(RUN, "P"):
        assert c.redis.ttl(k) == 777


def test_ttl_set_on_halt():
    submit = umock.MagicMock()
    c, _ = make_sg_consumer(sg_status(P={"state": "running", "expected": 2}), submit)
    c.barrier_ttl = 555
    _sg_with_barrier(c)
    c.process_event(ev("A", "job-failed", spec_id=SG_ID, error_class="worker", retry_count=0))
    assert submit.call_args[0][3]["step_id"] == "_halt"
    for k in wb.barrier_keys(RUN, "P"):
        assert c.redis.ttl(k) == 555


def test_ttl_default_is_one_day():
    c, _ = make_sg_consumer(sg_status(P={"state": "running", "expected": 2}), umock.MagicMock())
    assert c.barrier_ttl == 86400


def test_ttl_not_set_on_ordinary_submit_or_retry():
    submit = umock.MagicMock()
    c, _ = make_sg_consumer(sg_status(P={"state": "running", "expected": 2}), submit)
    c._es.get.return_value = {"found": True, "_source": sg_retry_spec()}
    c._retry_job_type = RT
    _sg_with_barrier(c)
    c.process_event(sg_fail(1))  # retry
    c.process_event(ev("A", spec_id=SG_ID))  # pass submit
    for k in wb.barrier_keys(RUN, "P"):
        assert c.redis.ttl(k) > 3 * 86400


def test_ttl_failure_is_best_effort():
    submit = umock.MagicMock()
    c, _ = make_consumer(status_doc(A="running"), submit=submit)
    c._expire_barriers = umock.MagicMock(side_effect=RuntimeError("redis"))
    [a] = c.process_event(ev("A", "job-failed", error_class="worker", retry_count=0))
    assert isinstance(a, Submit) and submit.call_count == 1


def test_ttl_linear_spec_without_scatter_is_noop():
    submit = umock.MagicMock()
    c, _ = make_consumer(status_doc(A="running"), submit=submit)
    c.process_event(ev("A", "job-failed", error_class="worker", retry_count=0))
    assert submit.call_count == 1


# ------------------------------------------------------------------ contract 12.8
def replayed(e):
    return dict(e, replayed=True)


# -- decide: lost gather after barrier release
def test_replayed_child_barrier_already_released_gather_pending_resubmits_gather():
    st = sg_status(P={"state": "completed", "expected": 5}, gather_1="pending")
    [a] = decide(replayed(child(0)), scatter_gather_spec(), st,
                 barrier=wb.BarrierResult(5, 0, 5, False, False, True))
    assert isinstance(a, Submit) and a.workflow["step_id"] == "gather_1" and a.stale_ok
    assert json.loads(a.params["workflow_event"])["status"] == "barrier-released"


def test_replayed_child_barrier_released_gather_queued_resubmits():
    st = sg_status(gather_1="queued")
    [a] = decide(replayed(child(0)), scatter_gather_spec(), st,
                 barrier=wb.BarrierResult(5, 0, 5, False, False, True))
    assert isinstance(a, Submit) and a.workflow["step_id"] == "gather_1"


@pytest.mark.parametrize("state", ["running", "completed", "failed", "cancelled"])
def test_replayed_child_gather_already_running_no_resubmit(state):
    st = sg_status(gather_1=state)
    [a] = decide(replayed(child(0)), scatter_gather_spec(), st,
                 barrier=wb.BarrierResult(5, 0, 5, False, False, True))
    assert isinstance(a, Ignore) and "not pending" in a.reason


def test_replayed_child_barrier_not_released_ignored():
    st = sg_status(gather_1="pending")
    [a] = decide(replayed(child(0)), scatter_gather_spec(), st,
                 barrier=wb.BarrierResult(1, 0, 5, False, False, False))
    assert isinstance(a, Ignore) and "not released" in a.reason


def test_replayed_child_released_run_terminal_ignored():
    st = dict(sg_status(gather_1="pending"), status="failed")
    [a] = decide(replayed(child(0)), scatter_gather_spec(), st,
                 barrier=wb.BarrierResult(5, 0, 5, False, False, True))
    assert isinstance(a, Ignore)


def test_non_replayed_event_unchanged_when_barrier_already_released():
    st = sg_status(gather_1="pending")
    [a] = decide(child(0), scatter_gather_spec(), st,
                 barrier=wb.BarrierResult(5, 0, 5, False, False, True))
    assert isinstance(a, Ignore) and "not released" in a.reason


def test_non_replayed_winner_submit_not_stale_ok():
    st = sg_status(gather_1="pending")
    [a] = decide(child(4), scatter_gather_spec(), st, barrier=barrier_result(5, 0, 5))
    assert isinstance(a, Submit) and a.stale_ok is False


def test_replayed_marks_pass_successor_and_finalize_submits_stale_ok():
    st = status_doc(A="completed", pass_1="pending")
    [a] = decide(replayed(ev("A")), linear_spec(), st)
    assert isinstance(a, Submit) and a.stale_ok
    [f] = decide(replayed(ev("B")), linear_spec(), status_doc(B="running"))
    assert isinstance(f, Submit) and f.stale_ok
    [h] = decide(replayed(ev("B", "job-failed")), linear_spec(), status_doc(B="running"))
    assert isinstance(h, Submit) and h.stale_ok
    [n] = decide(ev("B"), linear_spec(), status_doc(B="running"))
    assert n.stale_ok is False


def test_barrier_released_flag_from_lua():
    import fakeredis
    r = fakeredis.FakeStrictRedis()
    assert wb.record_child(r, "r", "C", "a", "job-completed", 2).released is False
    w = wb.record_child(r, "r", "C", "b", "job-completed", 2)
    assert w.winner and w.released
    again = wb.record_child(r, "r", "C", "b", "job-completed", 2)
    assert not again.winner and again.released


# -- shell: stale claims
class Clock:
    def __init__(self, t=1_700_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def sg_shell(status, **kw):
    kw.setdefault("stall_seconds", 600)
    c, es = make_consumer(status, spec=scatter_gather_spec(), **kw)
    return c, es


def released_barrier(c):
    for i in range(5):
        wb.record_child(c.redis, RUN, "P", f"jc-{i}", "job-completed", 5)


def test_shell_replayed_lost_gather_resubmitted_when_claim_stale_and_no_job():
    clock, submit = Clock(), umock.MagicMock()
    c, _ = sg_shell(sg_status(gather_1="pending"), submit=submit, clock=clock)
    released_barrier(c)
    key = "wf-claim:run-1:gather_1:None"
    c.redis.set(key, repr(clock.t - 601), ex=1000)
    c.process_event(replayed(child(0)))
    submit.assert_called_once()
    assert submit.call_args[0][3]["step_id"] == "gather_1"
    assert float(c.redis.get(key)) == clock.t  # re-claimed with a fresh timestamp


def test_shell_replayed_claim_young_not_resubmitted():
    clock, submit = Clock(), umock.MagicMock()
    c, _ = sg_shell(sg_status(gather_1="pending"), submit=submit, clock=clock)
    released_barrier(c)
    c.redis.set("wf-claim:run-1:gather_1:None", repr(clock.t - 100), ex=1000)
    [a] = c.process_event(replayed(child(0)))
    submit.assert_not_called()
    assert isinstance(a, Ignore) and "already claimed" in a.reason


def test_shell_replayed_claim_old_but_job_exists_not_resubmitted():
    clock, submit = Clock(), umock.MagicMock()
    c, _ = sg_shell(sg_status(gather_1="pending"), submit=submit, clock=clock,
                    job_exists_fn=lambda r, s, ci: (r, s, ci) == (RUN, "gather_1", None))
    released_barrier(c)
    c.redis.set("wf-claim:run-1:gather_1:None", repr(clock.t - 5000), ex=1000)
    c.process_event(replayed(child(0)))
    submit.assert_not_called()


def test_shell_replayed_gather_already_running_no_resubmit_even_with_stale_claim():
    clock, submit = Clock(), umock.MagicMock()
    c, _ = sg_shell(sg_status(gather_1="running"), submit=submit, clock=clock)
    released_barrier(c)
    c.redis.set("wf-claim:run-1:gather_1:None", repr(clock.t - 5000), ex=1000)
    c.process_event(replayed(child(0)))
    submit.assert_not_called()


def test_shell_non_replayed_old_claim_never_taken_over():
    clock, submit = Clock(), umock.MagicMock()
    c, _ = sg_shell(sg_status(gather_1="pending"), submit=submit, clock=clock)
    # barrier winner event with a stale claim held: unchanged behaviour (ignore)
    for i in range(4):
        wb.record_child(c.redis, RUN, "P", f"jc-{i}", "job-completed", 5)
    c.redis.set("wf-claim:run-1:gather_1:None", repr(clock.t - 5000), ex=1000)
    [a] = c.process_event(child(4))
    submit.assert_not_called()
    assert isinstance(a, Ignore) and "already claimed" in a.reason


def test_shell_replayed_non_replayed_redelivery_of_released_barrier_ignored():
    submit = umock.MagicMock()
    c, _ = sg_shell(sg_status(gather_1="pending"), submit=submit)
    released_barrier(c)
    [a] = c.process_event(child(0))
    submit.assert_not_called()
    assert isinstance(a, Ignore)


def test_shell_replayed_stale_claim_no_claim_present_just_submits():
    submit = umock.MagicMock()
    c, _ = make_consumer(status_doc(A="completed", pass_1="pending"), submit=submit)
    c.process_event(replayed(ev("A")))
    submit.assert_called_once()


def test_shell_replayed_pass_successor_stale_claim_taken_over():
    clock, submit = Clock(), umock.MagicMock()
    c, _ = make_consumer(status_doc(A="completed", pass_1="completed", B="queued"), submit=submit,
                         clock=clock)
    key = "wf-claim:run-1:B:None"
    c.redis.set(key, repr(clock.t - 700), ex=1000)
    c.process_event(replayed(ev("pass_1")))
    submit.assert_called_once()


def test_shell_replayed_halt_stale_claim_taken_over_and_legacy_value_uses_ttl():
    clock, submit = Clock(), umock.MagicMock()
    c, _ = make_consumer(status_doc(B="running"), submit=submit, clock=clock)
    key = "wf-claim:run-1:_halt:None"
    c.redis.set(key, "1", ex=30 * 24 * 3600 - 900)  # legacy value, created ~900s ago
    assert 890 < c.claim_age(key) < 910
    c.process_event(replayed(ev("B", "job-failed")))
    submit.assert_called_once()


def test_shell_claim_value_is_timestamp():
    clock = Clock(1234.5)
    c, _ = make_consumer(status_doc(A="completed", pass_1="pending"), clock=clock)
    c.process_event(ev("A"))
    assert float(c.redis.get("wf-claim:run-1:pass_1:None")) == 1234.5


# -- retry claims and stale retry events
def retry_shell(job_doc=None, **kw):
    submit = umock.MagicMock()
    c, _ = make_consumer(status_doc(A="completed", pass_1="completed", B="running"), spec=retry_spec(),
                         submit=submit, retry_job_type=RT, job_doc_fn=lambda e: job_doc, **kw)
    return c, submit


def test_retry_claim_ttl_default_and_configured():
    c, submit = retry_shell()
    c.process_event(fail())
    assert 1190 < c.redis.ttl("wf-claim:run-1:B:None:retry1") <= 1200  # 2 x 600
    c2, _ = retry_shell(stall_seconds=100, retry_claim_ttl=77)
    c2.process_event(fail())
    assert 70 < c2.redis.ttl("wf-claim:run-1:B:None:retry1") <= 77


def test_retry_stale_when_job_retry_count_greater():
    c, submit = retry_shell({"status": "job-failed", "job": {"retry_count": 1}})
    [a] = c.process_event(fail(rc=0))
    submit.assert_not_called()
    assert isinstance(a, Ignore) and "retry_count" in a.reason
    assert c.redis.get("wf-claim:run-1:B:None:retry1") is None


@pytest.mark.parametrize("st", ["job-started", "job-queued", "job-revoked-not", "job-retried"])
def test_retry_stale_when_job_non_terminal(st):
    c, submit = retry_shell({"status": st, "job": {"retry_count": 0}})
    [a] = c.process_event(fail())
    submit.assert_not_called()
    assert isinstance(a, Ignore) and "non-terminal" in a.reason


def test_retry_proceeds_when_job_terminal_and_same_retry_count():
    c, submit = retry_shell({"status": "job-failed", "job": {"retry_count": 0}})
    [a] = c.process_event(fail())
    assert isinstance(a, RetryAction)
    submit.assert_called_once()


def test_retry_proceeds_when_job_doc_missing():
    c, submit = retry_shell(None)
    c.process_event(fail())
    submit.assert_called_once()


def test_es_job_doc_lookup_uses_payload_id_then_job_id():
    c, es = make_consumer(status_doc(B="running"))
    c._job_doc_fn = c._es_job_doc
    es.search.return_value = {"hits": {"hits": [{"_source": {"status": "job-failed"}}]}}
    assert c._es_job_doc({"payload_id": "pid", "job_id": "jid"}) == {"status": "job-failed"}
    body = es.search.call_args.kwargs["body"]
    assert body["query"]["bool"]["should"][0] == {"ids": {"values": ["pid"]}}
    es.search.return_value = {"hits": {"hits": []}}
    assert c._es_job_doc({"job_id": "jid"}) is None


# -- retry waits for the failed job's status doc to become visible (indexer lag)
def _docs_then(docs):
    it = iter(docs)
    last = [None]

    def fn(event):
        try:
            last[0] = next(it)
        except StopIteration:
            pass
        return last[0]
    return fn


def _lag_shell(docs, wait=15):
    submit = umock.MagicMock()
    sleeps = []
    c, _ = make_consumer(status_doc(A="completed", pass_1="completed", B="running"), spec=retry_spec(),
                         submit=submit, retry_job_type=RT, job_doc_fn=_docs_then(docs),
                         retry_visibility_wait=wait, sleep_fn=sleeps.append)
    return c, submit, sleeps


def test_retry_waits_until_doc_is_visible():
    c, submit, sleeps = _lag_shell([None, None, {"status": "job-failed", "job": {}}])
    [a] = c.process_event(fail())
    assert isinstance(a, RetryAction)
    submit.assert_called_once()
    assert len(sleeps) == 2


def test_retry_waits_until_doc_is_terminal():
    c, submit, sleeps = _lag_shell([{"status": "job-started", "job": {}}, {"status": "job-failed", "job": {}}])
    [a] = c.process_event(fail())
    assert isinstance(a, RetryAction)
    submit.assert_called_once()


def test_retry_still_non_terminal_after_wait_is_stale():
    c, submit, sleeps = _lag_shell([{"status": "job-started", "job": {}}], wait=3)
    [a] = c.process_event(fail())
    assert isinstance(a, Ignore) and "non-terminal" in a.reason
    submit.assert_not_called()
    assert len(sleeps) == 3


def test_retry_never_visible_proceeds_after_wait():
    c, submit, sleeps = _lag_shell([None], wait=2)
    c.process_event(fail())
    submit.assert_called_once()
    assert len(sleeps) == 2


def test_retry_stale_retry_count_does_not_wait():
    c, submit, sleeps = _lag_shell([{"status": "job-failed", "job": {"retry_count": 1}}])
    [a] = c.process_event(fail(rc=0))
    assert isinstance(a, Ignore)
    assert sleeps == []
