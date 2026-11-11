"""workflow_rules: pure decide() table tests and the kombu consumer shell (mocked I/O)."""
import json
import unittest.mock as umock

import pytest

from hysds_commons.workflow_examples import linear_spec, scatter_gather_spec
from hysds_commons.workflow_status import WorkflowStatusNotFound
from hysds.workflow_rules import (
    DeadLetter, Ignore, Submit, WorkflowRulesConsumer, decide,
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
def make_consumer(status=None, spec=None, submit=None, get_status_fn=None):
    es = umock.MagicMock()
    es.get.return_value = {"found": True, "_source": spec if spec is not None else linear_spec()}
    if get_status_fn is None:
        def get_status_fn(client, index, run_id):
            if status is None:
                raise WorkflowStatusNotFound(run_id)
            return status, 1, 1
    import fakeredis
    c = WorkflowRulesConsumer(umock.MagicMock(), "workflow_queue", es=es, get_status_fn=get_status_fn,
                              submit_fn=submit or umock.MagicMock(), redis_client=fakeredis.FakeStrictRedis())
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
