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


def test_scatter_child_gather_ignored_phase3():
    sg = scatter_gather_spec()
    st = {"run_id": RUN, "spec_id": "demo-scatter:1.0.0", "status": "running", "inputs": {},
          "nodes": {n: {"state": "pending"} for n in sg["nodes"]}}
    [a] = decide(ev("P", child_index=0, child_total=3), sg, st)
    assert isinstance(a, Ignore) and "not implemented in v1 phase 2" in a.reason
    [a] = decide(ev("gather_1"), sg, st)
    assert isinstance(a, Ignore) and "not implemented in v1 phase 2" in a.reason


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
    c = WorkflowRulesConsumer(umock.MagicMock(), "workflow_queue", es=es, get_status_fn=get_status_fn,
                              submit_fn=submit or umock.MagicMock())
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
