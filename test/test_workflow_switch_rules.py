"""workflow_rules: switch / gate / halt routing (contract v1.2 Consumer section)."""
import json
import os

import pytest

from hysds.workflow_rules import DeadLetter, Ignore, Submit, decide

SPEC = json.load(open(os.path.join(os.path.dirname(__file__), "fixtures", "spec-switch.json")))
SID = "e2e-switch:1.0.0"
RUN = "run-1"
RESOLVED = json.loads(json.dumps(SPEC))
for _n in RESOLVED["nodes"].values():
    if _n["kind"] in ("switch", "gate"):
        _n["connector"], _n["connector_queue"] = "job-workflow-switch:v1.0.0", "workflow-connector-switch"
    elif _n["kind"] == "pass":
        _n["connector"], _n["connector_queue"] = "job-workflow-pass:v1.0.0", "workflow-connector-pass"


def ev(step, status="job-completed", outputs=None, **kw):
    e = {"run_id": RUN, "step_id": step, "spec_id": SID, "child_index": None, "child_total": None,
         "job_id": "j-" + step, "payload_id": "p-" + step, "status": status, "short_error": None,
         "workflow_outputs": outputs}
    e.update(kw)
    return e


def arm_out(arm, target):
    return [{"name": "arm", "value_s": arm}, {"name": "target", "value_s": target}]


def status(**states):
    nodes = {n: {"state": "pending"} for n in SPEC["nodes"]}
    for n, s in states.items():
        nodes[n] = s if isinstance(s, dict) else {"state": s}
    return {"run_id": RUN, "spec_id": SID, "status": "running",
            "inputs": {"qa_min": 0.8, "tier": "gold", "l2_outputs": "x", "reprocess_outputs": "r",
                       "publish_outputs": "p", "archive_outputs": "a"}, "nodes": nodes}


L2 = {"state": "completed", "outputs": {"qa_score": 0.9, "product": "p.h5"}}


def hevent(a):
    return json.loads(a.params["workflow_event"])


def test_job_completed_submits_switch_connector():
    [a] = decide(ev("L2SMP", outputs=[{"name": "qa_score", "value_d": 0.9}]), RESOLVED, status())
    assert isinstance(a, Submit)
    assert a.job_type == "job-workflow-switch:v1.0.0" and a.queue == "workflow-connector-switch"
    assert a.workflow["step_id"] == "qa_route" and hevent(a)["step_id"] == "L2SMP"


def test_job_to_switch_precheck_and_fallback_without_resolved_connector():
    assert isinstance(decide(ev("L2SMP"), SPEC, status())[0], Submit)
    [a] = decide(ev("L2SMP"), SPEC, status())
    assert a.job_type == "job-workflow-switch:v1.0.0" and a.queue == "workflow-connector-switch"
    [i] = decide(ev("L2SMP"), RESOLVED, status(qa_route="completed"))
    assert isinstance(i, Ignore) and "not pending" in i.reason


def test_gate_uses_the_switch_connector():
    st = status(L2SMP=L2, publish="completed")
    [a] = decide(ev("publish"), RESOLVED, st)
    assert a.job_type == "job-workflow-switch:v1.0.0" and a.workflow["step_id"] == "qa_gate"
    [a] = decide(ev("publish"), SPEC, st)  # unresolved spec: gate falls back to the switch connector
    assert a.job_type == "job-workflow-switch:v1.0.0" and a.queue == "workflow-connector-switch"


def test_switch_completed_default_arm_submits_job_with_resolved_inputs():
    st = status(L2SMP=L2, qa_route={"state": "completed", "outputs": {"arm": "default", "target": "publish"}},
                publish="queued")
    [a] = decide(ev("qa_route", outputs=arm_out("default", "publish")), RESOLVED, st)
    assert isinstance(a, Submit) and a.job_type == "job-wf_test:v1.0"
    assert a.params == {"mode": "ok", "outputs": "p", "route": "default", "product": "p.h5"}
    assert a.workflow["step_id"] == "publish" and a.tag == "publish"


def test_switch_completed_job_arm_accepts_pending_and_rejects_running():
    e = ev("qa_route", outputs=arm_out("reprocess", "reprocess_L2SMP"))
    st = status(L2SMP=L2)
    assert isinstance(decide(e, RESOLVED, st)[0], Submit)
    st["nodes"]["reprocess_L2SMP"]["state"] = "running"
    [i] = decide(e, RESOLVED, st)
    assert isinstance(i, Ignore) and "pending/queued" in i.reason


def test_switch_arm_label_is_mapped_through_spec_not_event_target():
    # event says target=archive_gold but the spec maps 'reprocess' -> reprocess_L2SMP
    [a] = decide(ev("qa_route", outputs=arm_out("reprocess", "archive_gold")), RESOLVED, status(L2SMP=L2))
    assert a.workflow["step_id"] == "reprocess_L2SMP"


@pytest.mark.parametrize("outputs", [arm_out("bogus", "publish"), None, [{"name": "target", "value_s": "x"}]])
def test_unknown_or_missing_arm_is_dead_lettered(outputs):
    [a] = decide(ev("qa_route", outputs=outputs), RESOLVED, status(L2SMP=L2))
    assert isinstance(a, DeadLetter)


def test_switch_arm_to_halt_failed_submits_halt_with_halted_class():
    [a] = decide(ev("qa_route", outputs=arm_out("reject", "reject")), RESOLVED, status(L2SMP=L2))
    assert a.job_type == "job-workflow-halt:v1.0.0" and a.workflow["step_id"] == "_halt"
    h = hevent(a)
    assert h["step_id"] == "reject" and h["error_class"] == "halted" and h["status"] == "job-failed"
    assert h["short_error"] == "L2SMP qa_score below 0.3" and "predecessor" not in h


def test_gate_arm_to_halt_completed_submits_finalize():
    [a] = decide(ev("qa_gate", outputs=arm_out("true", "finish")), RESOLVED, status(L2SMP=L2))
    assert a.job_type == "job-workflow-finalize:v1.0.0" and a.workflow["step_id"] == "_finalize"
    h = hevent(a)
    assert h["step_id"] == "finish" and h["status"] == "job-completed" and not h.get("error_class")


def test_switch_arm_to_another_switch_submits_connector():
    [a] = decide(ev("qa_gate", outputs=arm_out("false", "tier")), RESOLVED, status(L2SMP=L2))
    assert a.job_type == "job-workflow-switch:v1.0.0" and a.workflow["step_id"] == "tier"
    assert hevent(a)["step_id"] == "qa_gate"


def test_job_directly_to_halt_attaches_predecessor():
    spec = json.loads(json.dumps(RESOLVED))
    spec["nodes"]["L2SMP"]["next"] = ["reject"]
    [a] = decide(ev("L2SMP", outputs=[{"name": "qa_score", "value_d": 0.1}]), spec, status())
    h = hevent(a)
    assert h["step_id"] == "reject" and h["predecessor"]["step_id"] == "L2SMP"
    assert h["predecessor"]["workflow_outputs"] == [{"name": "qa_score", "value_d": 0.1}]


def test_halt_precheck_duplicate():
    [i] = decide(ev("qa_route", outputs=arm_out("reject", "reject")), RESOLVED, status(L2SMP=L2, reject="failed"))
    assert isinstance(i, Ignore) and "not pending" in i.reason


def test_switch_connector_failure_maps_error_class():
    for token in ("input_resolution", "routing"):
        e = ev("qa_route", "job-failed", error_class="pge:" + token, short_error="qa_route: x")
        [a] = decide(e, RESOLVED, status(L2SMP=L2))
        assert a.job_type == "job-workflow-halt:v1.0.0"
        h = hevent(a)
        assert h["step_id"] == "qa_route" and h["error_class"] == token
    e = ev("qa_route", "job-failed", error_class="exit_code:1")
    assert hevent(decide(e, RESOLVED, status())[0])["error_class"] == "exit_code:1"


def test_last_job_on_an_arm_finalizes():
    [a] = decide(ev("archive_gold"), RESOLVED, status(archive_gold="running"))
    assert a.job_type == "job-workflow-finalize:v1.0.0"


def test_terminal_run_ignores_switch_event():
    st = status(L2SMP=L2)
    st["status"] = "failed"
    assert isinstance(decide(ev("qa_route", outputs=arm_out("default", "publish")), RESOLVED, st)[0], Ignore)
