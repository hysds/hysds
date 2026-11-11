"""Orchestrator copies the top-level ``workflow`` tag into each queued job dict
and folds it into the dedup payload hash."""
import importlib
import json
import sys
import types
import unittest.mock as umock

import pytest

sys.modules.setdefault("hysds.celery", umock.MagicMock())

WF = {"run_id": "run-1", "step_id": "step-1", "spec_id": "sp", "child_index": 3, "child_total": 4}


@pytest.fixture
def orch(monkeypatch, tmp_path):
    celery_mod = sys.modules["hysds.celery"]
    monkeypatch.setattr(celery_mod.app.task, "side_effect", lambda f: f, raising=False)
    import hysds.orchestrator as o

    o = importlib.reload(o)
    cfg = tmp_path / "orch.json"
    cfg.write_text(json.dumps({"configs": []}))
    monkeypatch.setenv("HYSDS_ORCHESTRATOR_CFG", str(cfg))
    monkeypatch.setenv("HYSDS_JOB_CREATORS_DIR", str(tmp_path))
    o.submit_job.request = types.SimpleNamespace(id="payload-1", delivery_info={"exchange": "jobs_processed"})
    monkeypatch.setattr(o, "get_function", lambda *a, **k: (lambda payload: {"name": "job-x"}))
    monkeypatch.setattr(o, "do_run_job", umock.MagicMock())
    monkeypatch.setattr(o, "ensure_hard_time_limit_gap", lambda s, t: (s, t))
    monkeypatch.setattr(o, "log_job_status", umock.MagicMock())
    monkeypatch.setattr(o, "queue_finished_job", umock.MagicMock())
    monkeypatch.setattr(o, "query_dedup_job", umock.MagicMock(side_effect=o.NoDedupJobFoundException("none")))
    yield o
    # drop the identity-decorated module so later tests re-import cleanly
    sys.modules.pop("hysds.orchestrator", None)


def _j(**extra):
    j = {"job_type": "job:x:v1", "job_queue": "q", "payload": {"_command": "x", "a": 1}}
    j.update(extra)
    return j


def _queued_job(o):
    return o.do_run_job.call_args[0][0]


def test_workflow_copied_into_job_dict(orch):
    orch.submit_job(_j(workflow=WF))
    assert _queued_job(orch)["workflow"] == WF
    # and rides on the job-queued status doc's job dict
    status = orch.log_job_status.call_args[0][0]
    assert status["job"]["workflow"] == WF


def test_no_workflow_leaves_job_dict_unchanged(orch):
    orch.submit_job(_j())
    assert "workflow" not in _queued_job(orch)


def test_dedup_hash_includes_workflow_tags(orch):
    from hysds.utils import get_payload_hash

    orch.submit_job(_j(workflow=WF))
    assert orch.query_dedup_job.call_args[0][0] == get_payload_hash({"_command": "x", "a": 1}, WF)
    orch.submit_job(_j(workflow=dict(WF, child_index=4)))
    assert orch.query_dedup_job.call_args[0][0] != get_payload_hash({"_command": "x", "a": 1}, WF)


def test_untagged_hash_unchanged(orch):
    from hysds.utils import get_payload_hash

    orch.submit_job(_j())
    assert orch.query_dedup_job.call_args[0][0] == get_payload_hash({"_command": "x", "a": 1})
