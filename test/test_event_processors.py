import importlib.util
import pathlib
import sys
import unittest.mock as umock

import pytest

# hysds.celery reads configuration on import, so mock it before any hysds
# import. event_processors also instantiates a mozart ES client at module
# scope (get_mozart_es -> reads ~/.netrc-os), so hysds.es_util must be mocked
# too. FORCE both (not setdefault): under CI's single-process collection an
# earlier test file imports the real modules first, which would make setdefault
# a no-op and let the netrc read escape. unsafe=True because user_rules_job
# does `from hysds.es_util import assert_doc_settled` and a default MagicMock
# raises AttributeError for attributes starting with "assert". Snapshot and
# restore afterwards so this does not pollute later test files (test_user_rules,
# test_triage) that need the real modules.
# restore es_util and the two modules that bind it at import, so the mock does
# not leak into later test files. celery stays mocked (test_job_worker does the
# same). event_processors stays cached on purpose -- the regex-provenance test
# below relies on `pe.TASK_FAILED_RE is ep.TASK_FAILED_RE`, which requires the
# script to import this exact module object.
_saved = {k: sys.modules.get(k) for k in
          ("hysds.es_util", "hysds.user_rules_job", "hysds.task_worker")}
sys.modules["hysds.celery"] = umock.MagicMock()
sys.modules["hysds.es_util"] = umock.MagicMock(unsafe=True)

import hysds.event_processors as ep  # noqa: E402

for _k, _v in _saved.items():
    if _v is not None:
        sys.modules[_k] = _v
    else:
        sys.modules.pop(_k, None)

# (exception string, should trigger the ES-overwrite path)
CASES = [
    ("WorkerExecutionError('SoftTimeLimitExceeded()')", False),
    ("SoftTimeLimitExceeded()", False),
    (
        "UnpickleableExceptionWrapper: WorkerExecutionError('SoftTimeLimitExceeded()')",
        False,
    ),
    ("TimeLimitExceeded(3600,)", True),
    ("celery.exceptions.TimeLimitExceeded(3600,)", True),
    ("WorkerLostError('Worker exited prematurely: signal 9 (SIGKILL).')", True),
    ("ConnectionError('Error 111 connecting to redis')", True),
]


@pytest.mark.parametrize("exc,should_match", CASES)
def test_task_failed_re_classification(exc, should_match):
    """AC1: SoftTimeLimitExceeded must not false-match TimeLimitExceeded."""
    assert bool(ep.TASK_FAILED_RE.search(exc)) is should_match


def test_guard_reads_redis_not_es():
    """Two functions named get_job_status exist with opposite staleness:
    hysds.log_utils (redis, correct) and hysds.utils (ES, the lagging store
    this guard exists to avoid). Pin the import provenance -- a wrong import
    passes every behavioral test here and does nothing in production."""
    import hysds.log_utils as lu

    assert ep.is_job_finalized is lu.is_job_finalized


def test_fail_job_skips_when_worker_finalized(monkeypatch):
    """AC2: when redis says the job is terminal the guard declines -- no ES
    read, no write, and no rule requeue. Skipping queue_finished_job here is
    correct: the worker's own job-failed path already triggers rule
    evaluation, so the supervisory path must not re-fire it."""
    monkeypatch.setattr(ep, "is_job_finalized", lambda uuid: True)
    mock_es = umock.MagicMock()
    monkeypatch.setattr(ep, "mozart_es", mock_es)
    mock_log = umock.MagicMock()
    monkeypatch.setattr(ep, "log_job_status", mock_log)
    mock_finished = umock.MagicMock()
    monkeypatch.setattr(ep, "queue_finished_job", mock_finished)

    ep.fail_job({"traceback": "tb"}, "uuid-1", "exc", "short")

    mock_es.search.assert_not_called()
    mock_log.assert_not_called()
    mock_finished.assert_not_called()


def test_fail_job_proceeds_when_worker_not_finalized(monkeypatch):
    """AC5: a genuinely lost worker is still reaped."""
    monkeypatch.setattr(ep, "is_job_finalized", lambda uuid: False)
    mock_es = umock.MagicMock()
    mock_es.search.return_value = {
        "hits": {
            "total": {"value": 1},
            "hits": [
                {
                    "_index": "job_status-current",
                    "_source": {
                        "status": "job-started",
                        "payload_id": "p1",
                        "uuid": "uuid-1",
                        "job": {"job_info": {}},
                    },
                }
            ],
        }
    }
    monkeypatch.setattr(ep, "mozart_es", mock_es)
    mock_log = umock.MagicMock()
    monkeypatch.setattr(ep, "log_job_status", mock_log)
    mock_finished = umock.MagicMock()
    monkeypatch.setattr(ep, "queue_finished_job", mock_finished)

    ep.fail_job(
        {"traceback": "tb"},
        "uuid-1",
        "WorkerLostError('Worker exited prematurely: signal 9 (SIGKILL).')",
        "WorkerLostError",
    )

    mock_log.assert_called_once()
    written = mock_log.call_args[0][0]
    assert written["status"] == "job-failed"
    assert written["short_error"] == "WorkerLostError"
    assert written["job"]["job_info"]["time_end"].endswith("Z")
    mock_finished.assert_called_once()


def test_script_imports_the_package_regex():
    """scripts/process_events.py must consume the shared regex, not a stale
    local copy -- reverting only the script hunk brings the false-match back."""
    sys.modules.setdefault("future", umock.MagicMock())
    scripts = pathlib.Path(__file__).resolve().parents[1] / "scripts"
    spec = importlib.util.spec_from_file_location(
        "process_events", scripts / "process_events.py"
    )
    pe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pe)
    assert pe.TASK_FAILED_RE is ep.TASK_FAILED_RE


def test_retry_does_not_self_arm_guard(monkeypatch):
    """A transient queue_finished_job failure must not let the backoff retry
    read the redis key the first attempt wrote and silently drop the requeue."""
    state = {"finalized": False, "queue_calls": 0}
    monkeypatch.setattr(ep, "is_job_finalized", lambda uuid: state["finalized"])
    mock_es = umock.MagicMock()

    def fresh_stale_response(*args, **kwargs):
        # each retry re-queries ES, which is still serving the stale doc
        return {
            "hits": {
                "total": {"value": 1},
                "hits": [
                    {
                        "_index": "job_status-current",
                        "_source": {
                            "status": "job-started",
                            "payload_id": "p1",
                            "uuid": "uuid-1",
                            "job": {"job_info": {}},
                        },
                    }
                ],
            }
        }

    mock_es.search.side_effect = fresh_stale_response
    monkeypatch.setattr(ep, "mozart_es", mock_es)

    def log_side_effect(job_status):
        # the synchronous setex the real write performs
        state["finalized"] = True

    monkeypatch.setattr(
        ep, "log_job_status", umock.MagicMock(side_effect=log_side_effect)
    )

    def queue_side_effect(payload_id, index=None):
        state["queue_calls"] += 1
        if state["queue_calls"] == 1:
            raise ConnectionError("transient hiccup")

    monkeypatch.setattr(
        ep, "queue_finished_job", umock.MagicMock(side_effect=queue_side_effect)
    )

    ep.fail_job(
        {"traceback": "tb"},
        "uuid-1",
        "WorkerLostError('Worker exited prematurely: signal 9 (SIGKILL).')",
        "WorkerLostError",
    )

    assert state["queue_calls"] == 2


def test_fail_job_leaves_terminal_es_doc_alone(monkeypatch):
    """Second line of defence: the pre-existing ES-status else-branch."""
    monkeypatch.setattr(ep, "is_job_finalized", lambda uuid: False)
    mock_es = umock.MagicMock()
    mock_es.search.return_value = {
        "hits": {
            "total": {"value": 1},
            "hits": [
                {
                    "_index": "job_status-current",
                    "_source": {"status": "job-failed", "payload_id": "p1"},
                }
            ],
        }
    }
    monkeypatch.setattr(ep, "mozart_es", mock_es)
    mock_log = umock.MagicMock()
    monkeypatch.setattr(ep, "log_job_status", mock_log)

    ep.fail_job({"traceback": "tb"}, "uuid-1", "exc", "short")

    mock_log.assert_not_called()
