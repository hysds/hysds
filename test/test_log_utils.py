import sys
import unittest.mock as umock

import pytest

# hysds.celery searches for configuration on import, so mock it before any
# hysds import. setdefault to avoid cross-test pollution.
sys.modules.setdefault("hysds.celery", umock.MagicMock())

import hysds.log_utils as lu  # noqa: E402

TERMINAL = ["job-completed", "job-failed", "job-deduped", "job-offline", "job-revoked"]
NON_TERMINAL = ["job-queued", "job-started"]


@pytest.mark.parametrize("status", TERMINAL)
def test_is_job_finalized_true_for_terminal(monkeypatch, status):
    monkeypatch.setattr(lu, "get_job_status", lambda task_id: status)
    assert lu.is_job_finalized("uuid-1") is True


@pytest.mark.parametrize("status", NON_TERMINAL)
def test_is_job_finalized_false_for_non_terminal(monkeypatch, status):
    monkeypatch.setattr(lu, "get_job_status", lambda task_id: status)
    assert lu.is_job_finalized("uuid-1") is False


def test_is_job_finalized_false_on_missing_key(monkeypatch):
    """Fail open: absence of evidence must not disable the backstop."""
    monkeypatch.setattr(lu, "get_job_status", lambda task_id: None)
    assert lu.is_job_finalized("uuid-1") is False


def test_is_job_finalized_false_on_redis_error(monkeypatch):
    def boom(task_id):
        raise ConnectionError("redis down")

    monkeypatch.setattr(lu, "get_job_status", boom)
    assert lu.is_job_finalized("uuid-1") is False


def _run_log_job_status(monkeypatch, job):
    """Run log_job_status with redis mocked out; return the mock client."""
    # monkeypatch (never plain assignment): lu.app is the session-shared
    # hysds.celery mock, and stray conf values would leak into every
    # later-collected test file
    monkeypatch.setattr(lu.app.conf, "HYSDS_JOB_STATUS_EXPIRES", 86400, raising=False)
    monkeypatch.setattr(lu.app.conf, "HARD_TIME_LIMIT_GAP", 300, raising=False)
    monkeypatch.setattr(lu.app.conf, "REDIS_JOB_STATUS_KEY", "logstash", raising=False)
    monkeypatch.setattr(lu, "set_redis_job_status_pool", lambda: None)
    mock_redis = umock.MagicMock()
    monkeypatch.setattr(lu, "StrictRedis", umock.MagicMock(return_value=mock_redis))
    lu.log_job_status(job)
    return mock_redis


def _job(time_limit):
    job = {
        "uuid": "uuid-1",
        "status": "job-started",
        "job": {"type": "test", "job_info": {}},
    }
    if time_limit is not None:
        job["job"]["job_info"]["time_limit"] = time_limit
    return job


def test_status_key_ttl_exceeds_long_time_limit(monkeypatch):
    """The key must outlive an 86400s-limit job or the guard is disabled for
    exactly the population most likely to soft-limit."""
    r = _run_log_job_status(monkeypatch, _job(86700))
    ttl = r.setex.call_args[0][1]
    assert ttl == 86700 + 2 * 300
    assert ttl > 86400


def test_status_key_ttl_floors_at_configured_expires(monkeypatch):
    r = _run_log_job_status(monkeypatch, _job(600))
    assert r.setex.call_args[0][1] == 86400


def test_status_key_ttl_defaults_without_time_limit(monkeypatch):
    r = _run_log_job_status(monkeypatch, _job(None))
    assert r.setex.call_args[0][1] == 86400


def test_status_key_ttl_ignores_non_int_time_limit(monkeypatch):
    r = _run_log_job_status(monkeypatch, _job("86700"))
    assert r.setex.call_args[0][1] == 86400


def test_status_key_ttl_extends_for_float_time_limit(monkeypatch):
    """A float time_limit must extend the TTL like an int (the watchdog accepts
    floats); the resulting TTL stays an integer for setex."""
    r = _run_log_job_status(monkeypatch, _job(86700.5))
    ttl = r.setex.call_args[0][1]
    assert ttl == 86700 + 2 * 300
    assert isinstance(ttl, int)
