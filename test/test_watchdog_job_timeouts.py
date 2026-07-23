import importlib.util
import logging
import pathlib
import sys
import unittest.mock as umock
from datetime import datetime, timedelta, timezone

# hysds.celery searches for configuration on import, so mock it before any
# hysds import. scripts/ is not a package: the watchdog imports its sibling
# job_utils and (on some platforms) the non-installed `future` package -- stub
# both before loading it by path. setdefault to avoid cross-test pollution.
sys.modules.setdefault("hysds.celery", umock.MagicMock())
sys.modules.setdefault("future", umock.MagicMock())
sys.modules.setdefault("job_utils", umock.MagicMock())

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
_spec = importlib.util.spec_from_file_location(
    "watchdog_job_timeouts", SCRIPTS / "watchdog_job_timeouts.py"
)
watchdog = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(watchdog)


def _ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _job_doc(
    status="job-started",
    time_limit=100000,
    started_secs_ago=3600,
    uuid="task-1",
    tags=None,
):
    now = datetime.now(timezone.utc)
    return {
        "_id": "payload-1",
        "_index": "job_status-2026.07.20",
        "_source": {
            "status": status,
            "tags": list(tags or []),
            "uuid": uuid,
            "celery_hostname": "worker-1",
            "job": {
                "job_info": {
                    "time_limit": time_limit,
                    "time_start": _ts(now - timedelta(seconds=started_secs_ago)),
                }
            },
        },
    }


def _task_failed_hit(secs_ago, exception="WorkerLostError('signal 9 (SIGKILL).')"):
    now = datetime.now(timezone.utc)
    return [
        {
            "_source": {
                "status": "task-failed",
                "@timestamp": _ts(now - timedelta(seconds=secs_ago)),
                "event": {"exception": exception, "traceback": "tb"},
            }
        }
    ]


def _wire(
    monkeypatch,
    docs,
    task_hits=None,
    worker_hits=None,
    finalized=False,
    redis_status=None,
):
    """Wire the watchdog's collaborators to mocks.

    Returns (write mock, guard mock) -- the guard is a recording mock so
    tests can assert it was not consulted on paths that must not reach it.
    """
    mock_ju = umock.MagicMock()
    mock_ju.run_query_with_scroll.return_value = docs

    def es_query(query, index=None):
        if index == "task_status-current":
            return {"hits": {"hits": task_hits or []}}
        return {"hits": {"hits": worker_hits or []}}

    mock_ju.es_query.side_effect = es_query
    monkeypatch.setattr(watchdog, "job_utils", mock_ju)
    guard = umock.MagicMock(return_value=finalized)
    monkeypatch.setattr(watchdog, "is_job_finalized", guard)
    if redis_status is None:
        redis_status = "job-failed" if finalized else "job-started"
    monkeypatch.setattr(watchdog, "get_job_status", lambda task_id: redis_status)
    mock_log = umock.MagicMock()
    monkeypatch.setattr(watchdog, "log_job_status", mock_log)
    return mock_log, guard


def test_guard_reads_redis_not_es():
    """Pin the guard's import provenance to hysds.log_utils (redis); the
    identically-named hysds.utils.get_job_status reads lagging ES."""
    import hysds.log_utils as lu

    assert watchdog.is_job_finalized is lu.is_job_finalized


def test_within_grace_defers(monkeypatch):
    """AC3: a fresh task-failed event defers to the worker, writes nothing,
    and does not even consult the guard (a loop-top guard would)."""
    mock_log, guard = _wire(
        monkeypatch, [_job_doc()], task_hits=_task_failed_hit(secs_ago=30)
    )
    watchdog.tag_timedout_jobs("http://es", 300, grace_secs=300)
    mock_log.assert_not_called()
    guard.assert_not_called()


def test_within_grace_defers_even_when_tag_eligible(monkeypatch):
    """The deferral must skip the tag lane too: a doc past its time_limit
    inside the grace window still gets NO write (any write re-stamps
    @timestamp and re-arms the sweep clock)."""
    mock_log, guard = _wire(
        monkeypatch,
        [_job_doc(time_limit=600, started_secs_ago=3600)],
        task_hits=_task_failed_hit(secs_ago=30),
    )
    watchdog.tag_timedout_jobs("http://es", 300, grace_secs=300)
    mock_log.assert_not_called()
    guard.assert_not_called()


def test_past_grace_reaps(monkeypatch):
    """AC5: a genuinely lost worker is still reaped once the grace elapses."""
    mock_log, guard = _wire(
        monkeypatch, [_job_doc()], task_hits=_task_failed_hit(secs_ago=600)
    )
    watchdog.tag_timedout_jobs("http://es", 300, grace_secs=300)
    mock_log.assert_called_once()
    written = mock_log.call_args[0][0]
    assert written["status"] == "job-failed"
    assert "WorkerLostError" in written["error"]
    assert written["traceback"] == "tb"


def test_finalized_job_skipped(monkeypatch, caplog):
    """AC2: past the grace, the redis guard declines a finalized job -- and
    a stale non-terminal ES doc is surfaced as a DIVERGENCE health signal."""
    mock_log, guard = _wire(
        monkeypatch,
        [_job_doc()],
        task_hits=_task_failed_hit(secs_ago=600),
        finalized=True,
    )
    with caplog.at_level(logging.INFO):
        watchdog.tag_timedout_jobs("http://es", 300, grace_secs=300)
    mock_log.assert_not_called()
    assert "DIVERGENCE" in caplog.text
    assert "divergence_count=1" in caplog.text


def test_grace_configurable(monkeypatch):
    """AC3: the grace period is honored from the CLI flag value."""
    mock_log, guard = _wire(
        monkeypatch, [_job_doc()], task_hits=_task_failed_hit(secs_ago=30)
    )
    watchdog.tag_timedout_jobs("http://es", 300, grace_secs=10)
    mock_log.assert_called_once()


def test_missing_task_timestamp_fails_open(monkeypatch):
    """A task doc without @timestamp must not disable reaping (guard still
    consulted)."""
    hit = _task_failed_hit(secs_ago=600)
    del hit[0]["_source"]["@timestamp"]
    mock_log, guard = _wire(monkeypatch, [_job_doc()], task_hits=hit)
    watchdog.tag_timedout_jobs("http://es", 300, grace_secs=300)
    mock_log.assert_called_once()


def test_malformed_task_timestamp_fails_open(monkeypatch):
    """A non-string @timestamp must neither defer forever nor abort the
    sweep -- it falls through to the redis guard."""
    hit = _task_failed_hit(secs_ago=600)
    hit[0]["_source"]["@timestamp"] = 12345
    mock_log, guard = _wire(monkeypatch, [_job_doc()], task_hits=hit)
    watchdog.tag_timedout_jobs("http://es", 300, grace_secs=300)
    mock_log.assert_called_once()


def test_timedout_tagging_guarded(monkeypatch, caplog):
    """AC2: the tag-only path republishes the whole stale _source (including
    status), so it must decline on a finalized job. A consistent decline
    (redis and ES both job-offline) is NOT a divergence."""
    mock_log, guard = _wire(
        monkeypatch,
        [_job_doc(status="job-offline", time_limit=600, started_secs_ago=3600)],
        finalized=True,
        redis_status="job-offline",
    )
    with caplog.at_level(logging.INFO):
        watchdog.tag_timedout_jobs("http://es", 300, grace_secs=300)
    mock_log.assert_not_called()
    assert "DIVERGENCE" not in caplog.text


def test_job_offline_docs_still_enter_the_loop(monkeypatch):
    """Guard-placement regression: a top-of-loop guard would silently
    disable the whole job-offline tagging lane."""
    mock_log, guard = _wire(
        monkeypatch,
        [_job_doc(status="job-offline", time_limit=600, started_secs_ago=3600)],
        finalized=False,
    )
    watchdog.tag_timedout_jobs("http://es", 300, grace_secs=300)
    mock_log.assert_called_once()
    written = mock_log.call_args[0][0]
    assert "timedout" in written["tags"]
    assert written["status"] == "job-offline"


def test_partial_source_doc_does_not_abort_sweep(monkeypatch):
    """One malformed document must not strand jobs later in the scroll."""
    partial = {"_id": "payload-0", "_index": "job_status-2026.07.20", "_source": {}}
    good = _job_doc(status="job-offline", time_limit=600, started_secs_ago=3600)
    mock_log, guard = _wire(monkeypatch, [partial, good], finalized=False)
    watchdog.tag_timedout_jobs("http://es", 300, grace_secs=300)
    mock_log.assert_called_once()
    assert "timedout" in mock_log.call_args[0][0]["tags"]


def test_non_numeric_time_limit_does_not_abort_sweep(monkeypatch):
    """A doc whose time_limit is null/string must be skipped like any other
    partial doc instead of raising past the per-doc handler."""
    bad = _job_doc(status="job-offline", time_limit=None, started_secs_ago=3600)
    good = _job_doc(status="job-offline", time_limit=600, started_secs_ago=3600)
    mock_log, guard = _wire(monkeypatch, [bad, good], finalized=False)
    watchdog.tag_timedout_jobs("http://es", 300, grace_secs=300)
    mock_log.assert_called_once()
    assert "timedout" in mock_log.call_args[0][0]["tags"]
