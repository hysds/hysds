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


# --- job_supersession ------------------------------------------------------


def _mget_stub(monkeypatch, docs_by_index, errors=None, raise_exc=None):
    """Install a fake hysds.es_util whose client answers one mget.

    docs_by_index: {index: hit}; errors: {index: error_type}; anything else
    answers found=False. Records the request so tests can assert its shape.
    """
    import types

    seen = {}
    errors = errors or {}

    class _Raw:
        def mget(self, body=None, _source=None, **_kw):
            if raise_exc:
                raise raise_exc
            seen["homes"] = [d["_index"] for d in body["docs"]]
            seen["_source"] = _source
            out = []
            for d in body["docs"]:
                idx, _id = d["_index"], d["_id"]
                if idx in errors:
                    out.append({"_index": idx, "_id": _id,
                                "error": {"type": errors[idx]}})
                elif idx in docs_by_index:
                    hit = dict(docs_by_index[idx])
                    hit["_index"] = idx
                    out.append(hit)
                else:
                    out.append({"_index": idx, "_id": _id, "found": False})
            return {"docs": out}

    class _ES:
        es = _Raw()

    monkeypatch.setitem(
        sys.modules, "hysds.es_util",
        types.SimpleNamespace(get_mozart_es=lambda: _ES()),
    )
    return seen


def _hit(uuid, retry_count=None):
    src = {"uuid": uuid}
    if retry_count is not None:
        src["job"] = {"retry_count": retry_count}
    return {"found": True, "_source": src}


def _daily(days_ago):
    from datetime import datetime, timedelta, timezone
    d = datetime.now(timezone.utc).date() - timedelta(days=days_ago)
    return f"job_status-{d.strftime('%Y.%m.%d')}"


def test_same_uuid_is_owned(monkeypatch):
    _mget_stub(monkeypatch, {"job_failed": _hit("uuid-1")})
    assert lu.job_supersession("payload-1", "uuid-1") == lu.OWNED
    assert lu.is_job_superseded("payload-1", "uuid-1") is False


def test_a_later_attempt_supersedes(monkeypatch):
    """A retry keeps the payload_id, mints a new uuid AND bumps retry_count."""
    _mget_stub(monkeypatch, {"job_failed": _hit("uuid-2", retry_count=1)})
    assert lu.job_supersession("payload-1", "uuid-1", retry_count=0) == lu.SUPERSEDED
    assert lu.is_job_superseded("payload-1", "uuid-1", retry_count=0) is True


def test_an_older_leftover_does_not_supersede(monkeypatch):
    """Regression: a different uuid alone must NOT count. An orphaned
    job_failed doc or an unswept job-revoked doc from an EARLIER attempt also
    carries a uuid that is not ours; treating that as supersession made a
    supervisory writer drop a legitimate failure on a live cluster."""
    _mget_stub(monkeypatch, {"job_failed": _hit("uuid-0", retry_count=0)})
    assert lu.job_supersession("payload-1", "uuid-1", retry_count=1) == lu.OWNED


def test_equal_or_missing_retry_counts_tie_and_do_not_supersede(monkeypatch):
    _mget_stub(monkeypatch, {"job_failed": _hit("uuid-2", retry_count=2)})
    assert lu.job_supersession("payload-1", "uuid-1", retry_count=2) == lu.OWNED
    _mget_stub(monkeypatch, {"job_failed": _hit("uuid-2")})
    assert lu.job_supersession("payload-1", "uuid-1") == lu.OWNED


def test_no_live_doc_anywhere_is_absent(monkeypatch):
    """The signature of a retry having just deleted the doc. Folding it in
    with 'not superseded' let a writer resurrect the old attempt and, via
    logstash's paired delete, destroy the retried attempt's fresh doc."""
    _mget_stub(monkeypatch, {})
    assert lu.job_supersession("payload-1", "uuid-1") == lu.ABSENT
    assert lu.is_job_superseded("payload-1", "uuid-1") is False


def test_could_not_ask_is_unknown_not_absent(monkeypatch):
    """A closed home plus nothing found is not evidence of deletion. Reading
    it as ABSENT made the guard fail closed during a rolling node restart and
    drop every failure record in flight."""
    _mget_stub(monkeypatch, {}, errors={"job_failed": "index_closed_exception"})
    assert lu.job_supersession("payload-1", "uuid-1") == lu.UNKNOWN


def test_probe_raising_is_unknown(monkeypatch):
    _mget_stub(monkeypatch, {}, raise_exc=ConnectionError("transport blip"))
    assert lu.job_supersession("payload-1", "uuid-1") == lu.UNKNOWN


def test_a_daily_that_never_existed_is_a_clean_miss(monkeypatch):
    """Days with no jobs have no index. That is not a failed probe."""
    _mget_stub(monkeypatch, {}, errors={_daily(1): "index_not_found_exception"})
    assert lu.job_supersession("payload-1", "uuid-1", index=_daily(2)) == lu.ABSENT


def test_a_found_doc_wins_over_an_errored_home(monkeypatch):
    _mget_stub(monkeypatch, {"job_failed": _hit("uuid-2", retry_count=1)},
               errors={_daily(0): "index_closed_exception"})
    assert lu.job_supersession("payload-1", "uuid-1", retry_count=0) == lu.SUPERSEDED


def test_day_crossing_retry_is_found_in_a_middle_daily(monkeypatch):
    """Submitted on D-3, retried on D-1: the caller holds D-3 and the live
    doc is in D-1. Probing only the caller's index and today missed it."""
    seen = _mget_stub(monkeypatch, {_daily(1): _hit("uuid-2", retry_count=1)})
    assert lu.job_supersession(
        "payload-1", "uuid-1", retry_count=0, index=_daily(3)) == lu.SUPERSEDED
    assert _daily(1) in seen["homes"]


def test_own_leftover_does_not_hide_a_newer_attempt(monkeypatch):
    """The false OWNED: our own leftover sits in the caller's daily AND a newer
    attempt sits in a later one. Stopping at the first found doc authorised
    the watchdog to manufacture the very orphan the reaper exists to reap."""
    _mget_stub(monkeypatch, {
        _daily(3): _hit("uuid-1", retry_count=0),          # ours, old
        _daily(1): _hit("uuid-2", retry_count=1),          # newer
    })
    assert lu.job_supersession(
        "payload-1", "uuid-1", retry_count=0, index=_daily(3)) == lu.SUPERSEDED


def test_probe_is_one_mget_over_every_home_with_a_projection(monkeypatch):
    seen = _mget_stub(monkeypatch, {"job_failed": _hit("uuid-1")})
    lu.job_supersession("payload-1", "uuid-1", index=_daily(2))
    assert seen["homes"][0] == "job_failed"
    assert seen["homes"][1:] == [_daily(2), _daily(1), _daily(0)]
    assert seen["_source"] == ["uuid", "job.retry_count"]


def test_job_status_homes_shape():
    from datetime import timedelta
    from datetime import date
    today = date(2026, 9, 2)
    assert lu.job_status_homes("job_status-2026.08.31", today=today) == [
        "job_failed", "job_status-2026.08.31", "job_status-2026.09.01",
        "job_status-2026.09.02"]
    # a non-daily index is asked as given, then today
    assert lu.job_status_homes("job_status-current", today=today) == [
        "job_failed", "job_status-current", "job_status-2026.09.02"]
    # job_failed is never listed twice; no index means just today
    assert lu.job_status_homes("job_failed", today=today) == [
        "job_failed", "job_status-2026.09.02"]
    assert lu.job_status_homes(None, today=today) == [
        "job_failed", "job_status-2026.09.02"]
    # a future or ancient caller date clamps
    assert lu.job_status_homes("job_status-2027.01.01", today=today)[-1] == "job_status-2026.09.02"
    old = lu.job_status_homes("job_status-2020.01.01", today=today)
    # the window is clamped, but the caller's own daily is still asked
    assert len(old) == lu.MAX_DAILY_HOMES + 3
    assert old[1] == "job_status-2020.01.01"
    assert old[2] == f"job_status-{(today - timedelta(days=lu.MAX_DAILY_HOMES)).strftime('%Y.%m.%d')}"
    # a future-dated daily (clock skew between hosts) is asked too, not dropped
    future = lu.job_status_homes("job_status-2030.01.01", today=today)
    assert future[:2] == ["job_failed", "job_status-2030.01.01"]
    assert future[-1] == f"job_status-{today.strftime('%Y.%m.%d')}"
    assert len(future) == len(set(future))
