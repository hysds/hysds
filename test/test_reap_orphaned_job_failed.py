"""The reaper deletes job_failed docs a newer attempt superseded."""
import importlib.util
import pathlib
import sys
import unittest.mock as umock

import pytest

# hysds.celery reads configuration on import; hysds.es_util builds a mozart
# client at get_mozart_es() time. Mock both before loading the script, then
# restore es_util so the stub does not leak into later test files (the script
# keeps its own reference, which is what the tests drive). scripts/ is not a
# package and the script imports `future`, so stub that too.
_saved_es_util = sys.modules.get("hysds.es_util")
sys.modules.setdefault("hysds.celery", umock.MagicMock())
sys.modules.setdefault("future", umock.MagicMock())
sys.modules["hysds.es_util"] = umock.MagicMock()

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
_spec = importlib.util.spec_from_file_location(
    "reap_orphaned_job_failed", SCRIPTS / "reap_orphaned_job_failed.py"
)
reaper = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reaper)

if _saved_es_util is not None:
    sys.modules["hysds.es_util"] = _saved_es_util
else:
    sys.modules.pop("hysds.es_util", None)


def _candidate(_id="payload-1", uuid="uuid-2", retry_count=1, status="job-completed",
               time_queued="2026-08-29T11:30:00.000000Z", retry_delete=None):
    """A retried attempt's doc in a dated index -- the scan's starting point.

    job_info.time_queued is stamped by retry.py immediately before it deletes
    the old status doc, so it marks the moment of the retry.
    """
    return {
        "_id": _id,
        "_index": "job_status-2026.08.29",
        "_source": {
            "payload_id": _id,
            "uuid": uuid,
            "status": status,
            "@timestamp": "2026-08-29T12:00:00.000Z",
            "job": {"retry_count": retry_count,
                    "job_info": {"time_queued": time_queued,
                                 **({"retry_delete": retry_delete} if retry_delete else {})}},
        },
    }


def _failed_hit(_id="payload-1", uuid="uuid-1", retry_count=None, version=3):
    """An mget hit for the job_failed doc under the same _id."""
    job = {} if retry_count is None else {"retry_count": retry_count}
    return {
        "_id": _id,
        "found": True,
        "_version": version,
        "_seq_no": 42,
        "_primary_term": 7,
        "_source": {
            "uuid": uuid,
            "status": "job-failed",
            "@timestamp": "2026-08-29T11:00:00.000Z",
            "job": job,
        },
    }


def _wire(monkeypatch, candidates, failed_docs, delete_result=None, redis_status=None):
    """Wire the reaper's collaborators; returns the mock mozart client."""
    es = umock.MagicMock()
    es.query.return_value = candidates
    es.es.mget.return_value = {"docs": failed_docs}
    es.delete_by_id.return_value = (
        delete_result if delete_result is not None else {"result": "deleted"}
    )
    monkeypatch.setattr(reaper.es_util, "get_mozart_es", lambda: es)
    monkeypatch.setattr(reaper, "get_job_status", lambda uuid: redis_status)
    monkeypatch.setattr(reaper, "log_custom_event", umock.MagicMock())
    return es


def test_superseded_orphan_is_reaped(monkeypatch):
    es = _wire(monkeypatch, [_candidate()], [_failed_hit()])

    counters = reaper.reap_orphans()

    assert counters["reaped"] == 1
    es.delete_by_id.assert_called_once_with(
        index="job_failed",
        id="payload-1",
        if_seq_no=42,
        if_primary_term=7,
        ignore=[404, 409],
    )


def test_reap_records_the_version_that_classifies_the_orphan(monkeypatch):
    """_version is the post-release triage signal: 2 = delete-side stale read
    (the delete-side stale read is back), 3 or 1 = write-after-delete."""
    _wire(monkeypatch, [_candidate()], [_failed_hit(version=2)])

    counters = reaper.reap_orphans()

    assert counters["reaped_by_version"] == {2: 1}
    event = reaper.log_custom_event.call_args[0]
    assert event[0] == "worker_anomaly"
    assert event[1] == "job_failed_orphan_reaped"
    assert event[2]["version"] == 2
    assert event[2]["orphan_uuid"] == "uuid-1"
    assert event[2]["newer_uuid"] == "uuid-2"


def test_no_failed_doc_is_left_alone(monkeypatch):
    """Most retried attempts have no job_failed doc at all."""
    es = _wire(monkeypatch, [_candidate()], [{"_id": "payload-1", "found": False}])

    counters = reaper.reap_orphans()

    assert counters["reaped"] == 0
    es.delete_by_id.assert_not_called()


def test_same_uuid_sibling_is_not_an_orphan(monkeypatch):
    """The same attempt visible in both homes mid-move."""
    es = _wire(monkeypatch, [_candidate(uuid="uuid-1")], [_failed_hit(uuid="uuid-1")])

    counters = reaper.reap_orphans()

    assert counters["skipped_same_uuid"] == 1
    es.delete_by_id.assert_not_called()


def test_terminal_failure_is_never_reaped(monkeypatch):
    """Retry budget exhausted: the failed doc IS the later attempt.

    Its retry_count is >= the dated leftover's, so the comparison -- not a
    timestamp -- keeps the real failure visible.
    """
    es = _wire(
        monkeypatch,
        [_candidate(retry_count=1, status="job-revoked")],
        [_failed_hit(retry_count=3)],
    )

    counters = reaper.reap_orphans()

    assert counters["skipped_not_superseded"] == 1
    es.delete_by_id.assert_not_called()


def test_equal_retry_counts_are_not_superseded(monkeypatch):
    es = _wire(monkeypatch, [_candidate(retry_count=2)], [_failed_hit(retry_count=2)])

    reaper.reap_orphans()

    es.delete_by_id.assert_not_called()


def test_missing_retry_count_on_the_failed_doc_counts_as_zero(monkeypatch):
    """The original attempt carries no retry_count key."""
    es = _wire(
        monkeypatch, [_candidate(retry_count=1)], [_failed_hit(retry_count=None)]
    )

    counters = reaper.reap_orphans()

    assert counters["reaped"] == 1
    es.delete_by_id.assert_called_once()


def test_live_orphan_uuid_in_redis_is_skipped(monkeypatch):
    """Redis is authoritative while ES is in flight."""
    es = _wire(monkeypatch, [_candidate()], [_failed_hit()], redis_status="job-started")

    counters = reaper.reap_orphans()

    assert counters["skipped_redis"] == 1
    es.delete_by_id.assert_not_called()


@pytest.mark.parametrize("redis_status", [None, "job-failed"])
def test_terminal_or_absent_redis_status_allows_the_reap(monkeypatch, redis_status):
    _wire(monkeypatch, [_candidate()], [_failed_hit()], redis_status=redis_status)

    assert reaper.reap_orphans()["reaped"] == 1


def test_conflict_is_counted_not_raised(monkeypatch):
    """The retried attempt failed and replaced the doc between mget and delete."""
    _wire(
        monkeypatch,
        [_candidate()],
        [_failed_hit()],
        delete_result={"status": 409, "error": {"type": "version_conflict"}},
    )

    counters = reaper.reap_orphans()

    assert counters["skipped_conflict"] == 1
    assert counters["reaped"] == 0
    reaper.log_custom_event.assert_not_called()


def test_dry_run_deletes_nothing(monkeypatch):
    es = _wire(monkeypatch, [_candidate()], [_failed_hit()])

    counters = reaper.reap_orphans(dry_run=True)

    assert counters["would_reap"] == 1
    assert counters["reaped"] == 0
    es.delete_by_id.assert_not_called()
    # emitted anyway, flagged: the audit both PR bodies prescribe reads these
    # events, and a dry-run that produced none could not be reconciled
    event = reaper.log_custom_event.call_args[0][2]
    assert event["dry_run"] is True
    assert "dry_run" in reaper.log_custom_event.call_args.kwargs["tags"]


def test_candidate_query_scans_dated_indices_only(monkeypatch):
    """job_status-* would expand the job_status-current alias and drag
    job_failed itself into the scan."""
    es = _wire(monkeypatch, [], [])

    reaper.reap_orphans(grace_secs=120, lookback_days=2)

    kwargs = es.query.call_args_list[0].kwargs   # [0] = candidates, later = unpaired count
    assert kwargs["index"] == "job_status-2*"
    must = kwargs["body"]["query"]["bool"]["must"]
    assert {"range": {"job.retry_count": {"gte": 1}}} in must
    assert {"range": {"@timestamp": {"gte": "now-2d", "lte": "now-120s"}}} in must


def test_since_overrides_the_lookback_window(monkeypatch):
    es = _wire(monkeypatch, [], [])

    reaper.reap_orphans(grace_secs=60, lookback_days=2, since="2026-07-01")

    must = es.query.call_args_list[0].kwargs["body"]["query"]["bool"]["must"]
    assert {"range": {"@timestamp": {"gte": "2026-07-01", "lte": "now-60s"}}} in must


def test_candidates_are_paged_through_mget_in_order(monkeypatch):
    """One realtime mget per page, results zipped positionally."""
    total = reaper.PAGE_SIZE + 3
    candidates = [_candidate(_id=f"payload-{i}") for i in range(total)]
    hits = [_failed_hit(_id=f"payload-{i}") for i in range(total)]

    es = umock.MagicMock()
    es.query.return_value = candidates
    es.es.mget.side_effect = [
        {"docs": hits[: reaper.PAGE_SIZE]},
        {"docs": hits[reaper.PAGE_SIZE :]},
    ]
    es.delete_by_id.return_value = {"result": "deleted"}
    monkeypatch.setattr(reaper.es_util, "get_mozart_es", lambda: es)
    monkeypatch.setattr(reaper, "get_job_status", lambda uuid: None)
    monkeypatch.setattr(reaper, "log_custom_event", umock.MagicMock())

    counters = reaper.reap_orphans()

    assert es.es.mget.call_count == 2
    first_ids = es.es.mget.call_args_list[0].kwargs["body"]["ids"]
    assert len(first_ids) == reaper.PAGE_SIZE
    assert first_ids[0] == "payload-0"
    assert counters["scanned"] == total
    assert counters["reaped"] == total


def test_sweep_survives_a_partial_page(monkeypatch):
    """A mix of orphans and non-orphans in one page: each judged on its own."""
    candidates = [
        _candidate(_id="a", uuid="new-a", retry_count=1),
        _candidate(_id="b", uuid="same-b", retry_count=1),
        _candidate(_id="c", uuid="new-c", retry_count=1),
    ]
    hits = [
        _failed_hit(_id="a", uuid="old-a"),
        _failed_hit(_id="b", uuid="same-b"),
        {"_id": "c", "found": False},
    ]
    es = _wire(monkeypatch, candidates, hits)

    counters = reaper.reap_orphans()

    assert counters["reaped"] == 1
    assert counters["skipped_same_uuid"] == 1
    assert es.delete_by_id.call_args.kwargs["id"] == "a"


# --------------------------------------------------------------------------
# which mechanism left the orphan behind
# --------------------------------------------------------------------------

def test_an_expired_redis_key_is_counted_separately(monkeypatch):
    """A missing key counts as reapable, so the cross-check did not happen.
    HYSDS_JOB_STATUS_EXPIRES is a day, so any longer window runs mostly
    unverified and an operator should be able to see how much."""
    _wire(monkeypatch, [_candidate()], [_failed_hit()], redis_status=None)

    counters = reaper.reap_orphans()

    assert counters["reaped"] == 1
    assert counters["redis_expired"] == 1


def test_a_live_redis_key_is_not_counted_as_expired(monkeypatch):
    _wire(monkeypatch, [_candidate()], [_failed_hit()], redis_status="job-failed")

    counters = reaper.reap_orphans()

    assert counters["reaped"] == 1
    assert counters.get("redis_expired", 0) == 0


def test_a_window_longer_than_the_redis_ttl_is_refused(monkeypatch):
    import pytest as _pytest

    monkeypatch.setattr(reaper.app.conf, "get", lambda *a, **k: 86400, raising=False)
    with _pytest.raises(SystemExit, match="redis job-status TTL"):
        reaper.check_window_against_redis_ttl(
            lookback_days=63, since=None, dry_run=False, allow_expired=False
        )
    # allowed explicitly, and allowed for a dry run
    reaper.check_window_against_redis_ttl(63, None, True, False)
    reaper.check_window_against_redis_ttl(63, None, False, True)
    # and a short window is fine
    reaper.check_window_against_redis_ttl(0.5, None, False, False)


# --------------------------------------------------------------------------
# which side of the retry's delete the orphan was INDEXED on
# --------------------------------------------------------------------------

MARK = [{"index": "job_status-2026.08.29", "seq_no": 7, "primary_term": 1},
        {"index": "job_failed", "seq_no": 100, "primary_term": 3}]


def test_indexed_after_the_delete_by_seq_no():
    hit = {"_seq_no": 101, "_primary_term": 3, "_source": {"@timestamp": "2026-08-29T11:00:00Z"}}
    cand = _candidate(retry_delete=MARK)["_source"]
    assert reaper.classify(hit, cand) == ("indexed_after_retry_delete", "seq_no")


def test_indexed_before_the_delete_by_seq_no():
    hit = {"_seq_no": 99, "_primary_term": 3, "_source": {}}
    cand = _candidate(retry_delete=MARK)["_source"]
    assert reaper.classify(hit, cand) == ("indexed_before_retry_delete", "seq_no")


def test_primary_term_outranks_seq_no():
    """A new primary restarts nothing but a later term always sorts later."""
    hit = {"_seq_no": 5, "_primary_term": 4, "_source": {}}
    cand = _candidate(retry_delete=MARK)["_source"]
    assert reaper.classify(hit, cand) == ("indexed_after_retry_delete", "seq_no")


def test_a_mark_list_without_job_failed_is_no_mark():
    hit = {"_seq_no": 101, "_primary_term": 3,
           "_source": {"@timestamp": "2026-08-29T12:00:00Z"}}
    cand = _candidate(retry_delete=[MARK[0]])["_source"]
    assert reaper.classify(hit, cand)[1] == "timestamp"


def test_without_a_mark_only_write_order_is_reported():
    """No mark: the resubmitting job predates the field, or the sweep never
    reached job_failed. Timestamps answer WRITE order, not indexing order,
    and the gap between those is the whole write-after-delete mechanism, so
    the value says what it can establish and the basis says it is a guess."""
    early = {"_seq_no": 1, "_primary_term": 1, "_source": {"@timestamp": "2026-08-29T11:00:00.000Z"}}
    late = {"_seq_no": 1, "_primary_term": 1, "_source": {"@timestamp": "2026-08-29T11:45:00.000Z"}}
    cand = _candidate()["_source"]                  # retry queued 11:30, no mark
    assert reaper.classify(early, cand) == ("written_before_retry_queued", "timestamp")
    assert reaper.classify(late, cand) == ("written_after_retry_queued", "timestamp")


def test_nothing_to_compare_is_unknown():
    cand = _candidate(time_queued=None)["_source"]
    assert reaper.classify({"_source": {}}, cand) == ("unknown", "none")


def test_the_reaped_event_carries_mechanism_basis_and_tags(monkeypatch):
    _wire(monkeypatch, [_candidate(retry_delete=MARK)],
          [dict(_failed_hit(), _seq_no=101, _primary_term=3)])

    counters = reaper.reap_orphans()

    assert counters["by_mechanism"] == {"indexed_after_retry_delete": 1}
    args, kwargs = reaper.log_custom_event.call_args
    assert args[2]["mechanism"] == "indexed_after_retry_delete"
    assert args[2]["mechanism_basis"] == "seq_no"
    assert args[2]["retry_delete_mark"] == MARK[1]
    assert "mechanism:indexed_after_retry_delete" in kwargs["tags"]
    assert "reaped" in kwargs["tags"]


def test_mget_results_are_paired_by_id_not_position(monkeypatch):
    """A judgement that deletes a document must follow the _id."""
    cands = [_candidate(_id="a", uuid="new-a"), _candidate(_id="b", uuid="same-b")]
    hits = [_failed_hit(_id="b", uuid="same-b"), _failed_hit(_id="a", uuid="old-a")]  # scrambled
    es = _wire(monkeypatch, cands, hits)

    counters = reaper.reap_orphans()

    assert counters["reaped"] == 1
    assert es.delete_by_id.call_args.kwargs["id"] == "a"


def test_the_shipped_supervisord_defaults_pass_the_ttl_guard(monkeypatch):
    """--lookback-days 1 with the shipped TTL of one day, and no --dry-run:
    86400 is not greater than 86400. The earlier block shipped 2 days, which
    made the documented rollout step (drop --dry-run) exit before the first
    sweep and go FATAL under supervisord."""
    monkeypatch.setattr(reaper.app.conf, "get", lambda *a, **k: 86400, raising=False)
    reaper.check_window_against_redis_ttl(1, None, False, False)   # no SystemExit


def test_once_reports_a_failed_sweep(monkeypatch):
    monkeypatch.setattr(reaper.app.conf, "get", lambda *a, **k: 86400, raising=False)
    monkeypatch.setattr(reaper, "reap_orphans", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    assert reaper.daemon(300, 120, 1, once=True) is False
    monkeypatch.setattr(reaper, "reap_orphans", lambda *a, **k: {"scanned": 0})
    assert reaper.daemon(300, 120, 1, once=True) is True


def test_cli_defaults_match_the_shipped_supervisord_block():
    """A bare hand run must pass the TTL guard the shipped block passes; the
    defaults are what the block passes explicitly."""
    args = reaper.build_parser().parse_args([])
    assert (args.interval, args.grace_secs, args.lookback_days) == (300, 120, 1)
    assert args.dry_run is False and args.once is False and args.since is None
