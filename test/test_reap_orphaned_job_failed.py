"""The reaper deletes job_failed docs a newer attempt superseded."""
import importlib.util
import json
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


def _failed_hit(_id="payload-1", uuid="uuid-1", retry_count=None, version=3,
                time_end="2026-08-29T10:59:00.000000Z", hostname="worker-a"):
    """An mget hit for the job_failed doc under the same _id.

    job_info.time_end is the failed execution's own end stamp; @timestamp is
    the write time, a minute later here.
    """
    job = {} if retry_count is None else {"retry_count": retry_count}
    job["job_info"] = {"time_end": time_end} if time_end else {}
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
            "celery_hostname": hostname,
            "job": job,
        },
    }


def _redelivered(_id="payload-1", uuid="uuid-1", status="job-completed",
                 time_start="2026-08-29T11:05:00.000000Z", hostname="worker-b",
                 retry_count=None):
    """A redelivered execution's doc in a dated index -- the second scan's
    starting point. Its uuid is the failed doc's own; what makes it a later
    execution is its time_start, stamped when the re-run began."""
    job = {
        "delivery_info": {"redelivered": True},
        "job_info": {"time_start": time_start} if time_start else {},
    }
    if retry_count is not None:
        job["retry_count"] = retry_count
    return {
        "_id": _id,
        "_index": "job_status-2026.08.29",
        "_source": {
            "payload_id": _id,
            "uuid": uuid,
            "status": status,
            "@timestamp": "2026-08-29T12:00:00.000Z",
            "celery_hostname": hostname,
            "job": job,
        },
    }


def _route(candidates, redelivered):
    """A query stub that answers each scan with its own list."""
    def query(index=None, body=None, **kwargs):
        return list(redelivered) if "redelivered" in json.dumps(body) else candidates
    return query


def _wire(monkeypatch, candidates, failed_docs, delete_result=None, redis_status=None,
          redelivered=()):
    """Wire the reaper's collaborators; returns the mock mozart client.

    `candidates` answers the retried-attempt scan, `redelivered` the
    redelivered-execution scan; the mget answers both with `failed_docs`.
    """
    es = umock.MagicMock()
    es.query.side_effect = _route(candidates, redelivered)
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
    """One uuid in both homes is not the retried-attempt scan's to judge:
    mid-move it is one execution, after a redelivery it is two. The
    redelivered scan (below) tells those apart; this scan steps aside."""
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
    es.query.side_effect = _route(candidates, [])
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


def test_a_window_longer_than_the_redis_ttl_is_refused_when_deleting(monkeypatch):
    """Past the TTL a missing redis key is indistinguishable from an expired
    one, so the cross-check is inert. Reporting over a long window is the
    documented historical audit; deleting over one is refused."""
    monkeypatch.setattr(reaper.app.conf, "get", lambda *a, **k: 86400, raising=False)
    with pytest.raises(SystemExit, match="redis job-status TTL"):
        reaper.check_window_against_redis_ttl(
            lookback_days=63, since=None, deleting=True, allow_expired=False
        )
    reaper.check_window_against_redis_ttl(63, None, False, False)   # reporting only
    reaper.check_window_against_redis_ttl(63, None, True, True)     # acknowledged
    reaper.check_window_against_redis_ttl(0.5, None, True, False)   # short window


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


def test_cli_defaults_are_the_shipped_block_and_do_not_delete():
    """Deleting is an addition, never a preservation: a block copied into a
    PCM override that loses a flag can only get quieter."""
    args = reaper.build_parser().parse_args([])
    assert (args.interval, args.grace_secs, args.lookback_days) == (300, 120, 1)
    assert args.delete_orphans is False
    assert args.once is False and args.since is None
    assert reaper.build_parser().parse_args(["--delete-orphans"]).delete_orphans is True


# --------------------------------------------------------------------------
# celery redelivery: one uuid executed twice
# --------------------------------------------------------------------------

def test_a_redelivered_later_execution_is_reaped(monkeypatch):
    """The re-run started after the failed execution ended, and redis --
    one key for both executions, overwritten by the later one -- reads
    job-completed."""
    es = _wire(monkeypatch, [], [_failed_hit()], redis_status="job-completed",
               redelivered=[_redelivered()])

    counters = reaper.reap_orphans()

    assert counters["reaped"] == 1
    assert counters["redelivered_scanned"] == 1
    assert counters["by_mechanism"] == {"redelivered_after_terminal": 1}
    es.delete_by_id.assert_called_once_with(
        index="job_failed",
        id="payload-1",
        if_seq_no=42,
        if_primary_term=7,
        ignore=[404, 409],
    )
    args, kwargs = reaper.log_custom_event.call_args
    event = args[2]
    assert event["orphan_uuid"] == event["newer_uuid"] == "uuid-1"
    assert event["mechanism"] == "redelivered_after_terminal"
    assert event["mechanism_basis"] == "time_start"
    assert (event["orphan_host"], event["newer_host"]) == ("worker-a", "worker-b")
    assert event["orphan_time_end"] == "2026-08-29T10:59:00.000000Z"
    assert event["newer_time_start"] == "2026-08-29T11:05:00.000000Z"
    assert "mechanism:redelivered_after_terminal" in kwargs["tags"]
    assert "basis:time_start" in kwargs["tags"]
    assert "reaped" in kwargs["tags"]


def test_the_redelivered_scan_asks_for_finished_executions_only(monkeypatch):
    """The flag run_job stores from the broker, mapped boolean by the
    job_status template, is the whole candidate filter."""
    es = _wire(monkeypatch, [], [])

    reaper.reap_orphans(grace_secs=120, lookback_days=2)

    kwargs = es.query.call_args_list[1].kwargs
    assert kwargs["index"] == "job_status-2*"
    flt = kwargs["body"]["query"]["bool"]["filter"]
    assert {"term": {"job.delivery_info.redelivered": True}} in flt
    assert {"term": {"status": "job-completed"}} in flt
    assert {"range": {"@timestamp": {"gte": "now-2d", "lte": "now-120s"}}} in flt


def test_the_redelivered_scan_requires_the_same_uuid(monkeypatch):
    """A different uuid under the payload is a retry.py resubmission; the
    retried-attempt scan judges those by retry_count."""
    es = _wire(monkeypatch, [], [_failed_hit(uuid="uuid-1")],
               redelivered=[_redelivered(uuid="uuid-2")])

    counters = reaper.reap_orphans()

    assert counters["redelivered_skipped_different_uuid"] == 1
    es.delete_by_id.assert_not_called()


def test_the_same_execution_in_both_homes_is_not_reaped(monkeypatch):
    """Mid-move, or a job-offline a supervisory writer put over the same
    execution: the dated doc's time_start is the failed execution's own,
    so it is not later than that execution's end."""
    es = _wire(monkeypatch, [], [_failed_hit(time_end="2026-08-29T10:59:00Z")],
               redelivered=[_redelivered(time_start="2026-08-29T10:30:00Z")])

    counters = reaper.reap_orphans()

    assert counters["redelivered_skipped_not_later"] == 1
    es.delete_by_id.assert_not_called()


def test_a_doc_without_a_start_stamp_is_left_alone(monkeypatch):
    """The locking branch writes job-deduped before time_start is set."""
    es = _wire(monkeypatch, [], [_failed_hit()],
               redelivered=[_redelivered(time_start=None)])

    counters = reaper.reap_orphans()

    assert counters["redelivered_skipped_unclassified"] == 1
    es.delete_by_id.assert_not_called()


@pytest.mark.parametrize("redis_status", ["job-started", "job-failed", "job-offline"])
def test_a_shared_key_that_is_not_job_completed_blocks_the_reap(
    monkeypatch, redis_status
):
    """job-started means a further execution is under way; job-failed means
    one already failed and its own doc has replaced the one being judged."""
    es = _wire(monkeypatch, [], [_failed_hit()], redis_status=redis_status,
               redelivered=[_redelivered()])

    counters = reaper.reap_orphans()

    assert counters["redelivered_skipped_redis"] == 1
    es.delete_by_id.assert_not_called()


def test_an_expired_shared_key_still_allows_the_reap(monkeypatch):
    _wire(monkeypatch, [], [_failed_hit()], redis_status=None,
          redelivered=[_redelivered()])

    counters = reaper.reap_orphans()

    assert counters["reaped"] == 1
    assert counters["redis_expired"] == 1


def test_a_redelivered_dry_run_reports_without_deleting(monkeypatch):
    es = _wire(monkeypatch, [], [_failed_hit()], redis_status="job-completed",
               redelivered=[_redelivered()])

    counters = reaper.reap_orphans(dry_run=True)

    assert counters["would_reap"] == 1
    assert counters["reaped"] == 0
    es.delete_by_id.assert_not_called()
    assert "dry_run" in reaper.log_custom_event.call_args.kwargs["tags"]


def test_a_retried_attempt_that_was_then_redelivered_is_reaped(monkeypatch):
    """The retried attempt carries retry_count, so the retried-attempt scan
    sees the pair first and steps aside (same uuid); the redelivered scan
    then judges it by the stamps."""
    es = _wire(monkeypatch,
               [_candidate(uuid="uuid-1", retry_count=1)],
               [_failed_hit(uuid="uuid-1", retry_count=1)],
               redis_status="job-completed",
               redelivered=[_redelivered(uuid="uuid-1", retry_count=1)])

    counters = reaper.reap_orphans()

    assert counters["skipped_same_uuid"] == 1
    assert counters["reaped"] == 1
    assert es.delete_by_id.call_count == 1


def test_both_scans_run_in_one_sweep(monkeypatch):
    """A retry orphan and a redelivery orphan in one sweep are each judged
    by their own scan and both counted."""
    es = _wire(
        monkeypatch,
        [_candidate(_id="r", uuid="new-r")],
        [_failed_hit(_id="r", uuid="old-r"), _failed_hit(_id="d", uuid="same-d")],
        redis_status=None,
        redelivered=[_redelivered(_id="d", uuid="same-d")],
    )

    counters = reaper.reap_orphans()

    assert counters["scanned"] == 1
    assert counters["redelivered_scanned"] == 1
    assert counters["reaped"] == 2
    assert set(counters["by_mechanism"]) == {
        "written_before_retry_queued", "redelivered_after_terminal"
    }
    assert {c.kwargs["id"] for c in es.delete_by_id.call_args_list} == {"r", "d"}


def test_classify_redelivered_by_start_and_end_stamps():
    orphan = _failed_hit(time_end="2026-08-29T11:00:00Z")
    later = _redelivered(time_start="2026-08-29T11:00:01Z")["_source"]
    same = _redelivered(time_start="2026-08-29T10:00:00Z")["_source"]
    assert reaper.classify_redelivered(orphan, later) == (
        "redelivered_after_terminal", "time_start"
    )
    assert reaper.classify_redelivered(orphan, same) == (
        "same_or_earlier_execution", "time_start"
    )


def test_classify_redelivered_falls_back_to_the_write_time():
    """No time_end on the failed doc: its @timestamp is stamped at write
    time, so it is never earlier than the end it stands in for."""
    orphan = _failed_hit(time_end=None)                      # @timestamp 11:00
    later = _redelivered(time_start="2026-08-29T11:30:00Z")["_source"]
    earlier = _redelivered(time_start="2026-08-29T10:59:00Z")["_source"]
    assert reaper.classify_redelivered(orphan, later)[0] == "redelivered_after_terminal"
    assert (
        reaper.classify_redelivered(orphan, earlier)[0] == "same_or_earlier_execution"
    )


def test_classify_redelivered_with_nothing_to_compare_is_unknown():
    assert reaper.classify_redelivered({"_source": {}}, _redelivered()["_source"]) == (
        "unknown", "none"
    )
    assert reaper.classify_redelivered(
        _failed_hit(), _redelivered(time_start=None)["_source"]
    ) == ("unknown", "none")


def test_classify_redelivered_reads_a_naive_stamp_as_utc():
    orphan = _failed_hit(time_end="2026-08-29T11:00:00")     # no zone designator
    later = _redelivered(time_start="2026-08-29T11:00:01Z")["_source"]
    assert reaper.classify_redelivered(orphan, later)[0] == "redelivered_after_terminal"


def test_a_failed_doc_that_is_the_later_execution_is_never_deleted(monkeypatch):
    """The gate refuses rather than tags. A failed doc that postdates the
    completed doc can be the last trace of a completed job whose re-run then
    failed: in that variant logstash's paired delete has already removed the
    completed doc, so deleting the failed one would erase the victim. (Here a
    completed doc is still present, which is what makes the case reachable
    at all; the verdict must not depend on that.)"""
    es = _wire(monkeypatch, [], [_failed_hit(time_end="2026-08-29T11:40:00Z")],
               redis_status="job-completed",
               redelivered=[_redelivered(time_start="2026-08-29T11:05:00Z")])

    counters = reaper.reap_orphans()

    assert counters["redelivered_skipped_not_later"] == 1
    assert counters["reaped"] == 0
    es.delete_by_id.assert_not_called()
    reaper.log_custom_event.assert_not_called()


def test_classify_redelivered_equal_stamps_is_not_later():
    """The gate is strictly later: a re-run stamped at the same instant the
    failed execution ended is not provably a different execution, so it is
    never reaped. Clock skew between workers can only turn a real re-run into
    this case, which errs toward keeping the failed doc."""
    orphan = _failed_hit(time_end="2026-08-29T11:00:00Z")
    same = _redelivered(time_start="2026-08-29T11:00:00Z")["_source"]
    assert reaper.classify_redelivered(orphan, same) == (
        "same_or_earlier_execution", "time_start"
    )
