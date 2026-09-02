#!/usr/bin/env python
"""Reap job_failed docs orphaned by the retry flow.

When a failed job is retried, lightweight-jobs retry.py deletes its status
doc before resubmitting. A job-failed write that is still in flight -- a
duplicate terminal write, a supervisory writer, a message the async
redis -> logstash -> OpenSearch pipeline had not delivered yet -- can land
after that delete and re-create the doc. The result is a job_failed doc for
an attempt that has already been superseded, sitting in the job_status-current
alias next to the retried attempt's own doc, so operators read the job as
"never retried".

This sweeper deletes job_failed docs that a newer attempt has superseded. It
is the repair-side complement to the retry job's delete fix and the write-side
guards in hysds (is_job_finalized, is_job_superseded), and its per-sweep
counters are the standing health signal for late writes.

Scan direction matters. Retried jobs are rare next to failures, so the sweep
starts from the retried attempts in the dated indices and probes job_failed by
id (two requests per 500 candidates, and the probe is a realtime mget so it
cannot be stale). Scanning job_failed instead would re-run one search per
failed doc every interval -- tens of thousands of them on a busy venue.
"""
from future import standard_library

standard_library.install_aliases()
import argparse
import logging
import random
import time
import traceback
from collections import Counter
from datetime import datetime, timezone

import hysds.es_util as es_util
from hysds.celery import app
from hysds.log_utils import get_job_status, log_custom_event

log_format = "[%(asctime)s: %(levelname)s/reap_orphaned_job_failed] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# dated job_status indices ONLY. NOT "job_status-*": an index pattern expands
# aliases too, job_status-current matches it, and that would drag job_failed
# itself into the candidate scan.
CANDIDATE_INDEX = "job_status-2*"
FAILED_INDEX = "job_failed"
PAGE_SIZE = 500

# a redis status other than these means the orphan's own attempt is somehow
# still live, so leave its doc alone
REAPABLE_REDIS_STATUSES = (None, "job-failed")


def _retry_count(source):
    """job.retry_count as an int; the original attempt carries no key."""
    return int((source.get("job") or {}).get("retry_count") or 0)


def _parse_ts(value):
    """ISO-8601 as written by log_job_status and by logstash, or None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def retry_delete_mark(job_info):
    """The job_failed entry of job_info.retry_delete, or {}.

    retry.py records one {index, seq_no, primary_term} entry per alias member
    it swept -- a list rather than a dict keyed by member, so the field set
    stays fixed however many dailies the alias spans.
    """
    for mark in job_info.get("retry_delete") or []:
        if isinstance(mark, dict) and mark.get("index") == FAILED_INDEX:
            return mark
    return {}


def classify(orphan_hit, candidate):
    """Which side of the retry's delete this orphan was INDEXED on.

    Returns (mechanism, basis). The exact answer needs no clock: retry.py
    records each member's delete position as job_info.retry_delete on the
    resubmitted job, and the same _id always routes to the same job_failed
    shard, so the orphan's own (_primary_term, _seq_no) from the mget compares
    directly against the job_failed mark.

      indexed_after_retry_delete   the doc was indexed after the sweep's
                                   delete on job_failed: a late arrival
                                   re-created it. Whether that arrival was
                                   the job's own job-failed message still in
                                   the pipe or a duplicate write is not
                                   distinguished here; both are repaired.
      indexed_before_retry_delete  the doc predates the delete that should
                                   have removed it. Effectively unreachable
                                   once the sweep reached job_failed at all,
                                   since a not_found delete is still
                                   sequenced and stamps a mark.

    With no mark (the resubmitting job predates this field, or the sweep
    could not reach job_failed) it falls back to comparing the orphan's
    @timestamp with the candidate's job_info.time_queued, which retry.py
    stamps before the delete. That answers WRITE order, not indexing order,
    and the gap between those is the entire write-after-delete mechanism, so
    it is reported as such and never as a verdict on the mechanism:

      written_after_retry_queued / written_before_retry_queued

    basis is "seq_no", "timestamp" or "none".
    """
    ji = (candidate.get("job") or {}).get("job_info") or {}
    mark = retry_delete_mark(ji)
    pt, sn = orphan_hit.get("_primary_term"), orphan_hit.get("_seq_no")
    m_pt, m_sn = mark.get("primary_term"), mark.get("seq_no")
    if None not in (pt, sn, m_pt, m_sn):
        after = (int(pt), int(sn)) > (int(m_pt), int(m_sn))
        return ("indexed_after_retry_delete" if after
                else "indexed_before_retry_delete"), "seq_no"
    o_ts = _parse_ts((orphan_hit.get("_source") or {}).get("@timestamp"))
    r_ts = _parse_ts(ji.get("time_queued"))
    if o_ts is None or r_ts is None:
        return "unknown", "none"
    return ("written_before_retry_queued" if o_ts < r_ts
            else "written_after_retry_queued"), "timestamp"


def find_candidates(mozart_es, grace_secs, lookback_days, since=None):
    """Retried attempts old enough to be worth checking.

    The grace period is applied to the candidate, whose @timestamp moves with
    every transition, so a retry that is still writing is never raced; a
    candidate skipped as too young is picked up by a later sweep.
    """
    gte = since if since else f"now-{lookback_days}d"
    query = {
        "query": {
            "bool": {
                "must": [
                    {"range": {"job.retry_count": {"gte": 1}}},
                    {
                        "range": {
                            "@timestamp": {"gte": gte, "lte": f"now-{grace_secs}s"}
                        }
                    },
                ]
            }
        },
        "_source": ["payload_id", "uuid", "status", "@timestamp",
                    "job.retry_count", "job.job_info.time_queued",
                    "job.job_info.retry_delete"],
    }
    return mozart_es.query(index=CANDIDATE_INDEX, body=query)


def reap_orphans(grace_secs=120, lookback_days=2, since=None, dry_run=False):
    """One sweep. Returns the counters dict."""

    mozart_es = es_util.get_mozart_es()
    counters = Counter()
    reaped_by_version = Counter()
    by_mechanism = Counter()

    candidates = find_candidates(mozart_es, grace_secs, lookback_days, since=since)
    counters["scanned"] = len(candidates)
    window = f"since {since}" if since else f"{lookback_days}d"
    logging.info(
        f"Found {len(candidates)} retried attempts to check "
        f"(grace={grace_secs}s, window={window})"
    )

    for start in range(0, len(candidates), PAGE_SIZE):
        page = candidates[start : start + PAGE_SIZE]
        ids = [c["_id"] for c in page]
        # realtime multi-GET: unlike a search, this cannot show a stale view
        # of whether the failed doc is still there
        res = mozart_es.es.mget(
            index=FAILED_INDEX, body={"ids": ids},
            _source=["uuid", "status", "@timestamp", "job.retry_count"],
        )
        # pair by _id, not by position: mget preserves order, but a judgement
        # that deletes a document must not depend on it
        by_id = {d.get("_id"): d for d in res.get("docs", []) if isinstance(d, dict)}
        for cand in page:
            doc = by_id.get(cand["_id"], {})
            if not doc.get("found"):
                continue  # no failed doc for this payload; nothing to repair
            orphan = doc["_source"]
            cand_src = cand["_source"]

            if orphan.get("uuid") == cand_src.get("uuid"):
                # same attempt seen in both homes mid-move, not an orphan
                counters["skipped_same_uuid"] += 1
                continue

            orphan_rc = _retry_count(orphan)
            cand_rc = _retry_count(cand_src)
            if orphan_rc >= cand_rc:
                # the failed doc is the LATER attempt (an older leftover sits
                # in the dated index next to a legitimate later failure).
                # retry_count, not @timestamp, is what makes this monotonic.
                counters["skipped_not_superseded"] += 1
                continue

            redis_status = get_job_status(orphan["uuid"])
            if redis_status not in REAPABLE_REDIS_STATUSES:
                # redis is authoritative while ES is in flight
                counters["skipped_redis"] += 1
                logging.info(
                    f"{doc['_id']}: orphan uuid {orphan['uuid']} is not terminal "
                    f"in redis; leaving it."
                )
                continue
            if redis_status is None:
                # Reapable, but the cross-check did not actually happen: the
                # key is gone. HYSDS_JOB_STATUS_EXPIRES is a day in every
                # shipped celeryconfig, so anything older than that is judged
                # on ES alone. Counted so an operator can see how much of a
                # run had no live cross-check.
                counters["redis_expired"] += 1

            mechanism, basis = classify(doc, cand_src)
            detail = (
                f"payload_id={doc['_id']} orphan_uuid={orphan['uuid']} "
                f"newer_uuid={cand_src.get('uuid')} "
                f"retry_count={orphan_rc}->{cand_rc} "
                f"orphan_status={orphan.get('status')} "
                f"orphan_ts={orphan.get('@timestamp')} _version={doc.get('_version')} "
                f"mechanism={mechanism} basis={basis}"
            )

            event = {
                "payload_id": doc["_id"],
                "orphan_uuid": orphan["uuid"],
                "newer_uuid": cand_src.get("uuid"),
                "orphan_retry_count": orphan_rc,
                "newer_retry_count": cand_rc,
                "version": doc.get("_version"),
                "orphan_seq_no": doc.get("_seq_no"),
                "orphan_primary_term": doc.get("_primary_term"),
                "retry_delete_mark": retry_delete_mark(
                    (cand_src.get("job") or {}).get("job_info") or {}) or None,
                "mechanism": mechanism,
                "mechanism_basis": basis,
                "orphan_ts": orphan.get("@timestamp"),
                "retry_ts": ((cand_src.get("job") or {}).get("job_info") or {}).get("time_queued"),
                "dry_run": bool(dry_run),
            }
            # tags are indexed even where the event body is not, so the
            # mechanism can be counted and filtered on without pulling every
            # _source (event_status.template maps `event` enabled: false)
            tags = [f"mechanism:{mechanism}", f"basis:{basis}",
                    "dry_run" if dry_run else "reaped"]

            if dry_run:
                counters["would_reap"] += 1
                reaped_by_version[doc.get("_version")] += 1
                by_mechanism[mechanism] += 1
                logging.info(f"DRY-RUN would reap {detail}")
                # emitted on dry-run too: the audit both PR bodies prescribe
                # reads these events, and a dry-run that produced none could
                # not be reconciled against anything
                log_custom_event("worker_anomaly", "job_failed_orphan_reaped",
                                 event, tags=tags)
                continue

            # Optimistic concurrency: between the mget and this delete the
            # retried attempt can itself fail and REPLACE the doc under the
            # same _id. An unguarded delete would erase that real failure.
            r = mozart_es.delete_by_id(
                index=FAILED_INDEX,
                id=doc["_id"],
                if_seq_no=doc["_seq_no"],
                if_primary_term=doc["_primary_term"],
                ignore=[404, 409],
            )
            result = r.get("result") if isinstance(r, dict) else None
            status = r.get("status") if isinstance(r, dict) else None
            if result == "deleted":
                counters["reaped"] += 1
                reaped_by_version[doc.get("_version")] += 1
                by_mechanism[mechanism] += 1
                logging.info(f"Reaped orphaned job_failed doc: {detail}")
                log_custom_event("worker_anomaly", "job_failed_orphan_reaped",
                                 event, tags=tags)
            elif status == 409:
                # replaced under us; re-examined next sweep and skipped then
                counters["skipped_conflict"] += 1
                logging.info(f"Conflict, leaving for the next sweep: {detail}")
            else:
                counters["skipped_gone"] += 1

    counters["reaped_by_version"] = dict(reaped_by_version)
    counters["by_mechanism"] = dict(by_mechanism)
    logging.info(f"sweep summary: {dict(counters)}")
    return counters


def check_window_against_redis_ttl(lookback_days, since, dry_run, allow_expired):
    """The redis cross-check is structurally inert past HYSDS_JOB_STATUS_EXPIRES.

    A missing key counts as reapable, so any window longer than the TTL runs
    with that safety property disabled for most of its candidates -- including
    the documented one-time historical audit. Refuse rather than pretend.
    """
    ttl = int(app.conf.get("HYSDS_JOB_STATUS_EXPIRES", 86400))
    if since:
        try:
            start = datetime.fromisoformat(str(since).replace("Z", "+00:00"))
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            window = (datetime.now(timezone.utc) - start).total_seconds()
        except ValueError:
            window = float("inf")
    else:
        window = lookback_days * 86400
    if window > ttl and not (dry_run or allow_expired):
        raise SystemExit(
            f"window of {window / 86400:.1f}d exceeds the redis job-status TTL "
            f"({ttl / 86400:.1f}d), so the redis cross-check would be inert for "
            f"most candidates. Re-run with --dry-run, or with "
            f"--allow-expired-redis if that is understood."
        )
    if window > ttl:
        logging.warning(
            f"window of {window / 86400:.1f}d exceeds the redis TTL "
            f"({ttl / 86400:.1f}d); the redis cross-check is inert for most "
            f"candidates in this run"
        )


def daemon(interval, grace_secs, lookback_days, since=None, dry_run=False, once=False,
           allow_expired_redis=False):
    """Sweep forever, jittered like the other mozart watchdogs."""

    check_window_against_redis_ttl(lookback_days, since, dry_run, allow_expired_redis)
    empty_sweeps = 0

    interval_min = interval - int(interval / 4)
    interval_max = int(interval / 4) + interval
    logging.info(f"interval min: {interval_min}")
    logging.info(f"interval max: {interval_max}")
    logging.info(
        f"grace: {grace_secs}s  lookback: {lookback_days}d  dry_run: {dry_run}"
    )

    failed_sweeps = 0
    while True:
        try:
            counters = reap_orphans(grace_secs, lookback_days, since=since,
                                    dry_run=dry_run)
            # "no orphans" and "the query is structurally dead" look identical
            # from the counters otherwise. job.retry_count is not declared in
            # any job_status index template -- it exists by dynamic mapping --
            # so a venue that adds an explicit mapping without it reduces this
            # daemon to zero candidates forever.
            if counters.get("scanned"):
                empty_sweeps = 0
            else:
                empty_sweeps += 1
                if empty_sweeps in (10, 100, 1000):
                    logging.warning(
                        f"{empty_sweeps} consecutive sweeps found no retried "
                        f"attempts at all. If retries are happening, check that "
                        f"job.retry_count is mapped in the job_status template."
                    )
        except Exception as e:
            failed_sweeps += 1
            logging.error(f"Got error: {e}")
            logging.error(traceback.format_exc())
        if once:
            return failed_sweeps == 0
        time.sleep(random.randint(interval_min, interval_max))


if __name__ == "__main__":
    desc = "Reap job_failed docs superseded by a newer attempt."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        default=300,
        help="wake-up time interval in seconds",
    )
    parser.add_argument(
        "-g",
        "--grace-secs",
        type=int,
        default=120,
        help="leave candidates younger than this for the next sweep, so a "
        "retry that is still writing is never raced",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=1,
        help="steady-state candidate window",
    )
    parser.add_argument(
        "--since",
        default=None,
        help="ISO date overriding --lookback-days, for a one-time historical "
        "cleanup or audit (e.g. 2026-07-01)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="log and count what would be reaped without deleting anything",
    )
    parser.add_argument(
        "--once", action="store_true", help="run a single sweep and exit"
    )
    parser.add_argument(
        "--allow-expired-redis",
        action="store_true",
        help="permit a window longer than the redis job-status TTL, in which "
        "the redis cross-check is inert for most candidates",
    )
    args = parser.parse_args()
    ok = daemon(
        args.interval,
        args.grace_secs,
        args.lookback_days,
        since=args.since,
        dry_run=args.dry_run,
        once=args.once,
        allow_expired_redis=args.allow_expired_redis,
    )
    # a --once sweep that threw must not report success to whoever ran it
    if args.once and not ok:
        raise SystemExit(1)
