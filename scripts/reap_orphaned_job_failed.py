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
from datetime import datetime

import hysds.es_util as es_util
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


def classify(orphan, candidate):
    """Which mechanism left this orphan behind.

    retry.py stamps job_info.time_queued immediately before it deletes the old
    status doc, so that is the moment of the retry. An orphan written BEFORE
    it is one the delete failed to reach; an orphan written AFTER it is a late
    write that re-created the doc the delete had already removed.

    This is the discriminator to trust. _version was usable while the worker
    wrote the terminal doc twice, but the single-write fix in the same release
    shifts every version down by one: a missed delete now leaves the doc at 1,
    and a late write leaves it at 3 within index.gc_deletes or 1 once the
    tombstone has expired. So 1 is ambiguous on _version alone and 2 becomes
    unreachable except for docs written before the upgrade.
    """
    o_ts = _parse_ts(orphan.get("@timestamp"))
    r_ts = _parse_ts(
        ((candidate.get("job") or {}).get("job_info") or {}).get("time_queued")
    )
    if o_ts is None or r_ts is None:
        return "unknown"
    return "missed-delete" if o_ts < r_ts else "late-write"


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
                    "job.retry_count", "job.job_info.time_queued"],
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
        res = mozart_es.es.mget(index=FAILED_INDEX, body={"ids": ids})
        for cand, doc in zip(page, res["docs"]):  # mget preserves order
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

            if get_job_status(orphan["uuid"]) not in REAPABLE_REDIS_STATUSES:
                # redis is authoritative while ES is in flight
                counters["skipped_redis"] += 1
                logging.info(
                    f"{doc['_id']}: orphan uuid {orphan['uuid']} is not terminal "
                    f"in redis; leaving it."
                )
                continue

            mechanism = classify(orphan, cand_src)
            detail = (
                f"payload_id={doc['_id']} orphan_uuid={orphan['uuid']} "
                f"newer_uuid={cand_src.get('uuid')} "
                f"retry_count={orphan_rc}->{cand_rc} "
                f"orphan_status={orphan.get('status')} "
                f"orphan_ts={orphan.get('@timestamp')} _version={doc.get('_version')} "
                f"mechanism={mechanism}"
            )

            if dry_run:
                counters["would_reap"] += 1
                reaped_by_version[doc.get("_version")] += 1
                by_mechanism[mechanism] += 1
                logging.info(f"DRY-RUN would reap {detail}")
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
                # `mechanism` is the field triage, and it stays correct after
                # the single-write fix lands. _version is kept alongside it
                # because it is still informative on docs written by an older
                # worker, but on its own it no longer separates the two
                # causes -- see classify().
                log_custom_event(
                    "worker_anomaly",
                    "job_failed_orphan_reaped",
                    {
                        "payload_id": doc["_id"],
                        "orphan_uuid": orphan["uuid"],
                        "newer_uuid": cand_src.get("uuid"),
                        "orphan_retry_count": orphan_rc,
                        "newer_retry_count": cand_rc,
                        "version": doc.get("_version"),
                        "mechanism": mechanism,
                        "orphan_ts": orphan.get("@timestamp"),
                        "retry_ts": ((cand_src.get("job") or {}).get("job_info") or {}).get("time_queued"),
                    },
                )
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


def daemon(interval, grace_secs, lookback_days, since=None, dry_run=False, once=False):
    """Sweep forever, jittered like the other mozart watchdogs."""

    interval_min = interval - int(interval / 4)
    interval_max = int(interval / 4) + interval
    logging.info(f"interval min: {interval_min}")
    logging.info(f"interval max: {interval_max}")
    logging.info(
        f"grace: {grace_secs}s  lookback: {lookback_days}d  dry_run: {dry_run}"
    )

    while True:
        try:
            reap_orphans(grace_secs, lookback_days, since=since, dry_run=dry_run)
        except Exception as e:
            logging.error(f"Got error: {e}")
            logging.error(traceback.format_exc())
        if once:
            break
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
        default=2,
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
    args = parser.parse_args()
    daemon(
        args.interval,
        args.grace_secs,
        args.lookback_days,
        since=args.since,
        dry_run=args.dry_run,
        once=args.once,
    )
