"""
Job-dir bookkeeping shared by harikiri.py and harikiri_sqs.py.

A job dir under <root_work_dir>/jobs without a .done file is a running job,
and harikiri keeps the instance up for it. The .running -> .done transition is
written by the celery pool child that runs the job, or by the worker's revoke
handler. When that child is killed instead of finishing -- the hard time limit
is the usual case -- nothing writes .done, .running outlives the job, and the
instance runs until somebody notices.

The bound comes from the job itself. run_job writes .running when it starts,
and _job.json carries job_info.time_limit, the hard limit celery enforces from
the moment the child accepted the task. Once .running is older than that limit
plus a grace period, no process of that job can still be running on this
worker, so the dir can be marked done and aged out like any other.
"""

import json
import os
import time
from datetime import datetime, timezone

# seconds past .running + time_limit before a dir is treated as finished
DEFAULT_STALE_GRACE = 600


def job_time_limit(job_dir):
    """The hard time limit run_job recorded for the job, in seconds, or None."""

    try:
        with open(os.path.join(job_dir, "_job.json")) as f:
            limit = (json.load(f).get("job_info") or {}).get("time_limit")
    except (OSError, ValueError, AttributeError):
        return None
    if isinstance(limit, bool) or not isinstance(limit, (int, float)) or limit <= 0:
        return None
    return float(limit)


def job_start_time(job_dir):
    """When the job started: the mtime of .running, or of the dir itself when
    the job died before writing it. None when the dir is gone."""

    for path in (os.path.join(job_dir, ".running"), job_dir):
        try:
            return os.path.getmtime(path)
        except OSError:
            continue
    return None


def stale_job_deadline(
    job_dir, grace=DEFAULT_STALE_GRACE, default_time_limit=None, now=None
):
    """
    When a job dir without .done can be treated as finished.

    :param job_dir: str
    :param grace: seconds added to the time limit
    :param default_time_limit: seconds to assume when _job.json has no time
        limit; None keeps such a dir blocking, as before
    :param now: epoch seconds, for tests
    :return: the epoch seconds at which the job cannot still have been running
        (start + time_limit + grace) once that moment has passed; None while
        it has not, or when there is no time limit to bound it with
    """

    now = time.time() if now is None else now
    start = job_start_time(job_dir)
    if start is None:
        return None
    limit = job_time_limit(job_dir)
    if limit is None:
        limit = default_time_limit
    if limit is None:
        return None
    deadline = start + float(limit) + float(grace)
    return deadline if now >= deadline else None


def _iso(epoch):
    if epoch is None:
        return None
    return (
        datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
        + "Z"
    )


def mark_stale_job_done(job_dir, deadline, now=None):
    """
    Write .done for a job that outlived its time limit, the way run_job would
    have: .running becomes .done, first line the timestamp, and a second line
    saying why harikiri wrote it.

    :return: dict describing what was done, for the harikiri event log
    """

    now = time.time() if now is None else now
    start = job_start_time(job_dir)
    running_file = os.path.join(job_dir, ".running")
    done_file = os.path.join(job_dir, ".done")
    if os.path.exists(running_file):
        os.replace(running_file, done_file)
    with open(done_file, "w") as f:
        f.write(f"{_iso(now)}\n")
        f.write(
            f"stale: no .done {int(now - deadline)}s past .running + time_limit + "
            f"grace; marked done by harikiri\n"
        )
    return {
        "job_dir": job_dir,
        "job_id": os.path.basename(job_dir),
        "time_limit": job_time_limit(job_dir),
        "started": _iso(start),
        "deadline": _iso(deadline),
        "marked": _iso(now),
    }
