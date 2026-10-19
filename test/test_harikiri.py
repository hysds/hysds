"""harikiri's stale-job rule (HC-658).

A job dir without .done keeps the instance up. When the pool child that would
have written .done was killed (hard time limit, lost child, worker restart),
that used to be forever. Now .running older than the job's own time_limit plus
a grace is treated as finished. Both harikiri scripts carry the rule; the tests
run against each.
"""

import importlib.util
import json
import os
import pathlib
import shutil
import sys
import tempfile
import time
import unittest
import unittest.mock as umock
from datetime import datetime, timezone

# the scripts import boto3/requests/yaml/backoff and `future`; stub the last in
# case it is not installed. scripts/ is not a package: load by path.
sys.modules.setdefault("future", umock.MagicMock())

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


utils = load("harikiri_utils")

HOUR = 3600


class JobDirs:
    """A root work dir with the jobs/YYYY/MM/DD/HH/MM/<job_id> layout."""

    def __init__(self):
        self.root = tempfile.mkdtemp(prefix="harikiri-")

    def cleanup(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def job(self, job_id="job-1", running_age=None, time_limit=HOUR, done_age=None):
        """A job dir; running_age/done_age are seconds ago for the file mtimes,
        None leaves the file out; time_limit None leaves _job.json out."""
        d = os.path.join(self.root, "jobs", "2026", "08", "17", "21", "25", job_id)
        os.makedirs(d)
        now = time.time()
        if time_limit is not None:
            with open(os.path.join(d, "_job.json"), "w") as f:
                json.dump({"task_id": "t1", "job_info": {"time_limit": time_limit}}, f)
        if running_age is not None:
            p = os.path.join(d, ".running")
            open(p, "w").write("2026-08-17T21:25:41.577139Z\n")
            os.utime(p, (now - running_age, now - running_age))
        if done_age is not None:
            p = os.path.join(d, ".done")
            open(p, "w").write("x\n")
            os.utime(p, (now - done_age, now - done_age))
        return d


class TestStaleJobDeadline(unittest.TestCase):
    def setUp(self):
        self.dirs = JobDirs()
        self.addCleanup(self.dirs.cleanup)

    def test_a_fresh_running_file_is_not_stale(self):
        d = self.dirs.job(running_age=10, time_limit=HOUR)
        self.assertIsNone(utils.stale_job_deadline(d, grace=600))

    def test_running_past_the_limit_but_inside_the_grace_is_not_stale(self):
        d = self.dirs.job(running_age=HOUR + 100, time_limit=HOUR)
        self.assertIsNone(utils.stale_job_deadline(d, grace=600))

    def test_running_past_limit_and_grace_is_stale(self):
        d = self.dirs.job(running_age=HOUR + 601, time_limit=HOUR)
        deadline = utils.stale_job_deadline(d, grace=600)
        self.assertIsNotNone(deadline)
        start = os.path.getmtime(os.path.join(d, ".running"))
        self.assertAlmostEqual(deadline, start + HOUR + 600, places=3)

    def test_the_deadline_is_the_kill_time(self):
        """The OPS-POP1 dir: .running 2026-08-17T21:25:41Z, time_limit 129900,
        SIGKILL 2026-08-19T09:30:41Z. With grace 0 the deadline is that second."""
        d = self.dirs.job(running_age=0, time_limit=129900)
        running = os.path.join(d, ".running")
        t0 = datetime(2026, 8, 17, 21, 25, 41, tzinfo=timezone.utc).timestamp()
        os.utime(running, (t0, t0))
        deadline = utils.stale_job_deadline(d, grace=0, now=t0 + 129900)
        self.assertEqual(deadline, t0 + 129900)
        self.assertEqual(utils._iso(deadline), "2026-08-19T09:30:41.000000Z")

    def test_no_time_limit_and_no_default_keeps_the_dir_blocking(self):
        d = self.dirs.job(running_age=30 * HOUR, time_limit=None)
        self.assertIsNone(utils.stale_job_deadline(d, grace=0))

    def test_no_time_limit_uses_the_default_when_given(self):
        d = self.dirs.job(running_age=30 * HOUR, time_limit=None)
        self.assertIsNotNone(
            utils.stale_job_deadline(d, grace=0, default_time_limit=HOUR)
        )

    def test_a_job_json_without_a_usable_limit_counts_as_none(self):
        for bad in (0, -5, "36h", True, None):
            d = self.dirs.job(
                job_id=f"job-{bad}", running_age=30 * HOUR, time_limit=None
            )
            with open(os.path.join(d, "_job.json"), "w") as f:
                json.dump({"job_info": {"time_limit": bad}}, f)
            self.assertIsNone(utils.job_time_limit(d), bad)
        d = self.dirs.job(job_id="job-broken", running_age=30 * HOUR, time_limit=None)
        open(os.path.join(d, "_job.json"), "w").write("{not json")
        self.assertIsNone(utils.job_time_limit(d))

    def test_without_running_the_dir_mtime_is_the_start(self):
        """The child died between makedirs and the .running write."""
        d = self.dirs.job(running_age=None, time_limit=60)
        old = time.time() - 700
        os.utime(d, (old, old))
        self.assertIsNotNone(utils.stale_job_deadline(d, grace=600))

    def test_a_vanished_dir_is_not_stale(self):
        self.assertIsNone(utils.stale_job_deadline("/nonexistent/job", grace=0))


class TestMarkStaleJobDone(unittest.TestCase):
    def setUp(self):
        self.dirs = JobDirs()
        self.addCleanup(self.dirs.cleanup)

    def test_running_becomes_done_with_a_timestamp_first_line(self):
        d = self.dirs.job(running_age=2 * HOUR, time_limit=HOUR)
        deadline = utils.stale_job_deadline(d, grace=0)
        event = utils.mark_stale_job_done(d, deadline)
        self.assertFalse(os.path.exists(os.path.join(d, ".running")))
        lines = open(os.path.join(d, ".done")).read().splitlines()
        self.assertRegex(lines[0], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")
        self.assertIn("marked done by harikiri", lines[1])
        self.assertEqual(event["job_id"], "job-1")
        self.assertEqual(event["time_limit"], HOUR)
        self.assertTrue(event["deadline"].endswith("Z"))

    def test_marking_works_without_a_running_file(self):
        d = self.dirs.job(running_age=None, time_limit=HOUR)
        utils.mark_stale_job_done(d, time.time() - 1)
        self.assertTrue(os.path.exists(os.path.join(d, ".done")))


class IsJoblessMixin:
    """Run against both harikiri scripts; they carry the same is_jobless."""

    script = None

    def setUp(self):
        self.mod = load(self.script)
        self.mod.NO_JOBS_TIMER = None
        self.mod.KEEP_ALIVE = False
        self.mod.STALE_GRACE = 600
        self.mod.STALE_DEFAULT_TIME_LIMIT = None
        self.dirs = JobDirs()
        self.addCleanup(self.dirs.cleanup)
        self.log_event = umock.patch.object(
            self.mod, "log_event", return_value={}
        ).start()
        self.addCleanup(umock.patch.stopall)

    def is_jobless(self, inactivity=1800, logger=None):
        return self.mod.is_jobless(self.dirs.root, inactivity, logger)

    def test_a_running_job_still_blocks(self):
        d = self.dirs.job(running_age=10, time_limit=HOUR)
        self.assertFalse(self.is_jobless())
        self.assertFalse(os.path.exists(os.path.join(d, ".done")))
        self.assertIsNone(self.mod.NO_JOBS_TIMER)

    def test_a_job_inside_the_grace_still_blocks(self):
        d = self.dirs.job(running_age=HOUR + 10, time_limit=HOUR)
        self.assertFalse(self.is_jobless())
        self.assertFalse(os.path.exists(os.path.join(d, ".done")))

    def test_a_stale_job_is_marked_done_and_then_ages_out(self):
        d = self.dirs.job(running_age=HOUR + 601, time_limit=HOUR)
        # first pass: marked done just now, so not idle long enough yet
        self.assertFalse(self.is_jobless(inactivity=1800))
        done = os.path.join(d, ".done")
        self.assertTrue(os.path.exists(done))
        self.assertFalse(os.path.exists(os.path.join(d, ".running")))
        self.assertIn("marked done by harikiri", open(done).read())
        # later: the .done age is what counts, as for any other job
        old = time.time() - 1801
        os.utime(done, (old, old))
        self.assertTrue(self.is_jobless(inactivity=1800))

    def test_the_event_is_posted_when_a_logger_is_configured(self):
        d = self.dirs.job(running_age=HOUR + 601, time_limit=HOUR)
        self.is_jobless(logger="https://mozart/api/v0.1")
        self.log_event.assert_called_once()
        url, event_type, status, event, tags = self.log_event.call_args.args
        self.assertEqual(
            (url, event_type, status),
            ("https://mozart/api/v0.1", "harikiri", "stale_job_dir"),
        )
        self.assertEqual(event["job_dir"], d)
        self.assertEqual(tags, [])

    def test_no_event_without_a_logger(self):
        self.dirs.job(running_age=HOUR + 601, time_limit=HOUR)
        self.is_jobless(logger=None)
        self.log_event.assert_not_called()

    def test_a_job_with_no_time_limit_blocks_unless_a_default_is_set(self):
        d = self.dirs.job(running_age=30 * HOUR, time_limit=None)
        self.assertFalse(self.is_jobless())
        self.assertFalse(os.path.exists(os.path.join(d, ".done")))
        self.mod.STALE_DEFAULT_TIME_LIMIT = HOUR
        self.assertFalse(self.is_jobless())  # marked just now
        self.assertTrue(os.path.exists(os.path.join(d, ".done")))

    def test_the_grace_setting_is_honoured(self):
        d = self.dirs.job(running_age=HOUR + 100, time_limit=HOUR)
        self.mod.STALE_GRACE = 50
        self.is_jobless()
        self.assertTrue(os.path.exists(os.path.join(d, ".done")))

    def test_a_stale_dir_does_not_hide_a_live_one(self):
        self.dirs.job(job_id="stale", running_age=HOUR + 601, time_limit=HOUR)
        live = self.dirs.job(job_id="live", running_age=5, time_limit=HOUR)
        self.assertFalse(self.is_jobless())
        self.assertFalse(os.path.exists(os.path.join(live, ".done")))

    def test_done_jobs_behave_as_before(self):
        self.dirs.job(job_id="old", running_age=None, done_age=5000)
        self.assertTrue(self.is_jobless(inactivity=1800))
        self.dirs.job(job_id="recent", running_age=None, done_age=5)
        self.assertFalse(self.is_jobless(inactivity=1800))

    def test_keep_alive_still_wins(self):
        self.dirs.job(running_age=HOUR + 601, time_limit=HOUR)
        open(os.path.join(self.dirs.root, ".harikiri"), "w").close()
        self.assertIsNone(self.is_jobless())


class TestIsJoblessSqs(IsJoblessMixin, unittest.TestCase):
    script = "harikiri_sqs"


class TestIsJoblessAsg(IsJoblessMixin, unittest.TestCase):
    script = "harikiri"


if __name__ == "__main__":
    unittest.main()
