import os
import sys

try:
    import unittest.mock as umock
except ImportError:
    from unittest import mock as umock

import logging
import shutil
import tempfile
from unittest import TestCase
from unittest.mock import patch

# hysds.celery searches for configuration on import. So we need to make sure we
# mock it out before the first time it is imported
sys.modules["hysds.celery"] = umock.MagicMock()
logging.basicConfig()


class TestJobWorkerFuncs(TestCase):
    def setUp(self):
        self.examples_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "examples"
        )
        self.job_dir = tempfile.mkdtemp(prefix="job-")
        logging.info(f"self.job_dir: {self.job_dir}")

    def tearDown(self):
        umock.patch.stopall()
        shutil.rmtree(self.job_dir)

    def test_find_usage_stats(self):
        import hysds.job_worker

        # copy example _docker_stats.json
        stats_file = os.path.join(self.examples_dir, "_docker_stats.json")
        shutil.copy(stats_file, self.job_dir)
        subdir = os.path.join(self.job_dir, "subdir1", "subdir2")
        os.makedirs(subdir)
        shutil.copy(stats_file, subdir)

        # expected results
        expected_stats_file = os.path.join(self.job_dir, "_docker_stats.json")
        expected_stats_file2 = os.path.join(subdir, "_docker_stats.json")

        # test execution
        result = hysds.job_worker.find_usage_stats(self.job_dir)

        # assertions
        self.assertTrue(expected_stats_file in result)
        self.assertTrue(expected_stats_file2 in result)


class TestFailJob(TestCase):
    """fail_job must not re-log a status the caller already logged."""

    def setUp(self):
        import hysds.job_worker

        self.jw = hysds.job_worker
        self.doc = {
            "uuid": "task-1",
            "payload_id": "payload-1",
            "status": "job-failed",
            "error": "boom",
            "celery_hostname": "worker-1",
            "job": {"job_info": {"index": "job_status-2026.08.28"}},
        }
        self.log_status = patch.object(self.jw, "log_job_status").start()
        patch.object(self.jw, "job_drain_detected", return_value=False).start()
        patch.object(self.jw, "shutdown_worker").start()

    def tearDown(self):
        umock.patch.stopall()

    def test_fail_job_logs_status_by_default(self):
        """Call sites whose doc was never logged must still write it."""
        with self.assertRaises(self.jw.WorkerExecutionError):
            self.jw.fail_job(self.doc, "/tmp/nonexistent-jd-file")

        self.log_status.assert_called_once_with(self.doc)

    def test_fail_job_skips_the_duplicate_write(self):
        """The tail call site already logged this doc and queued its rules.

        A second terminal write travels the async redis -> logstash -> ES
        pipeline behind the first and can land after a fast retry has deleted
        the doc, resurrecting it as an orphan. On develop fail_job
        has no log_status parameter, so this raises TypeError.
        """
        with self.assertRaises(self.jw.WorkerExecutionError):
            self.jw.fail_job(self.doc, "/tmp/nonexistent-jd-file", log_status=False)

        self.log_status.assert_not_called()

    def test_fail_job_raises_carrying_the_status_doc(self):
        """Either way the error carries the doc for the caller's handler.

        The doc rides on .job_status, not in args: WorkerExecutionError keeps
        args to the message alone so celery can rebuild it (see #221).
        """
        with self.assertRaises(self.jw.WorkerExecutionError) as ctx:
            self.jw.fail_job(self.doc, "/tmp/nonexistent-jd-file", log_status=False)

        self.assertIs(ctx.exception.job_status, self.doc)
        self.assertEqual(ctx.exception.args, ("boom",))


class TestFailJobCallSites(TestCase):
    """The call sites, not just the parameter.

    Reverting the tail call to `fail_job(job_status_json, jd_file)` restores
    the deterministic double write for every failed job, and the behavioural
    tests above still pass because they exercise fail_job directly. Driving
    run_job to cover this would mean ~1,200 lines of filesystem, redis, docker
    and celery side effects, so the invariant is asserted at the source level:
    once the terminal doc has been logged, no later call may log it again.
    """

    def _run_job(self):
        import ast
        import inspect

        import hysds.job_worker

        tree = ast.parse(inspect.getsource(hysds.job_worker))
        return next(
            n for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "run_job"
        )

    def _status_logged_line(self, run_job):
        import ast

        lines = [
            n.lineno for n in ast.walk(run_job)
            if isinstance(n, ast.Assign)
            and any(
                isinstance(tgt, ast.Name) and tgt.id == "status_logged"
                for tgt in n.targets
            )
            and isinstance(n.value, ast.Constant)
            and n.value.value is True
        ]
        self.assertEqual(
            len(lines), 1,
            "expected exactly one `status_logged = True`, marking the point "
            "after which the terminal doc exists",
        )
        return lines[0]

    def test_the_flag_is_set_right_after_the_terminal_write(self):
        import ast

        run_job = self._run_job()
        flag_line = self._status_logged_line(run_job)
        writes = [
            n.lineno for n in ast.walk(run_job)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "log_job_status"
        ]
        self.assertIn(
            flag_line - 1, writes,
            "`status_logged = True` must directly follow the log_job_status "
            "call it describes, or it stops tracking what it claims to",
        )

    def test_no_fail_job_call_relogs_after_the_terminal_write(self):
        import ast

        run_job = self._run_job()
        flag_line = self._status_logged_line(run_job)
        later = [
            n for n in ast.walk(run_job)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "fail_job"
            and n.lineno > flag_line
        ]
        self.assertTrue(later, "expected fail_job call sites after the write")
        for call in later:
            kwargs = {k.arg for k in call.keywords}
            self.assertIn(
                "log_status", kwargs,
                f"fail_job at line {call.lineno} runs after the terminal doc "
                f"was written but does not say whether it logs it; the "
                f"default writes the same _id a second time",
            )
