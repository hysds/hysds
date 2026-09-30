"""Finishing a job on behalf of a pool child that cannot (HC-654).

The hard time limit SIGKILLs the child that would have stopped the job's
containers and written .done. These tests pin the main-process side: where a
job dir is found, when it is marked done, that containers go first, and which
celery signals get there.
"""

import ast
import inspect
import json
import os
import shutil
import sys
import tempfile
import unittest
import unittest.mock as umock

# hysds.celery searches for configuration on import, so mock it before any
# hysds import. setdefault to avoid cross-test pollution.
sys.modules.setdefault("hysds.celery", umock.MagicMock())

import hysds.job_worker as jw  # noqa: E402


class JobDirs:
    def __init__(self):
        self.root = tempfile.mkdtemp(prefix="teardown-")

    def cleanup(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def job(self, job_id="job-1", minute="25", task_id="t1", running=True, done=False):
        d = os.path.join(self.root, "jobs", "2026", "08", "17", "21", minute, job_id)
        os.makedirs(d)
        if task_id is not None:
            with open(os.path.join(d, "_job.json"), "w") as f:
                json.dump({"task_id": task_id, "job_info": {"time_limit": 100}}, f)
        if running:
            open(os.path.join(d, ".running"), "w").write("2026-08-17T21:25:41Z\n")
        if done:
            open(os.path.join(d, ".done"), "w").write("2026-08-17T22:25:41Z\n")
        return d


class TestFindJobDirs(unittest.TestCase):
    def setUp(self):
        self.dirs = JobDirs()
        self.addCleanup(self.dirs.cleanup)

    def test_finds_every_dir_of_the_job_oldest_first(self):
        b = self.dirs.job(minute="30")
        a = self.dirs.job(minute="25")
        self.dirs.job(job_id="job-2", minute="26")
        self.assertEqual(jw.find_job_dirs(self.dirs.root, "job-1"), [a, b])

    def test_nothing_for_an_unknown_job(self):
        self.assertEqual(jw.find_job_dirs(self.dirs.root, "job-9"), [])

    def test_a_file_with_the_job_id_name_is_not_a_dir(self):
        d = os.path.join(self.dirs.root, "jobs", "2026", "08", "17", "21", "25")
        os.makedirs(d)
        open(os.path.join(d, "job-1"), "w").close()
        self.assertEqual(jw.find_job_dirs(self.dirs.root, "job-1"), [])

    def test_the_lookup_does_not_walk_into_job_dirs(self):
        """A job dir holds a PGE's whole output tree; the old os.walk descended
        into every one of them before reaching the next sibling."""
        self.assertNotIn("os.walk", inspect.getsource(jw.find_job_dirs))


class TestMarkJobDone(unittest.TestCase):
    def setUp(self):
        self.dirs = JobDirs()
        self.addCleanup(self.dirs.cleanup)

    def test_running_becomes_done(self):
        d = self.dirs.job()
        self.assertTrue(jw.mark_job_done(d))
        self.assertFalse(os.path.exists(os.path.join(d, ".running")))
        self.assertRegex(
            open(os.path.join(d, ".done")).read(), r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z\n$"
        )

    def test_done_is_written_even_without_running(self):
        d = self.dirs.job(running=False)
        self.assertTrue(jw.mark_job_done(d))
        self.assertTrue(os.path.exists(os.path.join(d, ".done")))

    def test_an_existing_done_is_left_untouched(self):
        d = self.dirs.job(running=False, done=True)
        before = open(os.path.join(d, ".done")).read()
        self.assertFalse(jw.mark_job_done(d))
        self.assertEqual(open(os.path.join(d, ".done")).read(), before)


class TestTeardownJob(unittest.TestCase):
    def setUp(self):
        self.dirs = JobDirs()
        self.addCleanup(self.dirs.cleanup)
        self.engine = umock.MagicMock()
        self.engine.kill_job_containers.return_value = ["c1"]
        umock.patch.object(
            jw, "container_engine_factory", return_value=self.engine
        ).start()
        self.addCleanup(umock.patch.stopall)

    def test_containers_are_stopped_then_the_dir_is_marked_done(self):
        d = self.dirs.job(task_id="t1")
        finished = jw.teardown_job(self.dirs.root, "job-1", task_id="t1")
        self.assertEqual(finished, [d])
        self.engine.kill_job_containers.assert_called_once_with(
            d, job_id="job-1", task_id="t1"
        )
        self.assertTrue(os.path.exists(os.path.join(d, ".done")))
        self.assertFalse(os.path.exists(os.path.join(d, ".running")))

    def test_the_kill_comes_before_the_done_write(self):
        """harikiri and the next job read .done; the containers must be gone
        by then, so order matters."""
        d = self.dirs.job(task_id="t1")

        def kill(job_dir, job_id=None, task_id=None):
            self.assertFalse(os.path.exists(os.path.join(job_dir, ".done")))
            return []

        self.engine.kill_job_containers.side_effect = kill
        jw.teardown_job(self.dirs.root, "job-1", task_id="t1")
        self.assertTrue(os.path.exists(os.path.join(d, ".done")))

    def test_a_dir_that_is_already_done_is_skipped(self):
        self.dirs.job(running=False, done=True)
        self.assertEqual(jw.teardown_job(self.dirs.root, "job-1", task_id="t1"), [])
        self.engine.kill_job_containers.assert_not_called()

    def test_another_attempts_dir_is_left_alone(self):
        """retry.py resubmits while the old task may still be running, so a
        late signal for the old attempt must not mark the live retry done."""
        old = self.dirs.job(minute="25", task_id="t1")
        live = self.dirs.job(minute="40", task_id="t2")
        finished = jw.teardown_job(self.dirs.root, "job-1", task_id="t1")
        self.assertEqual(finished, [old])
        self.assertTrue(os.path.exists(os.path.join(live, ".running")))
        self.assertFalse(os.path.exists(os.path.join(live, ".done")))

    def test_a_dir_without_job_json_is_still_finished(self):
        """The child died before writing _job.json; nobody else owns the dir."""
        d = self.dirs.job(task_id=None)
        self.assertEqual(jw.teardown_job(self.dirs.root, "job-1", task_id="t1"), [d])

    def test_without_a_task_id_every_dir_of_the_job_is_finished(self):
        a = self.dirs.job(minute="25", task_id="t1")
        b = self.dirs.job(minute="40", task_id="t2")
        self.assertEqual(jw.teardown_job(self.dirs.root, "job-1"), [a, b])

    def test_kill_containers_can_be_switched_off(self):
        d = self.dirs.job()
        jw.teardown_job(self.dirs.root, "job-1", kill_containers=False)
        self.engine.kill_job_containers.assert_not_called()
        self.assertTrue(os.path.exists(os.path.join(d, ".done")))

    def test_an_unknown_job_is_a_no_op(self):
        self.assertEqual(jw.teardown_job(self.dirs.root, "job-9"), [])
        self.engine.kill_job_containers.assert_not_called()

    def test_an_unreachable_engine_does_not_stop_the_done_write(self):
        d = self.dirs.job()
        self.engine.kill_job_containers.side_effect = RuntimeError("no daemon")
        self.assertEqual(jw.teardown_job(self.dirs.root, "job-1"), [d])
        self.assertTrue(os.path.exists(os.path.join(d, ".done")))

    def test_the_old_name_still_marks_done_without_touching_containers(self):
        d = self.dirs.job()
        jw.set_revoked_job_done(self.dirs.root, "job-1")
        self.assertTrue(os.path.exists(os.path.join(d, ".done")))
        self.engine.kill_job_containers.assert_not_called()


class TestKillJobContainersHelper(unittest.TestCase):
    def test_uses_the_configured_engine(self):
        engine = umock.MagicMock()
        engine.kill_job_containers.return_value = ["abc"]
        with umock.patch.object(
            jw, "container_engine_factory", return_value=engine
        ) as f:
            jw.app.conf.get.side_effect = lambda k, d=None: (
                "podman" if k == "CONTAINER_ENGINE" else d
            )
            try:
                self.assertEqual(jw.kill_job_containers("/j", "job-1", "t1"), ["abc"])
            finally:
                jw.app.conf.get.side_effect = None
        f.assert_called_once_with("podman")
        engine.kill_job_containers.assert_called_once_with(
            "/j", job_id="job-1", task_id="t1"
        )

    def test_never_raises(self):
        with umock.patch.object(
            jw, "container_engine_factory", side_effect=ValueError("x")
        ):
            self.assertEqual(jw.kill_job_containers("/j", "job-1", "t1"), [])


class TestSignalHandlers(unittest.TestCase):
    def setUp(self):
        self.teardown = umock.patch.object(jw, "teardown_job", return_value=[]).start()
        self.addCleanup(umock.patch.stopall)
        jw.app.conf.ROOT_WORK_DIR = "/data/work"

    def job(self):
        return {
            "task_id": "t1",
            "job_id": "job-1",
            "job_info": {
                "job_payload": {"payload_task_id": "p1"},
                "payload_hash": "h",
                "dedup": True,
                "index": "job_status-2026.08.17",
            },
        }

    def test_the_signals_are_connected_to_run_job(self):
        from celery.signals import task_revoked

        self.assertTrue(task_revoked.has_listeners(jw.run_job))

    def test_revoke_tears_the_job_down_for_its_own_task(self):
        umock.patch.object(jw, "log_job_status").start()
        umock.patch.object(jw, "job_supersession", return_value=jw.OWNED).start()
        request = umock.MagicMock()
        request.args = [self.job()]
        request.hostname = "celery@worker-1"
        jw.task_revoked_handler(sender=jw.run_job, request=request, signum=15)
        self.teardown.assert_called_once_with("/data/work", "job-1", task_id="t1")


def run_job_source():
    """run_job's source. With hysds.celery mocked, jw.run_job is the mock's
    return value, so read the module and cut the function out of it."""
    src = inspect.getsource(jw)
    tree = ast.parse(src)
    node = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_job"
    )
    return ast.get_source_segment(src, node)


class TestRunJobFailurePath(unittest.TestCase):
    """The failure path inside run_job stops the job's containers before triage
    and the close-out. Driving run_job itself would mean its full filesystem,
    redis, engine and celery surface, so this is pinned at the source level
    like the other run_job invariants in test_job_worker.py."""

    KILL = 'kill_job_containers(job_dir, job_id, job["task_id"])'

    def test_kill_runs_after_the_soft_limit_signal_and_before_the_close_out(self):
        src = run_job_source()
        kill = src.index(self.KILL)
        self.assertLess(src.index("Soft time limit (%ds) exceeded"), kill)
        self.assertLess(kill, src.index("# run post-processing steps"))
        self.assertLess(kill, src.index("os.rename(job_running_file, job_done_file)"))

    def test_the_kill_is_gated_on_the_command_having_started(self):
        src = run_job_source()
        kill = src.index(self.KILL)
        self.assertIn("if cmd_start is not None:", src[kill - 200 : kill])

    def test_the_job_containers_are_labelled(self):
        src = run_job_source()
        self.assertIn('JOB_ID_LABEL: job_id, TASK_ID_LABEL: job["task_id"]', src)
        self.assertEqual(src.count("labels=container_labels"), 2)


if __name__ == "__main__":
    unittest.main()
