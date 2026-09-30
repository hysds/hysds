"""Container labels and the kill step behind them (HC-654).

A revoke or the hard time limit only ever reaches the celery pool child; the
job's containers live on the host daemon. These tests pin how they are found
and stopped afterwards, against fake engine CLI output.
"""

import json
import sys
import unittest
import unittest.mock as umock

# hysds.celery searches for configuration on import, so mock it before any
# hysds import. setdefault to avoid cross-test pollution.
sys.modules.setdefault("hysds.celery", umock.MagicMock())

from hysds.containers import base  # noqa: E402

JOB_DIR = "/data/work/jobs/2026/08/17/21/25/job-WF-SCIFLO_L3_DISP_S1-frame-10855"
OTHER_JOB_DIR = "/data/work/jobs/2026/08/17/21/25/job-WF-SCIFLO_L3_DISP_S1-frame-3061"


def record(cid, status="running", labels=None, workdir="/", mounts=(), image="img"):
    """An inspect record with the fields find_job_containers reads."""
    return {
        "Id": cid,
        "Name": "/" + cid,
        "State": {"Status": status},
        "Config": {"Labels": labels or {}, "WorkingDir": workdir, "Image": image},
        "Mounts": [{"Source": s, "Destination": s} for s in mounts],
    }


class FakeCli:
    """Stands in for Base.run_cli: serves ps/inspect from a record list and
    records stop/kill calls; stop ends every named container, kill too."""

    def __init__(self, records, survive_stop=()):
        self.records = {r["Id"]: r for r in records}
        self.survive_stop = set(survive_stop)
        self.calls = []

    def __call__(self, args, timeout=None):
        args = [str(a) for a in args]
        self.calls.append(args)
        if args[0] == "ps":
            return "\n".join(self.records) + "\n"
        if args[0] == "inspect":
            return json.dumps([self.records[i] for i in args[1:] if i in self.records])
        if args[0] == "stop":
            for i in args[3:]:  # stop -t N ids...
                if i not in self.survive_stop:
                    self.records[i]["State"]["Status"] = "exited"
            return ""
        if args[0] == "kill":
            for i in args[1:]:
                self.records[i]["State"]["Status"] = "exited"
            return ""
        raise AssertionError(f"unexpected engine call: {args}")


class TestFindJobContainers(unittest.TestCase):
    def setUp(self):
        from hysds.containers.docker import Docker

        self.engine = Docker()

    def find(self, records, **kwargs):
        self.engine.run_cli = FakeCli(records)
        return [c["Id"] for c in self.engine.find_job_containers(JOB_DIR, **kwargs)]

    def test_the_job_container_is_found_by_its_labels(self):
        found = self.find(
            [
                record(
                    "pcm",
                    labels={base.JOB_ID_LABEL: "job-a", base.TASK_ID_LABEL: "t1"},
                    mounts=["/data/work/jobs"],
                )
            ],
            job_id="job-a",
            task_id="t1",
        )
        self.assertEqual(found, ["pcm"])

    def test_the_job_container_is_found_by_its_working_dir(self):
        """Older workers stamp no labels; the job container still has -w job_dir."""
        found = self.find(
            [record("pcm", workdir=JOB_DIR, mounts=["/data/work/jobs"])],
            job_id="job-a",
        )
        self.assertEqual(found, ["pcm"])

    def test_a_pge_container_is_found_by_the_job_dir_it_mounts(self):
        """The PGE wrapper launches its own container through the engine socket
        with subdirs of the job dir mounted and no labels."""
        found = self.find(
            [
                record(
                    "pge",
                    workdir="/home/conda",
                    mounts=[JOB_DIR + "/pge_input_dir", JOB_DIR + "/pge_output_dir"],
                )
            ]
        )
        self.assertEqual(found, ["pge"])

    def test_mounting_the_jobs_root_above_the_job_dir_does_not_match(self):
        """verdi and sdswatch mount /data/work/jobs; a prefix test the wrong
        way round would stop the worker itself."""
        found = self.find(
            [
                record("verdi", mounts=["/data/work/jobs", "/data/work/cache"]),
                record("sdswatch", mounts=["/data/work/jobs"]),
                record("registry", mounts=[]),
            ],
            job_id="job-a",
        )
        self.assertEqual(found, [])

    def test_a_sibling_job_dir_does_not_match(self):
        found = self.find(
            [record("other", workdir=OTHER_JOB_DIR, mounts=[OTHER_JOB_DIR + "/x"])]
        )
        self.assertEqual(found, [])

    def test_a_dir_whose_name_extends_the_job_dir_does_not_match(self):
        found = self.find([record("other", mounts=[JOB_DIR + "-2/pge_output_dir"])])
        self.assertEqual(found, [])

    def test_another_attempt_of_the_same_job_is_left_alone(self):
        """A late signal for an old attempt must not stop a live retry."""
        found = self.find(
            [
                record(
                    "retry",
                    labels={base.JOB_ID_LABEL: "job-a", base.TASK_ID_LABEL: "t2"},
                    mounts=[JOB_DIR + "/pge_output_dir"],
                ),
                record(
                    "old",
                    labels={base.JOB_ID_LABEL: "job-a", base.TASK_ID_LABEL: "t1"},
                ),
            ],
            job_id="job-a",
            task_id="t1",
        )
        self.assertEqual(found, ["old"])

    def test_without_a_task_id_the_job_label_is_enough(self):
        found = self.find(
            [
                record(
                    "c", labels={base.JOB_ID_LABEL: "job-a", base.TASK_ID_LABEL: "t9"}
                )
            ],
            job_id="job-a",
        )
        self.assertEqual(found, ["c"])

    def test_containers_without_a_process_are_skipped(self):
        found = self.find(
            [
                record("gone", status="exited", workdir=JOB_DIR),
                record("dead", status="dead", mounts=[JOB_DIR + "/x"]),
                record("paused", status="paused", workdir=JOB_DIR),
            ]
        )
        self.assertEqual(found, ["paused"])

    def test_no_containers_at_all(self):
        self.assertEqual(self.find([]), [])
        self.assertEqual(self.engine.run_cli.calls, [["ps", "-aq", "--no-trunc"]])

    def test_engine_output_that_is_not_json_yields_nothing(self):
        self.engine.run_cli = lambda args, timeout=None: (
            "abc\n" if args[0] == "ps" else "Error: No such object: abc\n"
        )
        self.assertEqual(self.engine.find_job_containers(JOB_DIR), [])


class TestKillJobContainers(unittest.TestCase):
    def setUp(self):
        from hysds.containers.docker import Docker

        self.engine = Docker()

    def test_stop_then_kill_only_the_survivors(self):
        cli = FakeCli(
            [
                record("pcm", workdir=JOB_DIR),
                record("pge", mounts=[JOB_DIR + "/pge_output_dir"]),
                record("verdi", mounts=["/data/work/jobs"]),
            ],
            survive_stop={"pge"},
        )
        self.engine.run_cli = cli

        stopped = self.engine.kill_job_containers(JOB_DIR, job_id="job-a", grace=7)

        self.assertEqual(sorted(stopped), ["pcm", "pge"])
        verbs = [c[0] for c in cli.calls]
        self.assertEqual(verbs, ["ps", "inspect", "stop", "ps", "inspect", "kill"])
        stop = next(c for c in cli.calls if c[0] == "stop")
        self.assertEqual(stop[:3], ["stop", "-t", "7"])
        self.assertEqual(sorted(stop[3:]), ["pcm", "pge"])
        kill = next(c for c in cli.calls if c[0] == "kill")
        self.assertEqual(kill, ["kill", "pge"])
        self.assertEqual(cli.records["verdi"]["State"]["Status"], "running")

    def test_nothing_running_means_no_stop_call(self):
        cli = FakeCli([record("verdi", mounts=["/data/work/jobs"])])
        self.engine.run_cli = cli
        self.assertEqual(self.engine.kill_job_containers(JOB_DIR, grace=1), [])
        self.assertEqual([c[0] for c in cli.calls], ["ps", "inspect"])

    def test_the_grace_falls_back_to_the_celery_config(self):
        cli = FakeCli([record("pcm", workdir=JOB_DIR)])
        self.engine.run_cli = cli
        with umock.patch.object(base, "app") as app:
            app.conf.get.side_effect = lambda k, d=None: (
                11 if k == "CONTAINER_KILL_GRACE" else d
            )
            self.engine.kill_job_containers(JOB_DIR)
        stop = next(c for c in cli.calls if c[0] == "stop")
        self.assertEqual(stop[:3], ["stop", "-t", "11"])


class TestRunCli(unittest.TestCase):
    def test_a_failing_engine_call_is_logged_not_raised(self):
        from hysds.containers.docker import Docker

        engine = Docker()
        with umock.patch.object(base.subprocess, "run") as run:
            run.return_value = umock.Mock(returncode=1, stdout="", stderr="boom")
            self.assertEqual(engine.run_cli(["ps", "-aq"]), "")
            run.assert_called_once()
            self.assertEqual(run.call_args.args[0], ["docker", "ps", "-aq"])

    def test_a_hanging_engine_call_times_out_quietly(self):
        from hysds.containers.docker import Docker

        engine = Docker()
        with umock.patch.object(base.subprocess, "run") as run:
            run.side_effect = base.subprocess.TimeoutExpired(cmd="docker", timeout=1)
            self.assertEqual(engine.run_cli(["inspect", "x"], timeout=1), "")

    def test_podman_talks_to_its_socket(self):
        with umock.patch("hysds.containers.podman.app") as app:
            app.conf.get.side_effect = lambda k, d=None: d
            from hysds.containers.podman import Podman

            engine = Podman()
        with umock.patch.object(base.subprocess, "run") as run:
            run.return_value = umock.Mock(returncode=0, stdout="", stderr="")
            engine.run_cli(["ps", "-aq"])
        cmd = run.call_args.args[0]
        self.assertEqual(cmd[:3], ["podman", "--remote", "--url"])
        self.assertTrue(cmd[3].startswith("unix:"))
        self.assertEqual(cmd[4:], ["ps", "-aq"])


class TestContainerLabels(unittest.TestCase):
    """The labels the kill step matches on are stamped at run time."""

    def params(self, labels):
        return {
            "uid": 1000,
            "gid": 1000,
            "labels": labels,
            "runtime_options": {},
            "volumes": [("/data/work/jobs", "/data/work/jobs")],
            "working_dir": JOB_DIR,
            "image_name": "pcm:1",
        }

    def test_docker_run_carries_the_labels(self):
        from hysds.containers.docker import Docker

        cmd = Docker().create_base_cmd(
            self.params({base.JOB_ID_LABEL: "job-a", base.TASK_ID_LABEL: "t1"})
        )
        self.assertEqual(
            cmd[:6], ["docker", "run", "--init", "--rm", "-u", "1000:1000"]
        )
        self.assertIn("--label", cmd)
        self.assertIn(f"{base.JOB_ID_LABEL}=job-a", cmd)
        self.assertIn(f"{base.TASK_ID_LABEL}=t1", cmd)
        self.assertLess(cmd.index("--label"), cmd.index("-v"))
        self.assertEqual(cmd[-3:], ["-w", JOB_DIR, "pcm:1"])

    def test_podman_run_carries_the_labels(self):
        with umock.patch("hysds.containers.podman.app") as app:
            app.conf.get.side_effect = lambda k, d=None: d
            from hysds.containers.podman import Podman

            cmd = Podman().create_base_cmd(self.params({base.JOB_ID_LABEL: "job-a"}))
        self.assertIn("--label", cmd)
        self.assertEqual(cmd[cmd.index("--label") + 1], f"{base.JOB_ID_LABEL}=job-a")
        self.assertLess(cmd.index("--label"), cmd.index("-v"))

    def test_params_without_labels_still_build(self):
        from hysds.containers.docker import Docker

        params = self.params({})
        del params["labels"]
        cmd = Docker().create_base_cmd(params)
        self.assertNotIn("--label", cmd)

    def test_create_container_params_records_the_labels(self):
        from hysds.containers.docker import Docker

        with umock.patch.object(base, "app") as app:
            app.conf.get.side_effect = lambda k, d=None: {
                "K8S": 0,
                "CACHE_READ_ONLY": True,
            }.get(k, d)
            app.conf.__file__ = "/home/ops/verdi/etc/celeryconfig.py"
            app.conf.WORKER_MOUNT_BLACKLIST = []
            params = Docker().create_container_params(
                "pcm:1",
                "s3://bucket/pcm.tar.gz",
                {},
                "/data/work",
                JOB_DIR,
                labels={base.JOB_ID_LABEL: "job-a", base.TASK_ID_LABEL: 42},
            )
        self.assertEqual(
            params["labels"], {base.JOB_ID_LABEL: "job-a", base.TASK_ID_LABEL: "42"}
        )

    def test_create_container_params_defaults_to_no_labels(self):
        from hysds.containers.docker import Docker

        with umock.patch.object(base, "app") as app:
            app.conf.get.side_effect = lambda k, d=None: {
                "K8S": 0,
                "CACHE_READ_ONLY": True,
            }.get(k, d)
            app.conf.__file__ = "/home/ops/verdi/etc/celeryconfig.py"
            app.conf.WORKER_MOUNT_BLACKLIST = []
            params = Docker().create_container_params(
                "pcm:1", "s3://bucket/pcm.tar.gz", {}, "/data/work", JOB_DIR
            )
        self.assertEqual(params["labels"], {})


if __name__ == "__main__":
    unittest.main()
