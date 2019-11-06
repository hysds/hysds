import os
import sys
import json
import re

try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
import unittest
import logging
import tempfile
import shutil
import glob
from contextlib import nullcontext


# hysds.celery searches for configuration on import. So we need to make sure we
# mock it out before the first time it is imported
sys.modules["hysds.celery"] = umock.MagicMock()
logging.basicConfig()


class TestContainerUtils(unittest.TestCase):
    def setUp(self):
        self.root_work_dir = "/data/work"
        self.job_dir = tempfile.mkdtemp(prefix="job-")

        # test data
        self.job_gpu = {
            "task_id": "da9be25e-e281-4d3c-a7d8-e3c0c8342972",
            "job_info": {
                "id": "boogaloo",
                "status": 1,
                "job_dir": self.job_dir,
                "time_start": "0001-01-01T00:00:00.000Z",
                "context_file": "electric",
                "datasets_cfg_file": "more/configuration",
                "metrics": {"product_provenance": dict(), "products_staged": list()},
            },
            "container_image_name": "gpu_pge",
            "container_image_url": "s3://bucket/object",
            "container_mappings": {
                "$HOME/.netrc": ["/root/.netrc"],
                "$HOME/.aws": ["/root/.aws", "ro"],
            },
            "runtime_options": {"gpus": "all"},
            "command": {
                "path": "python",
                "options": [],
                "arguments": [
                    "/home/ops/verdi/ops/scihub_acquisition_scraper/ipf_version.py",
                    "datasets.json",
                ],
                "env": [],
            },
            "dependency_images": [
                {
                    "container_image_name": "dep_gpu_pge",
                    "container_image_url": "s3://bucket/object_dep",
                    "container_mappings": {
                        "$HOME/.netrc": ["/home/ops/.netrc"],
                        "$HOME/.aws": ["/home/ops/.aws", "ro"],
                    },
                    "runtime_options": {"gpus": "2"},
                }
            ],
        }

        # mock app.conf.get()
        def app_conf_get_side_effect(*args, **kargs):
            if args[0] == "K8S":
                return 0
            elif args[0] == "CONTAINER_REGISTRY":
                return None
            else:
                raise RuntimeError("Handling {} not implemented yet.".format(args[0]))

        # mock data
        self.app_mock = umock.patch("hysds.container_utils.app").start()
        self.app_mock.conf.__file__ = "/home/ops/verdi/etc/celeryconfig.py"
        self.app_mock.conf.get.side_effect = app_conf_get_side_effect

    def tearDown(self):
        umock.patch.stopall()
        shutil.rmtree(self.job_dir)

    def get_docker_params_gpus(
        self, image_name, image_url, image_mappings, runtime_options, gpu_flag=None
    ):
        import hysds.container_utils

        # mock data
        makedirs_mock = umock.patch("os.makedirs").start()
        shutil_copy_mock = umock.patch("shutil.copy").start()
        shutil_copytree_mock = umock.patch("shutil.copytree").start()

        # get context
        if gpu_flag is not None:
            # mocked GPU set in os.environ
            cm = umock.patch.dict("os.environ", {"GPU": gpu_flag})
        else:
            # no GPU set in os.environ
            cm = nullcontext()

        # get docker params
        docker_params = {}
        with cm:
            docker_params[image_name] = hysds.container_utils.get_docker_params(
                image_name,
                image_url,
                image_mappings,
                self.root_work_dir,
                self.job_dir,
                runtime_options,
            )
        logging.info(
            "docker_params: {}".format(
                json.dumps(docker_params, indent=2, sort_keys=True)
            )
        )

        return docker_params

    def get_docker_cmd(self, job, params):
        import hysds.container_utils

        cmdLineList = [job["command"]["path"]]
        for opt in job["command"]["options"]:
            cmdLineList.append(opt)
        for arg in job["command"]["arguments"]:
            matchArg = re.search(r"^\$(\w+)$", arg)
            if matchArg:
                arg = job["params"][matchArg.group(1)]
            if isinstance(arg, (list, tuple)):
                cmdLineList.extend(arg)
            else:
                cmdLineList.append(arg)
        cmdLineList = hysds.container_utils.get_docker_cmd(params, cmdLineList)
        cmdLineList = [str(i) for i in cmdLineList]
        cmdLine = " ".join(cmdLineList)
        logging.info("cmdLine: {}".format(cmdLine))

        return cmdLine

    def test_get_docker_params_gpus(self):
        "Test docker params on GPU instance."

        # get params for get_docker_params()
        image_name = self.job_gpu.get("container_image_name")
        image_url = self.job_gpu.get("container_image_url")
        image_mappings = self.job_gpu.get("container_mappings")
        runtime_options = self.job_gpu.get("runtime_options")
        gpu_flag = "1"

        # run test
        docker_params = self.get_docker_params_gpus(
            image_name, image_url, image_mappings, runtime_options, gpu_flag
        )
        cmd_line = self.get_docker_cmd(self.job_gpu, docker_params[image_name])

        # assertions
        self.assertTrue(docker_params[image_name]["runtime_options"]["gpus"] == "all")
        self.assertTrue("--gpus all" in cmd_line)

    def test_get_docker_params_nogpus(self):
        "Test docker params on non-GPU instance."

        # get params for get_docker_params()
        image_name = self.job_gpu.get("container_image_name")
        image_url = self.job_gpu.get("container_image_url")
        image_mappings = self.job_gpu.get("container_mappings")
        runtime_options = self.job_gpu.get("runtime_options")

        # run test with GPU environment variable not defined
        docker_params = self.get_docker_params_gpus(
            image_name, image_url, image_mappings, runtime_options
        )
        cmd_line = self.get_docker_cmd(self.job_gpu, docker_params[image_name])

        # assertions
        self.assertTrue("gpus" not in docker_params[image_name]["runtime_options"])
        self.assertTrue("--gpus all" not in cmd_line)

        # run test with GPU environment variable defined as "0"
        docker_params = self.get_docker_params_gpus(
            image_name, image_url, image_mappings, runtime_options, gpu_flag="0"
        )
        cmd_line = self.get_docker_cmd(self.job_gpu, docker_params[image_name])

        # assertions
        self.assertTrue("gpus" not in docker_params[image_name]["runtime_options"])
        self.assertTrue("--gpus all" not in cmd_line)

    def test_dep_image_get_docker_params_gpus(self):
        "Test dependency image docker params on GPU instance."

        # get params for get_docker_params()
        image_name = self.job_gpu["dependency_images"][0].get("container_image_name")
        image_url = self.job_gpu["dependency_images"][0].get("container_image_url")
        image_mappings = self.job_gpu["dependency_images"][0].get("container_mappings")
        runtime_options = self.job_gpu["dependency_images"][0].get("runtime_options")
        gpu_flag = "1"

        # run test
        docker_params = self.get_docker_params_gpus(
            image_name, image_url, image_mappings, runtime_options, gpu_flag
        )

        # assertions
        self.assertTrue(docker_params[image_name]["runtime_options"]["gpus"] == "2")

    def test_dep_image_get_docker_params_nogpus(self):
        "Test  dependency image docker params on non-GPU instance."

        # get params for get_docker_params()
        image_name = self.job_gpu["dependency_images"][0].get("container_image_name")
        image_url = self.job_gpu["dependency_images"][0].get("container_image_url")
        image_mappings = self.job_gpu["dependency_images"][0].get("container_mappings")
        runtime_options = self.job_gpu["dependency_images"][0].get("runtime_options")

        # run test with GPU environment variable not defined
        docker_params = self.get_docker_params_gpus(
            image_name, image_url, image_mappings, runtime_options
        )

        # assertions
        self.assertTrue("gpus" not in docker_params[image_name]["runtime_options"])

        # run test with GPU environment variable defined as "0"
        docker_params = self.get_docker_params_gpus(
            image_name, image_url, image_mappings, runtime_options, gpu_flag="0"
        )

        # assertions
        self.assertTrue("gpus" not in docker_params[image_name]["runtime_options"])
