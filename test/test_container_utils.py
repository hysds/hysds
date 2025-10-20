import json
import os
import re
import sys

try:
    import unittest.mock as umock
except ImportError:
    from unittest import mock as umock

import glob
import logging
import shutil
import tempfile
import unittest
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
                raise RuntimeError(f"Handling {args[0]} not implemented yet.")

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
            cm = umock.patch.dict("os.environ", {"HYSDS_GPU_AVAILABLE": gpu_flag})
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
            f"docker_params: {json.dumps(docker_params, indent=2, sort_keys=True)}"
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
        logging.info(f"cmdLine: {cmdLine}")

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

    def test_runtime_options_env_expansion(self):
        """Test that environment variables in runtime_options are properly expanded."""
        
        # Set up test environment variables
        test_env_vars = {
            "HYSDS_ROOT_WORK_DIR": "/data/work_1",
            "TEST_VAR": "test_value",
            "PATH_VAR": "/some/path/with/variables"
        }
        
        with umock.patch.dict("os.environ", test_env_vars):
            # Test runtime_options with environment variables
            runtime_options = {
                "env": "HYSDS_ROOT_WORK_DIR=$HYSDS_ROOT_WORK_DIR,TEST_VAR=$TEST_VAR",
                "volume": "$PATH_VAR:/container/path",
                "label": "workdir=$HYSDS_ROOT_WORK_DIR"
            }
            
            # Get params using container_utils
            import hysds.container_utils
            
            docker_params = hysds.container_utils.get_docker_params(
                image_name="test_image",
                image_url="test_url",
                image_mappings={},
                root_work_dir=self.root_work_dir,
                job_dir=self.job_dir,
                runtime_options=runtime_options
            )
            
            # Verify environment variable expansion
            expected_runtime_options = {
                "env": "HYSDS_ROOT_WORK_DIR=/data/work_1,TEST_VAR=test_value",
                "volume": "/some/path/with/variables:/container/path",
                "label": "workdir=/data/work_1"
            }
            
            self.assertEqual(
                docker_params["runtime_options"], 
                expected_runtime_options,
                "Environment variables should be expanded in runtime_options"
            )

    def test_runtime_options_env_expansion_docker_class(self):
        """Test that environment variables in runtime_options are properly expanded using Docker class."""
        
        # Set up test environment variables
        test_env_vars = {
            "HYSDS_ROOT_WORK_DIR": "/data/work_1",
            "TEST_VAR": "test_value"
        }
        
        # Mock the app.conf.__file__ that's needed by the base class
        app_mock = umock.patch("hysds.containers.base.app").start()
        app_mock.conf.__file__ = "/home/ops/verdi/etc/celeryconfig.py"
        app_mock.conf.get.side_effect = lambda x, default=None: {
            "K8S": 0,
            "CONTAINER_REGISTRY": None,
            "CACHE_READ_ONLY": True,
            "EVICT_CACHE": True
        }.get(x, default)
        
        with umock.patch.dict("os.environ", test_env_vars):
            # Test runtime_options with environment variables
            runtime_options = {
                "env": "HYSDS_ROOT_WORK_DIR=$HYSDS_ROOT_WORK_DIR",
                "label": "test=$TEST_VAR"
            }
            
            # Get params using Docker class
            import hysds.containers.docker
            
            docker = hysds.containers.docker.Docker()
            docker_params = docker.create_container_params(
                image_name="test_image",
                image_url="test_url",
                image_mappings={},
                root_work_dir=self.root_work_dir,
                job_dir=self.job_dir,
                runtime_options=runtime_options
            )
            
            # Verify environment variable expansion
            expected_runtime_options = {
                "env": "HYSDS_ROOT_WORK_DIR=/data/work_1",
                "label": "test=test_value"
            }
            
            self.assertEqual(
                docker_params["runtime_options"], 
                expected_runtime_options,
                "Environment variables should be expanded in runtime_options via Docker class"
            )

    def test_runtime_options_no_expansion_for_non_strings(self):
        """Test that non-string values in runtime_options are not processed for expansion."""
        
        # Test runtime_options with mixed types
        runtime_options = {
            "env": "HYSDS_ROOT_WORK_DIR=$HYSDS_ROOT_WORK_DIR",  # string - should be expanded
            "gpus": 2,  # integer - should not be expanded
            "memory": 1024,  # integer - should not be expanded
            "label": ["workdir=$HYSDS_ROOT_WORK_DIR"]  # list - should not be expanded
        }
        
        test_env_vars = {
            "HYSDS_ROOT_WORK_DIR": "/data/work_1",
            "HYSDS_GPU_AVAILABLE": "1"  # Enable GPU to prevent filtering
        }
        
        with umock.patch.dict("os.environ", test_env_vars):
            import hysds.container_utils
            
            docker_params = hysds.container_utils.get_docker_params(
                image_name="test_image",
                image_url="test_url",
                image_mappings={},
                root_work_dir=self.root_work_dir,
                job_dir=self.job_dir,
                runtime_options=runtime_options
            )
            
            # Verify only string values are expanded
            expected_runtime_options = {
                "env": "HYSDS_ROOT_WORK_DIR=/data/work_1",  # expanded
                "gpus": 2,  # unchanged
                "memory": 1024,  # unchanged
                "label": ["workdir=$HYSDS_ROOT_WORK_DIR"]  # unchanged
            }
            
            self.assertEqual(
                docker_params["runtime_options"], 
                expected_runtime_options,
                "Only string values should be processed for environment variable expansion"
            )
