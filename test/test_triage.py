import json
import sys

try:
    import unittest.mock as umock
except ImportError:
    from unittest import mock as umock

import glob
import logging
import os
import shutil
import tempfile
import unittest

# hysds.celery searches for configuration on import. So we need to make sure we
# mock it out before the first time it is imported
sys.modules["hysds.celery"] = umock.MagicMock()
sys.modules["opensearchpy"] = umock.Mock()
sys.modules["opensearchpy.exceptions"] = umock.Mock()
logging.basicConfig()


class TestTriage(unittest.TestCase):
    def setUp(self):
        self.job_dir = tempfile.mkdtemp(prefix="job-")
        logging.info(f"self.job_dir: {self.job_dir}")

    def tearDown(self):
        umock.patch.stopall()
        shutil.rmtree(self.job_dir)

    def test_triage_job_succeeded(self):
        import hysds.triage

        job = {"job_info": {"status": 0}}
        self.assertTrue(hysds.triage.triage(job, None))

    def test_triage_disabled(self):
        import hysds.triage

        job = {"job_info": {"status": 1}}
        job_context = {"_triage_disabled": True}
        self.assertTrue(hysds.triage.triage(job, job_context))

    def test_triage_default_triage_id(self):
        import hysds.triage

        # Test case data
        _id = "da9be25e-e281-4d3c-a7d8-e3c0c8342972"
        triage_name = f"triaged_job-boogaloo_task-{_id}"
        job = {
            "task_id": _id,
            "job_info": {
                "id": "boogaloo",
                "status": 1,
                "job_dir": self.job_dir,
                "time_start": "0001-01-01T00:00:00.000Z",
                "context_file": "electric",
                "datasets_cfg_file": "more/configuration",
                "metrics": {"product_provenance": dict(), "products_staged": list()},
            },
        }

        job_context = {"_triage_disabled": False}

        # Mocked data
        open_mock = umock.patch("hysds.triage.open", umock.mock_open()).start()
        publish_dataset_mock = umock.patch(
            "hysds.dataset_ingest.publish_dataset"
        ).start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            self.job_dir + f"/{triage_name}/{triage_name}.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir + f"/{triage_name}/{triage_name}.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Mock the return value of get_triage_partition_format()
        triage_partition_format_mock = umock.patch(
            "hysds.triage.get_triage_partition_format"
        ).start()
        triage_partition_format_mock.return_value = {}

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    @umock.patch("hysds.dataset_ingest.publish_dataset")
    def test_triage_bad_custom_format_uses_default(self, publish_dataset_mock):
        import hysds.triage

        # Test case data
        _id = "da9be25e-e281-4d3c-a7d8-e3c0c8342972"
        triage_name = f"triaged_job-boogaloo_task-{_id}"
        job = {
            "task_id": _id,
            "job_info": {
                "id": "boogaloo",
                "status": 1,
                "job_dir": self.job_dir,
                "time_start": "0001-01-01T00:00:00.000Z",
                "context_file": "electric",
                "datasets_cfg_file": "more/configuration",
                "metrics": {"product_provenance": dict(), "products_staged": list()},
            },
        }

        job_context = {
            "_triage_disabled": False,
            "_triage_id_format": "{job[job_info][id]-job[job_info][time_start]",  # Unmatched '}'
        }

        # Mocked data
        open_mock = umock.patch("hysds.triage.open", umock.mock_open()).start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            self.job_dir + f"/{triage_name}/{triage_name}.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir + f"/{triage_name}/{triage_name}.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Mock the return value of get_triage_partition_format()
        triage_partition_format_mock = umock.patch(
            "hysds.triage.get_triage_partition_format"
        ).start()
        triage_partition_format_mock.return_value = {}

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    @umock.patch("hysds.dataset_ingest.publish_dataset")
    def test_triage_custom_triage_id(self, publish_dataset_mock):
        import hysds.triage

        # Test case data
        _id = "da9be25e-e281-4d3c-a7d8-e3c0c8342972"
        job = {
            "task_id": _id,
            "job_info": {
                "id": "boogaloo",
                "status": 1,
                "job_dir": self.job_dir,
                "time_start": "0001-01-01T00:00:00.000Z",
                "context_file": "electric",
                "datasets_cfg_file": "more/configuration",
                "metrics": {"product_provenance": dict(), "products_staged": list()},
            },
        }

        job_context = {
            "_triage_disabled": False,
            "_triage_id_format": "{job[job_info][id]}-{job[job_info][time_start]}",
        }

        # Mocked data
        open_mock = umock.patch("hysds.triage.open", umock.mock_open()).start()
        publish_dataset_mock.return_value = {}

        # Expectations
        boogaloo_ts = "boogaloo-0001-01-01T00:00:00.000Z"
        expected_triage_dataset_filename = (
            self.job_dir + f"/{boogaloo_ts}/{boogaloo_ts}.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir + f"/{boogaloo_ts}/{boogaloo_ts}.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Mock the return value of get_triage_partition_format()
        triage_partition_format_mock = umock.patch(
            "hysds.triage.get_triage_partition_format"
        ).start()
        triage_partition_format_mock.return_value = {}

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_no_time_start(self):
        import hysds.triage

        # Test case data
        _id = "da9be25e-e281-4d3c-a7d8-e3c0c8342972"
        triage_name = f"triaged_job-boogaloo_task-{_id}"
        job = {
            "task_id": _id,
            "job_info": {
                "id": "boogaloo",
                "status": 1,
                "job_dir": self.job_dir,
                "context_file": "electric",
                "datasets_cfg_file": "more/configuration",
                "metrics": {"product_provenance": dict(), "products_staged": list()},
            },
        }

        job_context = {"_triage_disabled": False}

        # Mocked data
        open_mock = umock.patch("hysds.triage.open", umock.mock_open()).start()
        json_dump_mock = umock.patch("json.dump").start()
        json_dump_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            self.job_dir + f"/{triage_name}/{triage_name}.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir + f"/{triage_name}/{triage_name}.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Mock the return value of get_triage_partition_format()
        triage_partition_format_mock = umock.patch(
            "hysds.triage.get_triage_partition_format"
        ).start()
        triage_partition_format_mock.return_value = {}

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_dataset_generation(self):
        import hysds.triage

        # Test case data
        _id = "da9be25e-e281-4d3c-a7d8-e3c0c8342972"
        triage_name = f"triaged_job-boogaloo_task-{_id}"
        job = {
            "task_id": _id,
            "job_info": {
                "id": "boogaloo",
                "status": 1,
                "job_dir": self.job_dir,
                "time_start": "0001-01-01T00:00:00.000Z",
                "context_file": "electric",
                "datasets_cfg_file": "more/configuration",
                "metrics": {"product_provenance": dict(), "products_staged": list()},
            },
        }

        job_context = {
            "_triage_disabled": False,
        }

        # create job and context json
        with open(os.path.join(self.job_dir, "_job.json"), "w") as f:
            json.dump(job, f)
        with open(os.path.join(self.job_dir, "_context.json"), "w") as f:
            json.dump(job_context, f)

        # create directory starting with __
        pycache_dir = os.path.join(self.job_dir, "__pycache__")
        os.makedirs(pycache_dir)
        with open(os.path.join(pycache_dir, "test.json"), "w") as f:
            f.write("{}")

        # Mocked data
        publish_dataset_mock = umock.patch(
            "hysds.dataset_ingest.publish_dataset"
        ).start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            self.job_dir + f"/{triage_name}/{triage_name}.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir + f"/{triage_name}/{triage_name}.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Mock the return value of get_triage_partition_format()
        triage_partition_format_mock = umock.patch(
            "hysds.triage.get_triage_partition_format"
        ).start()
        triage_partition_format_mock.return_value = {}

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)
        self.assertTrue(os.path.exists(expected_triage_dataset_filename))
        self.assertTrue(os.path.exists(expected_triage_met_filename))
        self.assertTrue(os.path.exists(expected_triage_json_filename))

    def test_triage_overlap(self):
        import hysds.triage

        # Test case data
        _id = "da9be25e-e281-4d3c-a7d8-e3c0c8342972"
        triage_name = f"triaged_job-boogaloo_task-{_id}"
        job = {
            "task_id": _id,
            "job_info": {
                "id": "boogaloo",
                "status": 1,
                "job_dir": self.job_dir,
                "time_start": "0001-01-01T00:00:00.000Z",
                "context_file": "electric",
                "datasets_cfg_file": "more/configuration",
                "metrics": {"product_provenance": dict(), "products_staged": list()},
            },
        }

        job_context = {
            "_triage_disabled": False,
            "_triage_additional_globs": ["sub_dir/", "sub_dir2/*.log"],
        }

        # create job and context json
        with open(os.path.join(self.job_dir, "_job.json"), "w") as f:
            json.dump(job, f)
        with open(os.path.join(self.job_dir, "_context.json"), "w") as f:
            json.dump(job_context, f)

        # create log file
        log_file = os.path.join(self.job_dir, "test.log")
        with open(log_file, "w") as f:
            f.write("this is a log line")

        # create subdirectory with same log file name
        sub_dir = os.path.join(self.job_dir, "sub_dir")
        os.makedirs(sub_dir)
        with open(os.path.join(sub_dir, "test.log"), "w") as f:
            f.write("this is a sub_dir log line")

        # create another subdirectory with same log file name
        sub_dir2 = os.path.join(self.job_dir, "sub_dir2")
        os.makedirs(sub_dir2)
        with open(os.path.join(sub_dir2, "test.log"), "w") as f:
            f.write("this is a sub_dir2 log line")

        # Mocked data
        publish_dataset_mock = umock.patch(
            "hysds.dataset_ingest.publish_dataset"
        ).start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset = self.job_dir + f"/{triage_name}"
        expected_triage_dataset_filename = (
            expected_triage_dataset + f"/{triage_name}.dataset.json"
        )
        expected_triage_met_filename = (
            expected_triage_dataset + f"/{triage_name}.met.json"
        )

        expected_triage_json_filename = self.job_dir + "/_triaged.json"
        expected_triage_log_file1 = expected_triage_dataset + "/test.log"
        expected_triage_log_file2 = expected_triage_dataset + "/sub_dir/test.log"

        # Mock the return value of get_triage_partition_format()
        triage_partition_format_mock = umock.patch(
            "hysds.triage.get_triage_partition_format"
        ).start()
        triage_partition_format_mock.return_value = {}

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)
        self.assertTrue(os.path.exists(expected_triage_dataset_filename))
        self.assertTrue(os.path.exists(expected_triage_met_filename))
        self.assertTrue(os.path.exists(expected_triage_json_filename))
        self.assertTrue(os.path.exists(expected_triage_log_file1))
        self.assertTrue(os.path.exists(expected_triage_log_file2))
        self.assertTrue(len(glob.glob(f"{expected_triage_dataset}/test.log*")) == 2)

    def test_triage_on_triage_dataset(self):
        import hysds.triage

        # Test case data
        _id = "da9be25e-e281-4d3c-a7d8-e3c0c8342972"
        triage_name = f"triaged_job-boogaloo_task-{_id}"
        job = {
            "task_id": _id,
            "job_info": {
                "id": "triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972",
                "status": 1,
                "job_dir": self.job_dir,
                "time_start": "0001-01-01T00:00:00.000Z",
                "context_file": "electric",
                "datasets_cfg_file": "more/configuration",
                "metrics": {"product_provenance": dict(), "products_staged": list()},
            },
        }

        job_context = {"_triage_disabled": False}

        # Mocked data
        open_mock = umock.patch("hysds.triage.open", umock.mock_open()).start()
        publish_dataset_mock = umock.patch(
            "hysds.dataset_ingest.publish_dataset"
        ).start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            self.job_dir + f"/{triage_name}/{triage_name}.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir + f"/{triage_name}/{triage_name}.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Mock the return value of get_triage_partition_format()
        triage_partition_format_mock = umock.patch(
            "hysds.triage.get_triage_partition_format"
        ).start()
        triage_partition_format_mock.return_value = {}

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_invalid_path(self):
        import hysds.triage

        # Test case data
        _id = "da9be25e-e281-4d3c-a7d8-e3c0c8342972"
        triage_name = f"triaged_job-boogaloo_task-{_id}"
        job = {
            "task_id": _id,
            "job_info": {
                "id": "boogaloo",
                "status": 1,
                "job_dir": self.job_dir,
                "time_start": "0001-01-01T00:00:00.000Z",
                "context_file": "electric",
                "datasets_cfg_file": "more/configuration",
                "metrics": {"product_provenance": dict(), "products_staged": list()},
            },
        }

        job_context = {
            "_triage_disabled": False,
            "_triage_additional_globs": ["sub_dir", "sub_dir2/"],
        }

        # create job and context json
        with open(os.path.join(self.job_dir, "_job.json"), "w") as f:
            json.dump(job, f)
        with open(os.path.join(self.job_dir, "_context.json"), "w") as f:
            json.dump(job_context, f)

        # create log file
        log_file = os.path.join(self.job_dir, "test.log")
        with open(log_file, "w") as f:
            f.write("this is a log line")

        # create subdirectory with non-existent referent
        sub_dir = os.path.join(self.job_dir, "sub_dir")
        os.symlink("/some_non_existent_dir", sub_dir)

        # create another subdirectory with inaccessible permissions
        sub_dir2 = os.path.join(self.job_dir, "sub_dir2")
        os.makedirs(sub_dir2)
        os.chmod(sub_dir2, 0o000)

        # Mocked data
        publish_dataset_mock = umock.patch(
            "hysds.dataset_ingest.publish_dataset"
        ).start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset = self.job_dir + f"/{triage_name}"
        expected_triage_dataset_filename = (
            expected_triage_dataset + f"/{triage_name}.dataset.json"
        )
        expected_triage_met_filename = (
            expected_triage_dataset + f"/{triage_name}.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"
        expected_triage_log_file1 = expected_triage_dataset + "/test.log"
        expected_triage_subdir1 = expected_triage_dataset + "/sub_dir"
        expected_triage_subdir2 = expected_triage_dataset + "/sub_dir2"

        # Mock the return value of get_triage_partition_format()
        triage_partition_format_mock = umock.patch(
            "hysds.triage.get_triage_partition_format"
        ).start()
        triage_partition_format_mock.return_value = {}

        # Test execution
        try:
            result = hysds.triage.triage(job, job_context)
        finally:
            os.chmod(sub_dir2, 0o755)

        # Assertions
        self.assertTrue(result)
        self.assertTrue(os.path.exists(expected_triage_dataset_filename))
        self.assertTrue(os.path.exists(expected_triage_met_filename))
        self.assertTrue(os.path.exists(expected_triage_json_filename))
        self.assertTrue(os.path.exists(expected_triage_log_file1))
        self.assertFalse(os.path.exists(expected_triage_subdir1))
        self.assertTrue(os.path.exists(expected_triage_subdir2))
