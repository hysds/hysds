import os
import sys
import json

try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
import unittest
import logging
import tempfile
import shutil
import glob

# hysds.celery searches for configuration on import. So we need to make sure we
# mock it out before the first time it is imported
sys.modules["hysds.celery"] = umock.MagicMock()
logging.basicConfig()


class TestTriage(unittest.TestCase):
    def setUp(self):
        self.job_dir = tempfile.mkdtemp(prefix="job-")
        logging.info("self.job_dir: {}".format(self.job_dir))

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
        # import hysds.utils
        # import hysds.dataset_ingest
        import hysds.triage

        job_dir = tempfile.mkdtemp(prefix="job-")

        # Test case data
        job = {
            "task_id": "da9be25e-e281-4d3c-a7d8-e3c0c8342972",
            "job_info": {
                "id": "boogaloo",
                "status": 1,
                "job_dir": job_dir,
                "time_start": "0001-01-01T00:00:00.000Z",
                "context_file": "electric",
                "datasets_cfg_file": "more/configuration",
                "metrics": {"product_provenance": dict(), "products_staged": list()},
            },
        }

        job_context = {"_triage_disabled": False}

        # Mocked data
        open_mock = umock.patch("hysds.utils.open", umock.mock_open()).start()
        makedirs_mock = umock.patch("os.makedirs").start()
        shutil_copy_mock = umock.patch("shutil.copy").start()
        publish_dataset_mock = umock.patch("hysds.dataset_ingest.publish_dataset").start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            job_dir
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json"
        )
        expected_triage_met_filename = (
            job_dir
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json"
        )
        expected_triage_json_filename = job_dir + "/_triaged.json"

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_bad_custom_format_uses_default(self):
        # import hysds.utils
        # import hysds.dataset_ingest
        import hysds.triage

        # Test case data
        job = {
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
        }

        job_context = {
            "_triage_disabled": False,
            "_triage_id_format": "{job[job_info][id]-job[job_info][time_start]",  # Unmatched '}'
        }

        # Mocked data
        open_mock = umock.patch("hysds.utils.open", umock.mock_open()).start()
        makedirs_mock = umock.patch("os.makedirs").start()
        shutil_copy_mock = umock.patch("shutil.copy").start()
        publish_dataset_mock = umock.patch("hysds.dataset_ingest.publish_dataset").start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            self.job_dir
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_custom_triage_id(self):
        # import hysds.utils
        # import hysds.dataset_ingest
        import hysds.triage

        # Test case data
        job = {
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
        }

        job_context = {
            "_triage_disabled": False,
            "_triage_id_format": "{job[job_info][id]}-{job[job_info][time_start]}",
        }

        # Mocked data
        open_mock = umock.patch("hysds.utils.open", umock.mock_open()).start()
        makedirs_mock = umock.patch("os.makedirs").start()
        shutil_copy_mock = umock.patch("shutil.copy").start()
        publish_dataset_mock = umock.patch("hysds.dataset_ingest.publish_dataset").start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            self.job_dir
            + "/boogaloo-0001-01-01T00:00:00.000Z/boogaloo-0001-01-01T00:00:00.000Z.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir
            + "/boogaloo-0001-01-01T00:00:00.000Z/boogaloo-0001-01-01T00:00:00.000Z.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_no_time_start(self):
        # import hysds.utils
        # import hysds.dataset_ingest
        import hysds.triage

        # Test case data
        job = {
            "task_id": "da9be25e-e281-4d3c-a7d8-e3c0c8342972",
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
        open_mock = umock.patch("hysds.utils.open", umock.mock_open()).start()
        makedirs_mock = umock.patch("os.makedirs").start()
        shutil_copy_mock = umock.patch("shutil.copy").start()
        ingest_mock = umock.patch("hysds.dataset_ingest.ingest").start()
        ingest_mock.return_value = (umock.MagicMock(), umock.MagicMock())
        json_dump_mock = umock.patch("json.dump").start()
        json_dump_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            self.job_dir
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_dataset_generation(self):
        # import hysds.utils
        # import hysds.dataset_ingest
        import hysds.triage

        # Test case data
        job = {
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
        publish_dataset_mock = umock.patch("hysds.dataset_ingest.publish_dataset").start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            self.job_dir
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)
        self.assertTrue(os.path.exists(expected_triage_dataset_filename))
        self.assertTrue(os.path.exists(expected_triage_met_filename))
        self.assertTrue(os.path.exists(expected_triage_json_filename))

    def test_triage_overlap(self):
        # import hysds.utils
        # import hysds.dataset_ingest
        import hysds.triage

        # Test case data
        job = {
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
        publish_dataset_mock = umock.patch("hysds.dataset_ingest.publish_dataset").start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset = (
            self.job_dir + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972"
        )
        expected_triage_dataset_filename = (
            expected_triage_dataset
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json"
        )
        expected_triage_met_filename = (
            expected_triage_dataset
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"
        expected_triage_log_file1 = expected_triage_dataset + "/test.log"
        expected_triage_log_file2 = expected_triage_dataset + "/sub_dir/test.log"

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)
        self.assertTrue(os.path.exists(expected_triage_dataset_filename))
        self.assertTrue(os.path.exists(expected_triage_met_filename))
        self.assertTrue(os.path.exists(expected_triage_json_filename))
        self.assertTrue(os.path.exists(expected_triage_log_file1))
        self.assertTrue(os.path.exists(expected_triage_log_file2))
        self.assertTrue(
            len(glob.glob("{}/test.log*".format(expected_triage_dataset))) == 2
        )

    def test_triage_on_triage_dataset(self):
        # import hysds.utils
        # import hysds.dataset_ingest
        import hysds.triage

        # Test case data
        job = {
            "task_id": "2a9be25e-e281-4d3c-a7d8-e3c0c8342971",
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
        open_mock = umock.patch("hysds.utils.open", umock.mock_open()).start()
        makedirs_mock = umock.patch("os.makedirs").start()
        shutil_copy_mock = umock.patch("shutil.copy").start()
        publish_dataset_mock = umock.patch("hysds.dataset_ingest.publish_dataset").start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = (
            self.job_dir
            + "/triaged_job-boogaloo_task-2a9be25e-e281-4d3c-a7d8-e3c0c8342971/triaged_job-boogaloo_task-2a9be25e-e281-4d3c-a7d8-e3c0c8342971.dataset.json"
        )
        expected_triage_met_filename = (
            self.job_dir
            + "/triaged_job-boogaloo_task-2a9be25e-e281-4d3c-a7d8-e3c0c8342971/triaged_job-boogaloo_task-2a9be25e-e281-4d3c-a7d8-e3c0c8342971.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"

        # Test execution
        result = hysds.triage.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_invalid_path(self):
        # import hysds.utils
        # import hysds.dataset_ingest
        import hysds.triage

        # Test case data
        job = {
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
        os.symlink('/some_non_existent_dir', sub_dir)

        # create another subdirectory with inaccessible permissions
        sub_dir2 = os.path.join(self.job_dir, "sub_dir2")
        os.makedirs(sub_dir2)
        os.chmod(sub_dir2, 0o000)

        # Mocked data
        publish_dataset_mock = umock.patch("hysds.dataset_ingest.publish_dataset").start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset = (
            self.job_dir + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972"
        )
        expected_triage_dataset_filename = (
            expected_triage_dataset
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json"
        )
        expected_triage_met_filename = (
            expected_triage_dataset
            + "/triaged_job-boogaloo_task-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json"
        )
        expected_triage_json_filename = self.job_dir + "/_triaged.json"
        expected_triage_log_file1 = expected_triage_dataset + "/test.log"
        expected_triage_subdir1 = expected_triage_dataset + "/sub_dir"
        expected_triage_subdir2 = expected_triage_dataset + "/sub_dir2"

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
        self.assertFalse(os.path.exists(expected_triage_subdir2))


class TestUtils(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="tmp-")
        logging.info("self.tmp_dir: {}".format(self.tmp_dir))

    def tearDown(self):
        umock.patch.stopall()
        shutil.rmtree(self.tmp_dir)

    def get_disk_usage(self, path):
        """Return disk usage in bytes. Equivalent to `du -sbL`."""

        if os.path.islink(path):
            return (os.lstat(path).st_size, 0)
        if os.path.isfile(path):
            st = os.lstat(path)
            return (st.st_size, st.st_blocks * 512)
        apparent_total_bytes = 0
        total_bytes = 0
        have = []
        for dirpath, dirnames, filenames in os.walk(path, followlinks=True):
            apparent_total_bytes += os.lstat(dirpath).st_size
            total_bytes += os.lstat(dirpath).st_blocks * 512
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if os.path.islink(fp):
                    apparent_total_bytes += os.lstat(os.path.realpath(fp)).st_size
                    continue
                st = os.lstat(fp)
                if st.st_ino in have:
                    continue  # skip hardlinks which were already counted
                have.append(st.st_ino)
                apparent_total_bytes += st.st_size
                total_bytes += st.st_blocks * 512
            for d in dirnames:
                dp = os.path.join(dirpath, d)
                if os.path.islink(dp):
                    apparent_total_bytes += os.lstat(dp).st_size
        return apparent_total_bytes

    def test_disk_usage(self):
        import hysds.utils

        size_bytes = 1024 * 1024  # 1 MB
        with open(os.path.join(self.tmp_dir, "test.bin"), "wb") as f:
            f.write(os.urandom(size_bytes))
        size = hysds.utils.get_disk_usage(self.tmp_dir)
        self.assertTrue(size == self.get_disk_usage(self.tmp_dir))

    def test_disk_usage_with_symlink(self):
        import hysds.utils

        size_bytes = 1024 * 1024  # 1 MB
        bin_file = os.path.join(self.tmp_dir, "test.bin")
        with open(bin_file, "wb") as f:
            f.write(os.urandom(size_bytes))
        self.tmp_dir2 = tempfile.mkdtemp(prefix="tmp-")
        sym_file = os.path.join(self.tmp_dir2, "test.bin")
        os.symlink(bin_file, sym_file)
        size = hysds.utils.get_disk_usage(self.tmp_dir2)
        self.assertTrue(size == self.get_disk_usage(self.tmp_dir2))
        shutil.rmtree(self.tmp_dir2)


class TestPublishDataset(unittest.TestCase):
    def setUp(self):
        self.job_dir = tempfile.mkdtemp(prefix="job-")
        logging.info("self.job_dir: {}".format(self.job_dir))

        self.datasets_cfg_file = os.path.join(self.job_dir, 'datasets.json')
        shutil.copy(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                '..', 'configs', 'datasets', 'datasets.json'),
            self.datasets_cfg_file
        )

        self.examples_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "examples"
        )
        self.prod_dir = os.path.join(self.job_dir, "AOI_sacramento_valley")
        shutil.copytree(
            os.path.join(self.examples_dir, "AOI_sacramento_valley"),
            self.prod_dir
        )

        self.job = {
            "job_info": {
                "status": 0,
                "pid": 123,
                "job_dir": self.job_dir,
                "time_start": "1976-12-17T00:00:00.000Z",
                "datasets_cfg_file": self.datasets_cfg_file,
                "metrics": {
                    "product_provenance": {},
                    "products_staged": [],
                }
            }
        }

        self.metrics = {
            "ipath": "my_project::data/area_of_interest"
        }

        self.prod_json = {
            "id": "AOI_sacramento_valley",
            "urls": [
                "https://mydav.example.com/repository/products/area_of_interest/2.0/AOI_sacramento_valley"
            ],
            "browse_urls": [
                "http://mydav.example.com/public/browse/area_of_interest/2.0/AOI_sacramento_valley"
            ],
            "dataset": "area_of_interest",
            "ipath": "my_project::data/area_of_interest",
            "system_version": "2.0",
            "dataset_level": "",
            "dataset_type": "",
            "grq_index_result": {
                "index": "grq_v01_area_of_interest"
            }
        }

    def tearDown(self):
        umock.patch.stopall()
        shutil.rmtree(self.job_dir)

    def test_publish_dataset(self):
        import hysds.dataset_ingest

        job_context = {}
        job_context_file = os.path.join(self.job_dir, "_context.json")
        with open(job_context_file, 'w') as f:
            json.dump(job_context, f, indent=2, sort_keys=True)
        self.job["job_info"]["context_file"] = job_context_file

        # Mocked data
        ingest_mock = umock.patch("hysds.dataset_ingest.ingest").start()
        ingest_mock.return_value = (self.metrics, self.prod_json)

        self.assertTrue(hysds.dataset_ingest.publish_datasets(self.job, job_context))

        # assert called args
        ingest_mock.assert_called_with(
            'AOI_sacramento_valley',
            self.datasets_cfg_file,
            umock.ANY,
            umock.ANY,
            self.prod_dir,
            self.job_dir,
            force=False
        )

    def test_force_ingest(self):
        import hysds.dataset_ingest

        job_context = {'_force_ingest': True}
        job_context_file = os.path.join(self.job_dir, "_context.json")
        with open(job_context_file, 'w') as f:
            json.dump(job_context, f, indent=2, sort_keys=True)
        self.job["job_info"]["context_file"] = job_context_file

        # Mocked data
        ingest_mock = umock.patch("hysds.dataset_ingest.ingest").start()
        ingest_mock.return_value = (self.metrics, self.prod_json)

        self.assertTrue(hysds.dataset_ingest.publish_datasets(self.job, job_context))

        # assert that ingest function was called with force=True
        ingest_mock.assert_called_with(
            'AOI_sacramento_valley',
            self.datasets_cfg_file,
            umock.ANY,
            umock.ANY,
            self.prod_dir,
            self.job_dir,
            force=True
        )
