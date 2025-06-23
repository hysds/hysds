import json
import sys

try:
    import unittest.mock as umock
except ImportError:
    from unittest import mock as umock

import logging
import os
import shutil
import tempfile
from unittest import TestCase

# hysds.celery searches for configuration on import. So we need to make sure we
# mock it out before the first time it is imported
sys.modules["hysds.celery"] = umock.MagicMock()
sys.modules["opensearchpy"] = umock.Mock()
logging.basicConfig()


class TestUtils(TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="tmp-")
        logging.info(f"self.tmp_dir: {self.tmp_dir}")

    def tearDown(self):
        umock.patch.stopall()
        shutil.rmtree(self.tmp_dir)

    def get_disk_usage(self, path):
        """Return disk usage in bytes. Equivalent to `du -sbL`."""

        if os.path.islink(path):
            return os.lstat(path).st_size, 0
        if os.path.isfile(path):
            st = os.lstat(path)
            return st.st_size, st.st_blocks * 512
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


class TestPublishDataset(TestCase):
    def setUp(self):
        self.job_dir = tempfile.mkdtemp(prefix="job-")
        logging.info(f"self.job_dir: {self.job_dir}")

        self.datasets_cfg_file = os.path.join(self.job_dir, "datasets.json")
        shutil.copy(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..",
                "configs",
                "datasets",
                "datasets.json",
            ),
            self.datasets_cfg_file,
        )

        self.examples_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "examples"
        )
        self.prod_dir = os.path.join(self.job_dir, "AOI_sacramento_valley")
        shutil.copytree(
            os.path.join(self.examples_dir, "AOI_sacramento_valley"), self.prod_dir
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
                },
            }
        }

        self.metrics = {"ipath": "my_project::data/area_of_interest"}

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
            "grq_index_result": {"index": "grq_v01_area_of_interest"},
        }

    def tearDown(self):
        umock.patch.stopall()
        shutil.rmtree(self.job_dir)

    def test_publish_dataset(self):
        import hysds.dataset_ingest

        job_context = {}
        job_context_file = os.path.join(self.job_dir, "_context.json")
        with open(job_context_file, "w") as f:
            json.dump(job_context, f, indent=2, sort_keys=True)
        self.job["job_info"]["context_file"] = job_context_file

        # Mocked data
        ingest_mock = umock.patch("hysds.dataset_ingest.ingest").start()
        ingest_mock.return_value = (self.metrics, self.prod_json)

        self.assertTrue(hysds.dataset_ingest.publish_datasets(self.job, job_context))

        # assert called args
        ingest_mock.assert_called_with(
            "AOI_sacramento_valley",
            self.datasets_cfg_file,
            umock.ANY,
            umock.ANY,
            self.prod_dir,
            self.job_dir,
            force=False,
        )

    def test_force_ingest(self):
        import hysds.dataset_ingest

        job_context = {"_force_ingest": True}
        job_context_file = os.path.join(self.job_dir, "_context.json")
        with open(job_context_file, "w") as f:
            json.dump(job_context, f, indent=2, sort_keys=True)
        self.job["job_info"]["context_file"] = job_context_file

        # Mocked data
        ingest_mock = umock.patch("hysds.dataset_ingest.ingest").start()
        ingest_mock.return_value = (self.metrics, self.prod_json)

        self.assertTrue(hysds.dataset_ingest.publish_datasets(self.job, job_context))

        # assert that ingest function was called with force=True
        ingest_mock.assert_called_with(
            "AOI_sacramento_valley",
            self.datasets_cfg_file,
            umock.ANY,
            umock.ANY,
            self.prod_dir,
            self.job_dir,
            force=True,
        )
