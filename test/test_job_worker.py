import os
import sys

try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
from unittest import TestCase
from unittest.mock import patch
import logging
import tempfile
import shutil

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
        logging.info("self.job_dir: {}".format(self.job_dir))

    def tearDown(self):
        umock.patch.stopall()
        shutil.rmtree(self.job_dir)

    def test_find_usage_stats(self):
        with patch("elasticsearch.Elasticsearch", return_value=None), patch("opensearchpy.OpenSearch", return_value=None):
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
