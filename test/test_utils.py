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

# hysds.celery searches for configuration on import. So we need to make sure we
# mock it out before the first time it is imported
sys.modules['hysds.celery'] = umock.MagicMock()
logging.basicConfig()


class TestTriage(unittest.TestCase):

    def setUp(self):
        self.job_dir = tempfile.mkdtemp(prefix="job-")
        logging.info("self.job_dir: {}".format(self.job_dir))

    def tearDown(self):
        umock.patch.stopall()
        shutil.rmtree(self.job_dir)

    def test_triage_job_succeeded(self):
        import hysds.utils
        job = {
            'job_info': {
                'status': 0
            }
        }

        self.assertTrue(hysds.utils.triage(job, None))

    def test_triage_disabled(self):
        import hysds.utils
        job = {
            'job_info': {
                'status': 1
            }
        }

        job_context = {
            '_triage_disabled': True
        }

        self.assertTrue(hysds.utils.triage(job, job_context))

    def test_triage_default_triage_id(self):
        import hysds.utils

        # Test case data
        job = {
            'task_id': "da9be25e-e281-4d3c-a7d8-e3c0c8342972",
            'job_info': {
                'id': "boogaloo",
                'status': 1,
                'job_dir': self.job_dir,
                'time_start': "0001-01-01T00:00:00.000Z",
                'context_file': "electric",
                'datasets_cfg_file': "more/configuration",
                'metrics': {
                    'product_provenance': dict(),
                    'products_staged': list()
                }
            }
        }

        job_context = {
            '_triage_disabled': False
        }

        # Mocked data
        open_mock = umock.patch('hysds.utils.open', umock.mock_open()).start()
        makedirs_mock = umock.patch('os.makedirs').start()
        shutil_copy_mock = umock.patch('shutil.copy').start()
        publish_dataset_mock = umock.patch('hysds.utils.publish_dataset').start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = self.job_dir + '/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json'
        expected_triage_met_filename = self.job_dir + '/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json'
        expected_triage_json_filename = self.job_dir + '/_triaged.json'

        # Test execution
        result = hysds.utils.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_bad_custom_format_uses_default(self):
        import hysds.utils

        # Test case data
        job = {
            'task_id': "da9be25e-e281-4d3c-a7d8-e3c0c8342972",
            'job_info': {
                'id': "boogaloo",
                'status': 1,
                'job_dir': self.job_dir,
                'time_start': "0001-01-01T00:00:00.000Z",
                'context_file': "electric",
                'datasets_cfg_file': "more/configuration",
                'metrics': {
                    'product_provenance': dict(),
                    'products_staged': list()
                }
            }
        }

        job_context = {
            '_triage_disabled': False,
            '_triage_id_format': '{job[job_info][id]-job[job_info][time_start]'  # Unmatched '}'
        }

        # Mocked data
        open_mock = umock.patch('hysds.utils.open', umock.mock_open()).start()
        makedirs_mock = umock.patch('os.makedirs').start()
        shutil_copy_mock = umock.patch('shutil.copy').start()
        publish_dataset_mock = umock.patch('hysds.utils.publish_dataset').start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = self.job_dir + '/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json'
        expected_triage_met_filename = self.job_dir + '/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json'
        expected_triage_json_filename = self.job_dir + '/_triaged.json'

        # Test execution
        result = hysds.utils.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_custom_triage_id(self):
        import hysds.utils

        # Test case data
        job = {
            'task_id': "da9be25e-e281-4d3c-a7d8-e3c0c8342972",
            'job_info': {
                'id': "boogaloo",
                'status': 1,
                'job_dir': self.job_dir,
                'time_start': "0001-01-01T00:00:00.000Z",
                'context_file': "electric",
                'datasets_cfg_file': "more/configuration",
                'metrics': {
                    'product_provenance': dict(),
                    'products_staged': list()
                }
            }
        }

        job_context = {
            '_triage_disabled': False,
            '_triage_id_format': '{job[job_info][id]}-{job[job_info][time_start]}'
        }

        # Mocked data
        open_mock = umock.patch('hysds.utils.open', umock.mock_open()).start()
        makedirs_mock = umock.patch('os.makedirs').start()
        shutil_copy_mock = umock.patch('shutil.copy').start()
        publish_dataset_mock = umock.patch('hysds.utils.publish_dataset').start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = self.job_dir + '/boogaloo-0001-01-01T00:00:00.000Z/boogaloo-0001-01-01T00:00:00.000Z.dataset.json'
        expected_triage_met_filename = self.job_dir + '/boogaloo-0001-01-01T00:00:00.000Z/boogaloo-0001-01-01T00:00:00.000Z.met.json'
        expected_triage_json_filename = self.job_dir + '/_triaged.json'

        # Test execution
        result = hysds.utils.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_no_time_start(self):
        import hysds.utils

        # Test case data
        job = {
            'task_id': "da9be25e-e281-4d3c-a7d8-e3c0c8342972",
            'job_info': {
                'id': "boogaloo",
                'status': 1,
                'job_dir': self.job_dir,
                'context_file': "electric",
                'datasets_cfg_file': "more/configuration",
                'metrics': {
                    'product_provenance': dict(),
                    'products_staged': list()
                }
            }
        }

        job_context = {
            '_triage_disabled': False
        }

        # Mocked data
        open_mock = umock.patch('hysds.utils.open', umock.mock_open()).start()
        makedirs_mock = umock.patch('os.makedirs').start()
        shutil_copy_mock = umock.patch('shutil.copy').start()
        ingest_mock = umock.patch('hysds.dataset_ingest.ingest').start()
        ingest_mock.return_value = (umock.MagicMock(), umock.MagicMock())
        json_dump_mock = umock.patch('json.dump').start()
        json_dump_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = self.job_dir + '/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json'
        expected_triage_met_filename = self.job_dir + '/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json'
        expected_triage_json_filename = self.job_dir + '/_triaged.json'

        # Test execution
        result = hysds.utils.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_dataset_generation(self):
        import hysds.utils

        # Test case data
        job = {
            'task_id': "da9be25e-e281-4d3c-a7d8-e3c0c8342972",
            'job_info': {
                'id': "boogaloo",
                'status': 1,
                'job_dir': self.job_dir,
                'time_start': "0001-01-01T00:00:00.000Z",
                'context_file': "electric",
                'datasets_cfg_file': "more/configuration",
                'metrics': {
                    'product_provenance': dict(),
                    'products_staged': list()
                }
            }
        }

        job_context = {
            '_triage_disabled': False,
        }

        # create job and context json
        with open(os.path.join(self.job_dir, '_job.json'), 'w') as f:
            json.dump(job, f)
        with open(os.path.join(self.job_dir, '_context.json'), 'w') as f:
            json.dump(job_context, f)

        # create directory starting with __
        pycache_dir = os.path.join(self.job_dir, "__pycache__")
        os.makedirs(pycache_dir)
        with open(os.path.join(pycache_dir, 'test.json'), 'w') as f:
            f.write("{}")

        # Mocked data
        publish_dataset_mock = umock.patch('hysds.utils.publish_dataset').start()
        publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = self.job_dir + '/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json'
        expected_triage_met_filename = self.job_dir + '/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json'
        expected_triage_json_filename = self.job_dir + '/_triaged.json'

        # Test execution
        result = hysds.utils.triage(job, job_context)

        # Assertions
        self.assertTrue(result)
        self.assertTrue(os.path.exists(expected_triage_dataset_filename))
        self.assertTrue(os.path.exists(expected_triage_met_filename))