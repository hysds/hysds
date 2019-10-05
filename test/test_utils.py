import sys

try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
import unittest
import logging

# hysds.celery searches for configuration on import. So we need to make sure we
# mock it out before the first time it is imported
sys.modules['hysds.celery'] = umock.MagicMock()
logging.basicConfig()


class TestTriage(unittest.TestCase):

    def setUp(self):
        self.open_mock = umock.patch('hysds.utils.open', umock.mock_open()).start()
        self.makedirs_mock = umock.patch('os.makedirs').start()
        self.shutil_copy_mock = umock.patch('shutil.copy').start()
        self.publish_dataset_mock = umock.patch('hysds.utils.publish_dataset').start()

        self.addCleanup(umock.patch.stopall)

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
                'job_dir': "/test",
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
        self.publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = '/test/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json'
        expected_triage_met_filename = '/test/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json'
        expected_triage_json_filename = '/test/_triaged.json'

        # Test execution
        result = hysds.utils.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        self.open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        self.open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        self.open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_bad_custom_format_uses_default(self):
        import hysds.utils

        # Test case data
        job = {
            'task_id': "da9be25e-e281-4d3c-a7d8-e3c0c8342972",
            'job_info': {
                'id': "boogaloo",
                'status': 1,
                'job_dir': "/test",
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
        self.publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = '/test/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.dataset.json'
        expected_triage_met_filename = '/test/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972/triaged_job-boogaloo-da9be25e-e281-4d3c-a7d8-e3c0c8342972.met.json'
        expected_triage_json_filename = '/test/_triaged.json'

        # Test execution
        result = hysds.utils.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        self.open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        self.open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        self.open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)

    def test_triage_custom_triage_id(self):
        import hysds.utils

        # Test case data
        job = {
            'task_id': "da9be25e-e281-4d3c-a7d8-e3c0c8342972",
            'job_info': {
                'id': "boogaloo",
                'status': 1,
                'job_dir': "/test",
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
        self.publish_dataset_mock.return_value = {}

        # Expectations
        expected_triage_dataset_filename = '/test/boogaloo-0001-01-01T00:00:00.000Z/boogaloo-0001-01-01T00:00:00.000Z.dataset.json'
        expected_triage_met_filename = '/test/boogaloo-0001-01-01T00:00:00.000Z/boogaloo-0001-01-01T00:00:00.000Z.met.json'
        expected_triage_json_filename = '/test/_triaged.json'

        # Test execution
        result = hysds.utils.triage(job, job_context)

        # Assertions
        self.assertTrue(result)

        self.open_mock.assert_any_call(expected_triage_dataset_filename, umock.ANY)
        self.open_mock.assert_any_call(expected_triage_met_filename, umock.ANY)
        self.open_mock.assert_any_call(expected_triage_json_filename, umock.ANY)
