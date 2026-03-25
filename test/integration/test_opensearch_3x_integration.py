import unittest
import os
import time
from unittest.mock import patch, Mock
from opensearchpy import OpenSearch
from opensearchpy.exceptions import ConnectionError, NotFoundError


class TestOpenSearch3xIntegration(unittest.TestCase):
    """Integration tests for OpenSearch 3.x compatibility."""

    @classmethod
    def setUpClass(cls):
        """Set up test class with OpenSearch connection."""
        cls.os_url = os.environ.get('OPENSEARCH_URL', 'https://localhost:9200')
        cls.os_user = os.environ.get('OPENSEARCH_USER', 'admin')
        cls.os_password = os.environ.get('OPENSEARCH_PASSWORD', 'admin')
        
        cls.client = OpenSearch(
            hosts=[cls.os_url],
            http_auth=(cls.os_user, cls.os_password),
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False
        )
        
        cls.test_index = 'test-opensearch-3x-integration'
        
    def setUp(self):
        """Set up test fixtures."""
        try:
            if self.client.indices.exists(index=self.test_index):
                self.client.indices.delete(index=self.test_index)
        except Exception:
            pass
            
    def tearDown(self):
        """Clean up test fixtures."""
        try:
            if self.client.indices.exists(index=self.test_index):
                self.client.indices.delete(index=self.test_index)
        except Exception:
            pass
    
    def test_index_document_without_doc_type(self):
        """Test indexing a document without doc_type parameter."""
        doc = {
            "job_id": "test-job-1",
            "status": "job-started",
            "timestamp": "2024-01-01T00:00:00Z"
        }
        
        response = self.client.index(
            index=self.test_index,
            id="test-job-1",
            body=doc
        )
        
        self.assertEqual(response['result'], 'created')
        self.assertNotIn('_type', response)
        
    def test_search_without_doc_type(self):
        """Test searching without doc_type parameter."""
        doc = {
            "job_id": "test-job-2",
            "status": "job-completed",
            "timestamp": "2024-01-01T00:00:00Z"
        }
        
        self.client.index(
            index=self.test_index,
            id="test-job-2",
            body=doc,
            refresh=True
        )
        
        query = {
            "query": {
                "match": {
                    "status": "job-completed"
                }
            }
        }
        
        response = self.client.search(
            index=self.test_index,
            body=query
        )
        
        self.assertEqual(response['hits']['total']['value'], 1)
        self.assertEqual(response['hits']['hits'][0]['_source']['job_id'], 'test-job-2')
        
    def test_bulk_operations_without_type(self):
        """Test bulk operations without _type field."""
        actions = []
        for i in range(5):
            actions.append({"index": {"_index": self.test_index, "_id": f"bulk-{i}"}})
            actions.append({"job_id": f"bulk-job-{i}", "status": "job-queued"})
        
        body = "\n".join([str(action).replace("'", '"') for action in actions]) + "\n"
        
        response = self.client.bulk(body=body, refresh=True)
        
        self.assertFalse(response['errors'])
        self.assertEqual(len(response['items']), 5)
        
    def test_update_document_without_doc_type(self):
        """Test updating a document without doc_type parameter."""
        doc = {
            "job_id": "test-job-3",
            "status": "job-started",
            "timestamp": "2024-01-01T00:00:00Z"
        }
        
        self.client.index(
            index=self.test_index,
            id="test-job-3",
            body=doc,
            refresh=True
        )
        
        update_body = {
            "doc": {
                "status": "job-completed"
            }
        }
        
        response = self.client.update(
            index=self.test_index,
            id="test-job-3",
            body=update_body,
            refresh=True
        )
        
        self.assertEqual(response['result'], 'updated')
        
        get_response = self.client.get(index=self.test_index, id="test-job-3")
        self.assertEqual(get_response['_source']['status'], 'job-completed')
        
    def test_delete_document_without_doc_type(self):
        """Test deleting a document without doc_type parameter."""
        doc = {
            "job_id": "test-job-4",
            "status": "job-failed",
            "timestamp": "2024-01-01T00:00:00Z"
        }
        
        self.client.index(
            index=self.test_index,
            id="test-job-4",
            body=doc,
            refresh=True
        )
        
        response = self.client.delete(
            index=self.test_index,
            id="test-job-4"
        )
        
        self.assertEqual(response['result'], 'deleted')
        
        with self.assertRaises(NotFoundError):
            self.client.get(index=self.test_index, id="test-job-4")
            
    def test_point_in_time_api(self):
        """Test Point-in-Time API for deep pagination."""
        for i in range(10):
            doc = {
                "job_id": f"pit-job-{i}",
                "status": "job-completed",
                "timestamp": f"2024-01-01T00:00:{i:02d}Z"
            }
            self.client.index(
                index=self.test_index,
                id=f"pit-job-{i}",
                body=doc
            )
        
        self.client.indices.refresh(index=self.test_index)
        
        pit = self.client.create_point_in_time(
            index=self.test_index,
            keep_alive="2m"
        )
        
        self.assertIn('pit_id', pit)
        
        query = {
            "size": 5,
            "query": {"match_all": {}},
            "pit": {
                "id": pit['pit_id'],
                "keep_alive": "2m"
            },
            "sort": [{"timestamp.keyword": "asc"}]
        }
        
        response = self.client.search(body=query)
        self.assertEqual(len(response['hits']['hits']), 5)
        
        self.client.delete_point_in_time(body={"pit_id": [pit['pit_id']]})


class TestLogstashConfigValidation(unittest.TestCase):
    """Test suite for Logstash configuration validation."""
    
    def test_opensearch_output_config_structure(self):
        """Test that Logstash config uses opensearch output."""
        config_path = os.path.join(
            os.path.dirname(__file__),
            '../../..',
            'hysds-dockerfiles/cluster/data/mozart/etc/indexer.conf'
        )
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_content = f.read()
            
            self.assertIn('opensearch {', config_content)
            self.assertNotIn('document_type =>', config_content)
            self.assertIn('hosts =>', config_content)
        
    def test_no_elasticsearch_output_in_config(self):
        """Test that elasticsearch output is not used."""
        config_path = os.path.join(
            os.path.dirname(__file__),
            '../../..',
            'hysds-dockerfiles/cluster/data/mozart/etc/indexer.conf'
        )
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_content = f.read()
            
            self.assertNotIn('elasticsearch {', config_content)


if __name__ == '__main__':
    unittest.main()
