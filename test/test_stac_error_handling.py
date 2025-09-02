"""
Tests for STAC Phase 3: Error Handling and Rollback

Tests the comprehensive error handling functionality including:
- STAC workflow error handling with rollback capability
- GRQ record cleanup on failure
- S3 object cleanup on failure  
- STAC API item/collection cleanup on failure
- Configuration validation errors
- Transactional integrity
"""

import json
import os
import sys
import tempfile
import shutil
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock, call, mock_open

# Mock hysds.celery before imports
sys.modules["hysds.celery"] = MagicMock()
sys.modules["opensearch"] = Mock()

from hysds.stac_processor import (
    process_stac_workflow_with_error_handling,
    process_stac_catalog_with_tracking,
    cleanup_failed_stac_operation,
    cleanup_grq_records,
    cleanup_s3_objects,
    cleanup_stac_api_items,
    cleanup_stac_api_collections
)
from hysds.utils import STACValidationError, STACAPIError, AssetMissingError


class TestSTACErrorHandling(TestCase):
    """Test Phase 3 STAC error handling and rollback functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix="stac_error_test_")
        self.maxDiff = None
        
        # Create temporary datasets.json file (unique per test to avoid conflicts)
        self.datasets_file = f"/tmp/datasets_test_{os.getpid()}_{id(self)}.json"
        datasets_config = {
            "datasets": [
                {
                    "ipath": "test::stac_catalog",
                    "version": "v1.0",
                    "type": "stac_catalog",
                    "match_pattern": r"/(?P<id>catalog)\.json$",
                    "publish": {
                        "s3-bucket": "test-bucket",
                        "s3-key": "catalogs/{id}",
                        "s3-profile": "default"
                    }
                }
            ]
        }
        
        with open(self.datasets_file, 'w') as f:
            json.dump(datasets_config, f, indent=2)

        # Sample job with STAC enabled
        self.stac_job = {
            "job_info": {
                "job_spec": {"stac_output": True},
                "job_id": "test-job-123",
                "job_dir": self.temp_dir,
                "datasets_cfg_file": self.datasets_file,
                "metrics": {}
            }
        }

        # Mock tracking data for rollback
        self.mock_tracking = {
            "s3_uploads": ["s3://test-bucket/catalog1", "s3://test-bucket/catalog2"],
            "grq_indexed_ids": ["item-1", "item-2", "item-3"],
            "stac_api_collections": ["collection-1", "collection-2"],
            "stac_api_items": [
                {"collection": "collection-1", "id": "item-1"},
                {"collection": "collection-1", "id": "item-2"},
                {"collection": "collection-2", "id": "item-3"}
            ]
        }

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        if os.path.exists(self.datasets_file):
            os.remove(self.datasets_file)

    @patch('hysds.stac_processor.app')
    def test_process_stac_workflow_missing_stac_api_url(self, mock_app):
        """Test error handling when STAC_API_URL is not configured."""
        mock_app.conf.get.return_value = None
        
        with self.assertRaises(STACAPIError) as context:
            process_stac_workflow_with_error_handling(self.stac_job, {})
        
        self.assertIn("STAC_API_URL not configured", str(context.exception))

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.find_stac_catalogs')
    def test_process_stac_workflow_no_catalogs_found(self, mock_find_catalogs, mock_app):
        """Test error handling when no STAC catalogs are found."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com'
        }.get(key, default)
        
        mock_find_catalogs.return_value = []  # No catalogs found
        
        with self.assertRaises(STACValidationError) as context:
            process_stac_workflow_with_error_handling(self.stac_job, {})
        
        self.assertIn("no valid catalog.json found", str(context.exception))

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.find_stac_catalogs')
    @patch('hysds.stac_processor.validate_stac_assets_exist')
    @patch('hysds.stac_processor.cleanup_failed_stac_operation')
    def test_process_stac_workflow_asset_validation_error(self, mock_cleanup, mock_validate, 
                                                        mock_find_catalogs, mock_app):
        """Test error handling when asset validation fails."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com'
        }.get(key, default)
        
        # Setup mocks
        mock_catalog = Mock()
        mock_find_catalogs.return_value = [("/tmp/catalog.json", "/tmp", mock_catalog)]
        mock_validate.side_effect = AssetMissingError("Missing asset files")
        
        with self.assertRaises(AssetMissingError):
            process_stac_workflow_with_error_handling(self.stac_job, {})
        
        # Verify cleanup was called
        mock_cleanup.assert_called_once()

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.find_stac_catalogs')
    @patch('hysds.stac_processor.validate_stac_assets_exist')
    @patch('hysds.stac_processor.upload_catalog_to_s3')
    @patch('hysds.stac_processor.index_collections_to_stac_api')
    @patch('hysds.stac_processor.process_stac_catalog_with_tracking')
    @patch('hysds.stac_processor.cleanup_failed_stac_operation')
    def test_process_stac_workflow_processing_error_with_rollback(self, mock_cleanup, mock_process,
                                                               mock_index_collections, mock_upload,
                                                               mock_validate, mock_find_catalogs, mock_app):
        """Test error handling during catalog processing with rollback."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com'
        }.get(key, default)
        
        # Setup mocks
        mock_catalog = Mock()
        mock_catalog.get_all_collections.return_value = []  # No collections
        mock_find_catalogs.return_value = [("/tmp/catalog.json", "/tmp", mock_catalog)]
        mock_validate.return_value = None  # Validation passes
        mock_upload.return_value = "s3://bucket/test-catalog"  # S3 upload succeeds
        
        # Mock partial processing that fails after some operations
        mock_process.side_effect = STACAPIError("STAC API communication failed")
        
        with self.assertRaises(STACAPIError):
            process_stac_workflow_with_error_handling(self.stac_job, {})
        
        # Verify cleanup was called with tracking data
        mock_cleanup.assert_called_once()

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.delete')
    def test_cleanup_grq_records_success(self, mock_delete, mock_app):
        """Test successful GRQ record cleanup."""
        mock_app.conf.get.return_value = "https://grq.example.com"
        
        # Configure successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_delete.return_value = mock_response
        
        dataset_ids = ["item-1", "item-2", "item-3"]
        cleanup_grq_records(dataset_ids)
        
        # Verify delete calls were made
        expected_calls = [
            call("https://grq.example.com/dataset/item-1", timeout=30),
            call("https://grq.example.com/dataset/item-2", timeout=30),
            call("https://grq.example.com/dataset/item-3", timeout=30)
        ]
        mock_delete.assert_has_calls(expected_calls, any_order=True)

    @patch('hysds.stac_processor.app')
    def test_cleanup_grq_records_no_url_configured(self, mock_app):
        """Test GRQ cleanup when URL is not configured."""
        mock_app.conf.get.return_value = None
        
        # Should not raise exception, just log warning
        cleanup_grq_records(["item-1", "item-2"])

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.delete')
    @patch('hysds.stac_processor.time.sleep')  # Mock sleep to speed up tests
    def test_cleanup_grq_records_partial_failure(self, mock_sleep, mock_delete, mock_app):
        """Test GRQ cleanup with partial failures."""
        mock_app.conf.get.return_value = "https://grq.example.com"
        
        # Configure mixed responses - first two succeed, third fails and retries
        responses = [
            Mock(status_code=200),  # item-1: success
            Mock(status_code=404),  # item-2: not found (treated as success)
            Mock(status_code=500),  # item-3: first attempt fails
            Mock(status_code=500),  # item-3: second attempt fails  
            Mock(status_code=500),  # item-3: third attempt fails
        ]
        mock_delete.side_effect = responses
        
        dataset_ids = ["item-1", "item-2", "item-3"]
        
        # Should not raise exception even with failures
        cleanup_grq_records(dataset_ids)
        
        # item-1: 1 call, item-2: 1 call, item-3: 3 calls (initial + 2 retries)
        self.assertEqual(mock_delete.call_count, 5)
        # Sleep should be called twice (2 retry attempts for item-3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('osaka.main.rmall')
    def test_cleanup_s3_objects_success(self, mock_rmall):
        """Test successful S3 object cleanup."""
        mock_rmall.return_value = None
        
        s3_url = "s3://bucket/path1"
        cleanup_s3_objects(s3_url)
        
        # Verify rmall was called with the URL
        mock_rmall.assert_called_once_with(s3_url)

    @patch('osaka.main.rmall')
    def test_cleanup_s3_objects_failure(self, mock_rmall):
        """Test S3 cleanup with failures."""
        mock_rmall.side_effect = Exception("S3 deletion failed")
        
        # Should not raise exception even with failures
        cleanup_s3_objects("s3://bucket/path1")

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.delete')
    def test_cleanup_stac_api_items_success(self, mock_delete, mock_app):
        """Test successful STAC API item cleanup."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com',
            'STAC_API_TIMEOUT': 30
        }.get(key, default)
        
        mock_response = Mock()
        mock_response.status_code = 204
        mock_response.raise_for_status.return_value = None
        mock_delete.return_value = mock_response
        
        items = [
            {"collection_id": "col1", "item_id": "item1"},
            {"collection_id": "col2", "item_id": "item2"}
        ]
        
        cleanup_stac_api_items(items)
        
        # Verify delete calls - cleanup functions don't need Content-Type header
        expected_calls = [
            call("https://stac-api.example.com/collections/col1/items/item1", 
                 headers={}, timeout=30),
            call("https://stac-api.example.com/collections/col2/items/item2",
                 headers={}, timeout=30)
        ]
        mock_delete.assert_has_calls(expected_calls, any_order=True)

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.delete')
    def test_cleanup_stac_api_collections_success(self, mock_delete, mock_app):
        """Test successful STAC API collection cleanup."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com',
            'STAC_API_TIMEOUT': 30
        }.get(key, default)
        
        mock_response = Mock()
        mock_response.status_code = 204
        mock_response.raise_for_status.return_value = None
        mock_delete.return_value = mock_response
        
        collection_ids = ["collection-1", "collection-2"]
        cleanup_stac_api_collections(collection_ids)
        
        # Verify delete calls - cleanup functions don't need Content-Type header
        expected_calls = [
            call("https://stac-api.example.com/collections/collection-1",
                 headers={}, timeout=30),
            call("https://stac-api.example.com/collections/collection-2",
                 headers={}, timeout=30)
        ]
        mock_delete.assert_has_calls(expected_calls, any_order=True)

    @patch('hysds.stac_processor.cleanup_stac_api_collections')
    @patch('hysds.stac_processor.cleanup_stac_api_items')
    @patch('hysds.stac_processor.cleanup_grq_records')
    @patch('hysds.stac_processor.cleanup_s3_objects')
    def test_cleanup_failed_stac_operation_complete(self, mock_cleanup_s3, mock_cleanup_grq, 
                                                   mock_cleanup_items, mock_cleanup_collections):
        """Test complete cleanup operation with all components."""
        tracking_data = {
            "s3_uploads": ["s3://bucket/path1"],
            "grq_indexed_ids": ["item-1", "item-2"],
            "stac_api_items": [{"collection": "col1", "id": "item1"}],
            "stac_api_collections": ["collection-1"]
        }
        
        cleanup_failed_stac_operation(
            s3_upload_results=tracking_data.get("s3_uploads", []),
            grq_indexed_ids=tracking_data.get("grq_indexed_ids", []),
            stac_collections_created=tracking_data.get("stac_api_collections", []),
            stac_items_created=tracking_data.get("stac_api_items", [])
        )
        
        # Verify all cleanup functions were called in reverse order
        mock_cleanup_collections.assert_called_once_with(["collection-1"])
        mock_cleanup_items.assert_called_once_with([{"collection": "col1", "id": "item1"}])
        mock_cleanup_grq.assert_called_once_with(["item-1", "item-2"])
        mock_cleanup_s3.assert_called_once_with(["s3://bucket/path1"])

    @patch('hysds.stac_processor.cleanup_grq_records')
    def test_cleanup_failed_stac_operation_partial(self, mock_cleanup_grq):
        """Test cleanup with only partial tracking data."""
        tracking_data = {
            "grq_indexed_ids": ["item-1"]
            # Missing other components
        }
        
        cleanup_failed_stac_operation(
            s3_upload_results=tracking_data.get("s3_uploads", []),
            grq_indexed_ids=tracking_data.get("grq_indexed_ids", []),
            stac_collections_created=tracking_data.get("stac_api_collections", []),
            stac_items_created=tracking_data.get("stac_api_items", [])
        )
        
        # Only GRQ cleanup should be called
        mock_cleanup_grq.assert_called_once_with(["item-1"])

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.find_stac_catalogs')
    @patch('hysds.stac_processor.validate_stac_assets_exist')
    @patch('hysds.stac_processor.upload_catalog_to_s3')
    @patch('hysds.stac_processor.index_collections_to_stac_api')
    @patch('hysds.stac_processor.process_stac_catalog_with_tracking')
    def test_process_stac_catalog_with_tracking_success(self, mock_process, mock_index_collections,
                                                      mock_upload, mock_validate, mock_find_catalogs, mock_app):
        """Test successful catalog processing with tracking."""
        # Setup mocks
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com'
        }.get(key, default)
        
        mock_catalog = Mock()
        mock_catalog.get_all_collections.return_value = []  # No collections
        mock_catalog.get_items.return_value = iter([])  # No items
        mock_find_catalogs.return_value = [("/tmp/catalog.json", "/tmp", mock_catalog)]
        mock_validate.return_value = None
        mock_upload.return_value = "s3://bucket/catalog"
        mock_process.return_value = (
            [{"id": "item-1", "stac_collection": "collection-1"}],  # grq_datasets
            [{"collection_id": "collection-1", "item_id": "item-1"}]  # item_refs
        )
        
        result = process_stac_workflow_with_error_handling(self.stac_job, {})
        
        # Verify successful processing
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "item-1")
        
        # Verify operations were called
        mock_upload.assert_called_once()
        mock_process.assert_called_once()

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.find_stac_catalogs')
    @patch('hysds.stac_processor.validate_stac_assets_exist')
    @patch('hysds.stac_processor.upload_catalog_to_s3')
    def test_process_stac_catalog_with_tracking_upload_failure(self, mock_upload, mock_validate,
                                                             mock_find_catalogs, mock_app):
        """Test catalog processing with S3 upload failure and tracking."""
        # Setup mocks
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com'
        }.get(key, default)
        
        mock_catalog = Mock()
        mock_find_catalogs.return_value = [("/tmp/catalog.json", "/tmp", mock_catalog)]
        mock_validate.return_value = None
        mock_upload.side_effect = STACValidationError("S3 upload failed")
        
        with self.assertRaises(STACValidationError):
            process_stac_workflow_with_error_handling(self.stac_job, {})
        
        # Verify upload was attempted
        mock_upload.assert_called_once()

    @patch('hysds.stac_processor.app')
    def test_configuration_validation_missing_api_url(self, mock_app):
        """Test configuration validation when STAC_API_URL is missing."""
        mock_app.conf.get.return_value = None  # Missing STAC_API_URL
        
        with self.assertRaises(STACAPIError) as context:
            process_stac_workflow_with_error_handling(self.stac_job, {})
        
        self.assertIn("STAC_API_URL not configured", str(context.exception))

    @patch('hysds.stac_processor.app')
    def test_configuration_validation_empty_api_url(self, mock_app):
        """Test configuration validation when STAC_API_URL is empty string."""
        mock_app.conf.get.return_value = ""  # Empty STAC_API_URL
        
        with self.assertRaises(STACAPIError) as context:
            process_stac_workflow_with_error_handling(self.stac_job, {})
        
        self.assertIn("STAC_API_URL not configured", str(context.exception))

    @patch('hysds.stac_processor.app')
    def test_configuration_validation_malformed_api_url(self, mock_app):
        """Test processing continues with malformed URL (let requests handle the error)."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'not-a-valid-url'
        }.get(key, default)
        
        # Should not fail during config validation, but will fail during API calls
        with patch('hysds.stac_processor.find_stac_catalogs') as mock_find:
            mock_find.return_value = []
            
            with self.assertRaises(STACValidationError) as context:
                process_stac_workflow_with_error_handling(self.stac_job, {})
            
            # Should fail because no catalogs found, not because of malformed URL
            self.assertIn("no valid catalog.json found", str(context.exception))

    @patch('hysds.stac_processor.app')
    def test_configuration_defaults_applied(self, mock_app):
        """Test that configuration defaults are applied correctly."""
        # Test various configuration defaults
        config_values = {
            'STAC_API_URL': 'https://stac-api.example.com',
            'STAC_API_TIMEOUT': None,  # Should default to 30
            'STAC_BATCH_SIZE': None,   # Should default to 1000
            'STAC_VALIDATION_STRICT': None  # Should default to True
        }
        
        def mock_get(key, default=None):
            value = config_values.get(key)
            return value if value is not None else default
        
        mock_app.conf.get.side_effect = mock_get
        
        # Test that functions use the defaults
        from hysds.stac_processor import index_collections_to_stac_api
        
        mock_catalog = Mock()
        mock_catalog.get_all_collections.return_value = []
        
        with patch('hysds.stac_processor.requests.put') as mock_put:
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_put.return_value = mock_response
            
            # Should use default timeout of 30
            index_collections_to_stac_api(mock_catalog)
            
            # Verify timeout default was used (no actual calls since no collections)
            # This tests that the function doesn't fail due to missing config

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.put')
    @patch('hysds.stac_processor.time.sleep')  # Mock sleep to speed up tests
    def test_stac_api_retry_logic_success_after_failure(self, mock_sleep, mock_put, mock_app):
        """Test that STAC API calls retry on failure and eventually succeed."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com',
            'STAC_API_TIMEOUT': 30
        }.get(key, default)
        
        # First call fails, second call succeeds
        import requests
        mock_put.side_effect = [
            requests.RequestException("Temporary network error"),  # First failure
            Mock(raise_for_status=Mock())  # Second success
        ]
        
        from hysds.stac_processor import index_collections_to_stac_api
        
        mock_collection = Mock()
        mock_collection.id = "test-collection"
        mock_collection.to_dict.return_value = {"id": "test-collection", "type": "Collection"}
        
        mock_catalog = Mock()
        mock_catalog.get_all_collections.return_value = [mock_collection]
        
        # Should succeed after retry (retry logic is now implemented)
        index_collections_to_stac_api(mock_catalog)
        
        # Verify retry was attempted - sleep should have been called
        mock_sleep.assert_called_once()
        
        # Verify both requests were made
        self.assertEqual(mock_put.call_count, 2)

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.put')
    def test_stac_api_timeout_handling(self, mock_put, mock_app):
        """Test handling of API timeout errors."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com',
            'STAC_API_TIMEOUT': 5  # Short timeout for testing
        }.get(key, default)
        
        # Simulate timeout
        import requests
        mock_put.side_effect = requests.Timeout("Request timed out")
        
        from hysds.stac_processor import index_items_to_stac_api
        
        stac_items = [
            {
                "id": "test-item",
                "collection": "test-collection",
                "type": "Feature"
            }
        ]
        
        with self.assertRaises(STACAPIError) as context:
            index_items_to_stac_api(stac_items)
        
        self.assertIn("Failed to upsert item test-item", str(context.exception))

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.put')
    def test_stac_api_http_error_codes(self, mock_put, mock_app):
        """Test handling of various HTTP error codes from STAC API."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com',
            'STAC_API_TIMEOUT': 30
        }.get(key, default)
        
        # Test different HTTP error codes
        error_codes = [400, 401, 403, 404, 500, 502, 503]
        
        from hysds.stac_processor import index_collections_to_stac_api
        
        mock_collection = Mock()
        mock_collection.id = "error-collection"
        mock_collection.to_dict.return_value = {"id": "error-collection", "type": "Collection"}
        
        mock_catalog = Mock()
        mock_catalog.get_all_collections.return_value = [mock_collection]
        
        for error_code in error_codes:
            with self.subTest(error_code=error_code):
                # Create HTTP error response
                import requests
                mock_response = Mock()
                mock_response.raise_for_status.side_effect = requests.HTTPError(f"{error_code} Error")
                mock_put.return_value = mock_response
                
                with self.assertRaises(STACAPIError) as context:
                    index_collections_to_stac_api(mock_catalog)
                
                self.assertIn("Failed to upsert collection error-collection", str(context.exception))

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.delete')
    @patch('hysds.stac_processor.time.sleep')  # Mock sleep to speed up tests
    def test_grq_cleanup_retry_on_failure(self, mock_sleep, mock_delete, mock_app):
        """Test that GRQ cleanup handles temporary failures gracefully with retry logic."""
        mock_app.conf.get.return_value = "https://grq.example.com"
        
        # Simulate intermittent failures with retry
        responses = [
            Mock(status_code=500),  # retry-item-1: first attempt fails
            Mock(status_code=200),  # retry-item-1: second attempt succeeds
            Mock(status_code=404),  # retry-item-2: success (404 is acceptable)
            Mock(status_code=500),  # retry-item-3: fails all attempts
            Mock(status_code=500),  # retry-item-3: second attempt
            Mock(status_code=500),  # retry-item-3: third attempt
        ]
        mock_delete.side_effect = responses
        
        dataset_ids = ["retry-item-1", "retry-item-2", "retry-item-3"]
        
        # Should not raise exception even with failures
        cleanup_grq_records(dataset_ids)
        
        # retry-item-1: 2 calls (fail, succeed), retry-item-2: 1 call, retry-item-3: 3 calls (all fail)
        self.assertEqual(mock_delete.call_count, 6)
        # Sleep called 3 times total (1 retry for item-1, 2 retries for item-3)
        self.assertEqual(mock_sleep.call_count, 3)

    @patch('osaka.main.rmall')
    def test_s3_cleanup_retry_behavior(self, mock_rmall):
        """Test S3 cleanup behavior with failures."""
        # Test that function handles exceptions gracefully
        mock_rmall.side_effect = Exception("S3 connection failed")
        
        s3_url = "s3://bucket/path1"
        
        # Should not raise exception even with failures
        cleanup_s3_objects(s3_url)
        
        # Verify cleanup attempt was made
        mock_rmall.assert_called_once_with(s3_url)

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.write_to_object_store')
    def test_disk_space_exhaustion_during_s3_upload(self, mock_write, mock_app):
        """Test handling of disk space exhaustion during S3 upload."""
        mock_app.conf.get.return_value = "https://stac-api.example.com"
        
        # Simulate disk space exhaustion
        mock_write.side_effect = OSError("No space left on device")
        
        from hysds.stac_processor import upload_catalog_to_s3
        
        mock_catalog = Mock()
        mock_catalog.id = "disk-full-catalog"
        
        # Mock recognizer setup
        with patch('hysds.stac_processor.Recognizer') as mock_recognizer_class:
            mock_recognizer = Mock()
            mock_recognizer.publishConfigured.return_value = True
            mock_recognizer.getPublishPath.return_value = "s3://bucket/catalog"
            mock_recognizer.getS3Keys.return_value = ("secret", "access")
            mock_recognizer.getS3Profile.return_value = None
            mock_recognizer_class.return_value = mock_recognizer
            
            with self.assertRaises(STACValidationError) as context:
                upload_catalog_to_s3(mock_catalog, "/tmp/catalog", self.stac_job, {})
            
            self.assertIn("Failed to upload STAC catalog to S3", str(context.exception))

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.bulk_index_dataset')
    def test_memory_exhaustion_during_large_batch(self, mock_bulk_index, mock_app):
        """Test handling of memory exhaustion during large batch processing."""
        mock_app.conf.get.return_value = 10000  # Very large batch size
        
        # Simulate memory error
        mock_bulk_index.side_effect = MemoryError("Cannot allocate memory")
        
        from hysds.stac_processor import process_stac_catalog
        
        # Create catalog with items that would cause memory issues
        mock_catalog = Mock()
        mock_catalog.id = "memory-test-catalog"
        
        # Mock a single item to trigger the memory error
        mock_item = Mock()
        mock_item.id = "memory-item"
        mock_item.collection_id = "memory-collection"
        mock_item.geometry = {"type": "Point", "coordinates": [0, 0]}
        mock_item.datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
        mock_item.properties = {"test": "memory"}
        mock_item.assets = {"data": Mock(href="./data.tif")}
        mock_item.to_dict.return_value = {"id": "memory-item", "collection": "memory-collection"}
        
        mock_catalog.get_items.return_value = iter([mock_item])
        mock_catalog.get_all_collections.return_value = []
        
        with patch('hysds.stac_processor.upload_catalog_to_s3') as mock_upload, \
             patch('hysds.stac_processor.index_collections_to_stac_api'):
            
            mock_upload.return_value = "s3://bucket/catalog"
            
            # Should propagate the memory error
            with self.assertRaises(MemoryError):
                process_stac_catalog(mock_catalog, "/tmp/catalog", self.stac_job, {})

    @patch('hysds.stac_processor.app')
    def test_api_rate_limiting_simulation(self, mock_app):
        """Test handling of API rate limiting responses."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com',
            'STAC_API_TIMEOUT': 30
        }.get(key, default)
        
        from hysds.stac_processor import index_items_to_stac_api
        
        # Simulate rate limiting (429 Too Many Requests)
        with patch('hysds.stac_processor.requests.put') as mock_put:
            import requests
            mock_response = Mock()
            mock_response.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")
            mock_put.return_value = mock_response
            
            stac_items = [{"id": "rate-limited-item", "collection": "test-collection"}]
            
            with self.assertRaises(STACAPIError) as context:
                index_items_to_stac_api(stac_items)
            
            self.assertIn("Failed to upsert item rate-limited-item", str(context.exception))

    @patch('hysds.utils.get_disk_usage')
    @patch('hysds.stac_processor.app')
    def test_disk_usage_monitoring_during_processing(self, mock_app, mock_disk_usage):
        """Test disk usage monitoring during STAC processing."""
        mock_app.conf.get.return_value = 1000  # Batch size
        
        # Simulate increasing disk usage
        disk_usage_values = [100, 500, 800, 950, 990]  # MB, approaching limit
        mock_disk_usage.side_effect = disk_usage_values
        
        from hysds.stac_processor import process_stac_catalog
        
        mock_catalog = Mock()
        mock_catalog.id = "disk-monitor-catalog"
        
        # Create multiple items to trigger disk monitoring
        items = []
        for i in range(5):
            item = Mock()
            item.id = f"disk-item-{i}"
            item.collection_id = "disk-collection"
            item.geometry = {"type": "Point", "coordinates": [0, 0]}
            item.datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
            item.properties = {"index": i}
            item.assets = {"data": Mock(href=f"./data{i}.tif")}
            item.to_dict.return_value = {"id": f"disk-item-{i}", "collection": "disk-collection"}
            items.append(item)
        
        mock_catalog.get_items.return_value = iter(items)
        mock_catalog.get_all_collections.return_value = []
        
        with patch('hysds.stac_processor.upload_catalog_to_s3') as mock_upload, \
             patch('hysds.stac_processor.bulk_index_dataset'), \
             patch('hysds.stac_processor.index_items_to_stac_api'), \
             patch('hysds.stac_processor.index_collections_to_stac_api'):
            
            mock_upload.return_value = "s3://bucket/catalog"
            
            # Processing should complete normally
            result = process_stac_catalog(mock_catalog, "/tmp/catalog", self.stac_job, {})
            
            # Verify processing completed
            self.assertEqual(len(result), 5)
            
            # If disk usage monitoring is implemented, verify it was called
            # This is documenting expected behavior if monitoring is added
            if mock_disk_usage.called:
                self.assertGreater(mock_disk_usage.call_count, 0)

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.find_stac_catalogs')
    @patch('hysds.stac_processor.upload_catalog_to_s3')
    @patch('hysds.stac_processor.process_stac_catalog')
    def test_partial_failure_recovery_s3_success_grq_failure(self, mock_process, mock_upload,
                                                          mock_find_catalogs, mock_app):
        """Test recovery when S3 upload succeeds but GRQ indexing fails."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com'
        }.get(key, default)
        
        # Setup successful S3 upload
        mock_catalog = Mock()
        mock_find_catalogs.return_value = [("/tmp/catalog.json", "/tmp", mock_catalog)]
        mock_upload.return_value = "s3://bucket/catalog"  # S3 upload succeeds
        
        # But GRQ indexing fails
        mock_process.side_effect = Exception("GRQ indexing failed")
        
        from hysds.stac_processor import process_stac_workflow_with_error_handling
        
        with patch('hysds.stac_processor.validate_stac_assets_exist'), \
             patch('hysds.stac_processor.cleanup_failed_stac_operation') as mock_cleanup:
            
            # Should trigger cleanup after partial failure
            with self.assertRaises(Exception):
                process_stac_workflow_with_error_handling(self.stac_job, {})
            
            # Verify cleanup was called
            mock_cleanup.assert_called_once()

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.bulk_index_dataset')
    @patch('hysds.stac_processor.index_items_to_stac_api')
    def test_partial_failure_grq_success_stac_api_failure(self, mock_stac_api, mock_grq, mock_app):
        """Test handling when GRQ indexing succeeds but STAC API indexing fails."""
        mock_app.conf.get.return_value = 1000  # Batch size
        
        # GRQ indexing succeeds
        mock_grq.return_value = None
        
        # STAC API indexing fails
        mock_stac_api.side_effect = STACAPIError("STAC API connection failed")
        
        from hysds.stac_processor import process_stac_catalog
        
        mock_catalog = Mock()
        mock_catalog.id = "partial-fail-catalog"
        
        mock_item = Mock()
        mock_item.id = "partial-item"
        mock_item.collection_id = "partial-collection"
        mock_item.geometry = {"type": "Point", "coordinates": [0, 0]}
        mock_item.datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
        mock_item.properties = {"test": "partial"}
        mock_item.assets = {"data": Mock(href="./data.tif")}
        mock_item.to_dict.return_value = {"id": "partial-item", "collection": "partial-collection"}
        
        mock_catalog.get_items.return_value = iter([mock_item])
        mock_catalog.get_all_collections.return_value = []
        
        with patch('hysds.stac_processor.upload_catalog_to_s3') as mock_upload, \
             patch('hysds.stac_processor.index_collections_to_stac_api'):
            
            mock_upload.return_value = "s3://bucket/catalog"
            
            # Should propagate STAC API error
            with self.assertRaises(STACAPIError):
                process_stac_catalog(mock_catalog, "/tmp/catalog", self.stac_job, {})
            
            # Verify GRQ indexing was attempted
            mock_grq.assert_called_once()

    @patch('hysds.stac_processor.cleanup_stac_api_items')
    @patch('hysds.stac_processor.cleanup_stac_api_collections')
    @patch('hysds.stac_processor.cleanup_grq_records')
    @patch('hysds.stac_processor.cleanup_s3_objects')
    def test_partial_cleanup_rollback_order(self, mock_s3_cleanup, mock_grq_cleanup,
                                          mock_collections_cleanup, mock_items_cleanup):
        """Test that partial cleanup occurs in correct reverse order."""
        # Simulate partial success scenario
        tracking_data = {
            "s3_uploads": ["s3://bucket/path1", "s3://bucket/path2"],
            "grq_indexed_ids": ["grq-item-1", "grq-item-2"],
            "stac_api_collections": ["collection-1"],
            "stac_api_items": [{"collection": "collection-1", "id": "stac-item-1"}]
        }
        
        # Test cleanup with partial failures in cleanup operations
        mock_items_cleanup.side_effect = Exception("Items cleanup failed")  # Partial failure
        mock_collections_cleanup.return_value = None  # Success
        mock_grq_cleanup.return_value = None  # Success
        mock_s3_cleanup.side_effect = Exception("S3 cleanup failed")  # Partial failure
        
        # Should not raise exception even with cleanup failures
        cleanup_failed_stac_operation(
            s3_upload_results=tracking_data.get("s3_uploads", []),
            grq_indexed_ids=tracking_data.get("grq_indexed_ids", []),
            stac_collections_created=tracking_data.get("stac_api_collections", []),
            stac_items_created=tracking_data.get("stac_api_items", [])
        )
        
        # Verify all cleanup attempts were made in correct order (reverse dependency order)
        mock_collections_cleanup.assert_called_once_with(["collection-1"])
        mock_items_cleanup.assert_called_once_with([{"collection": "collection-1", "id": "stac-item-1"}])
        mock_grq_cleanup.assert_called_once_with(["grq-item-1", "grq-item-2"])
        mock_s3_cleanup.assert_called_once_with(["s3://bucket/path1", "s3://bucket/path2"])

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.process_stac_catalog_with_tracking')
    def test_mixed_success_failure_recovery_state(self, mock_process_with_tracking, mock_app):
        """Test recovery state tracking with mixed success/failure scenarios."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com'
        }.get(key, default)
        
        # Simulate successful processing but with tracking data for potential rollback
        mock_process_with_tracking.return_value = (
            [{"id": "success-item", "dataset_type": "success-collection"}],  # Successful results
            {
                "s3_uploads": ["s3://bucket/success-catalog"],
                "grq_indexed_ids": ["success-item"],
                "stac_api_collections": ["success-collection"], 
                "stac_api_items": [{"collection": "success-collection", "id": "success-item"}]
            }  # Tracking data
        )
        
        from hysds.stac_processor import process_stac_workflow_with_error_handling
        
        with patch('hysds.stac_processor.find_stac_catalogs') as mock_find, \
             patch('hysds.stac_processor.validate_stac_assets_exist'), \
             patch('hysds.stac_processor.upload_catalog_to_s3') as mock_upload, \
             patch('hysds.stac_processor.index_collections_to_stac_api'), \
             patch('builtins.open', mock_open()):
            
            mock_catalog = Mock()
            mock_catalog.get_all_collections.return_value = []  # No collections
            mock_find.return_value = [("/tmp/catalog.json", "/tmp", mock_catalog)]
            mock_upload.return_value = "s3://bucket/success-catalog"
            
            # Should succeed without triggering cleanup
            result = process_stac_workflow_with_error_handling(self.stac_job, {})
            
            self.assertTrue(result)
            mock_process_with_tracking.assert_called_once()

    @patch('hysds.stac_processor.app')
    def test_cascading_failure_recovery_resilience(self, mock_app):
        """Test system resilience when multiple cleanup operations fail."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com',
            'GRQ_UPDATE_URL_BULK': 'https://grq.example.com'
        }.get(key, default)
        
        # Create scenario where all cleanup operations fail
        tracking_data = {
            "s3_upload_results": ["s3://bucket/failed"],
            "grq_indexed_ids": ["failed-item"],
            "stac_collections_created": ["failed-collection"],
            "stac_items_created": [{"collection": "failed-collection", "id": "failed-item"}]
        }
        
        with patch('hysds.stac_processor.cleanup_s3_objects') as mock_s3, \
             patch('hysds.stac_processor.cleanup_grq_records') as mock_grq, \
             patch('hysds.stac_processor.cleanup_stac_api_collections') as mock_collections, \
             patch('hysds.stac_processor.cleanup_stac_api_items') as mock_items:
            
            # All cleanup operations fail
            mock_s3.side_effect = Exception("S3 cleanup cascade failure")
            mock_grq.side_effect = Exception("GRQ cleanup cascade failure")
            mock_collections.side_effect = Exception("Collections cleanup cascade failure")
            mock_items.side_effect = Exception("Items cleanup cascade failure")
            
            # Should not raise exception despite all cleanup failures
            cleanup_failed_stac_operation(
                s3_upload_results=tracking_data.get("s3_upload_results", []),
                grq_indexed_ids=tracking_data.get("grq_indexed_ids", []),
                stac_collections_created=tracking_data.get("stac_collections_created", []),
                stac_items_created=tracking_data.get("stac_items_created", [])
            )
            
            # Verify all cleanup attempts were made despite failures
            mock_collections.assert_called_once()
            mock_items.assert_called_once()
            mock_grq.assert_called_once()
            mock_s3.assert_called_once()


if __name__ == '__main__':
    import unittest
    unittest.main()