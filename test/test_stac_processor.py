"""
Tests for STAC Phase 2: STAC Processing with Synthetic GRQ Records

Tests the STAC processor functionality including:
- STAC catalog processing and per-item GRQ record creation
- Asset URL resolution from relative to absolute S3 URLs
- STAC API indexing for collections and items
- Job metrics updates with STAC API URLs
- S3 upload integration
- Batch processing for large catalogs
"""

import os
import sys
import tempfile
import shutil
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock

# Mock hysds.celery before imports
sys.modules["hysds.celery"] = MagicMock()
sys.modules["opensearch"] = Mock()

from hysds.stac_processor import (
    process_stac_catalog,
    create_grq_dataset_from_stac_item,
    resolve_asset_urls,
    upload_catalog_to_s3,
    index_collections_to_stac_api,
    index_items_to_stac_api,
    update_job_metrics_for_stac
)
from hysds.utils import STACValidationError, STACAPIError


class TestSTACProcessor(TestCase):
    """Test Phase 2 STAC processor functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix="stac_processor_test_")
        self.maxDiff = None

        # Sample job with STAC enabled
        self.stac_job = {
            "job_info": {
                "job_spec": {"stac_output": True},
                "job_id": "test-job-123",
                "job_dir": self.temp_dir,
                "datasets_cfg_file": "/tmp/datasets.json",
                "metrics": {}
            }
        }

        # Mock STAC item
        self.mock_stac_item = Mock()
        self.mock_stac_item.id = "test-item-1"
        self.mock_stac_item.collection_id = "test-collection"
        self.mock_stac_item.geometry = {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
        }
        self.mock_stac_item.datetime = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        self.mock_stac_item.properties = {
            "platform": "test-satellite",
            "instruments": ["test-sensor"],
            "processing_level": "L1C"
        }
        self.mock_stac_item.assets = {
            "data": Mock(href="./data.tif"),
            "thumbnail": Mock(href="./thumb.png")
        }
        self.mock_stac_item.to_dict.return_value = {
            "type": "Feature",
            "id": "test-item-1",
            "collection": "test-collection",
            "geometry": self.mock_stac_item.geometry,
            "properties": {
                "datetime": "2023-01-01T12:00:00Z",
                **self.mock_stac_item.properties
            },
            "assets": {
                "data": {"href": "./data.tif", "type": "image/tiff"},
                "thumbnail": {"href": "./thumb.png", "type": "image/png"}
            }
        }

        # Mock STAC collection
        self.mock_collection = Mock()
        self.mock_collection.id = "test-collection"
        self.mock_collection.to_dict.return_value = {
            "type": "Collection",
            "id": "test-collection",
            "description": "Test collection",
            "stac_version": "1.0.0",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[0, 0, 1, 1]]},
                "temporal": {"interval": [["2023-01-01T00:00:00Z", "2023-12-31T23:59:59Z"]]}
            }
        }

        # Mock STAC catalog
        self.mock_catalog = Mock()
        self.mock_catalog.id = "test-catalog"
        self.mock_catalog.get_items.return_value = iter([self.mock_stac_item])
        self.mock_catalog.get_all_collections.return_value = [self.mock_collection]

        # Mock context with provenance information
        self.mock_context = {
            "_prov": {
                "workflow": "test-workflow",
                "software": "hysds-1.3.8",
                "timestamp": "2023-01-01T12:00:00Z"
            },
            "job_id": "test-job-123"
        }

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_create_grq_dataset_from_stac_item(self):
        """Test STAC Item to GRQ dataset conversion."""
        grq_dataset = create_grq_dataset_from_stac_item(self.mock_stac_item, self.stac_job, self.mock_context)
        
        # Verify basic structure
        self.assertEqual(grq_dataset["id"], "test-item-1")
        self.assertEqual(grq_dataset["objectid"], "test-item-1")  # New required field
        self.assertEqual(grq_dataset["dataset"], "stac_item")
        self.assertEqual(grq_dataset["dataset_type"], "test-collection")
        self.assertEqual(grq_dataset["dataset_level"], "item")
        self.assertEqual(grq_dataset["system_version"], "v1.0")  # Renamed field
        self.assertEqual(grq_dataset["ipath"], "stac_item/test-item-1")  # New required field
        self.assertEqual(grq_dataset["label"], "STAC Item: test-item-1")
        
        # Verify spatial/temporal data
        self.assertEqual(grq_dataset["location"], self.mock_stac_item.geometry)
        self.assertEqual(grq_dataset["starttime"], "2023-01-01T12:00:00+00:00Z")
        self.assertEqual(grq_dataset["endtime"], "2023-01-01T12:00:00+00:00Z")
        
        # Verify STAC-specific metadata
        self.assertEqual(grq_dataset["stac_version"], "1.0.0")
        self.assertEqual(grq_dataset["stac_item_id"], "test-item-1")
        self.assertEqual(grq_dataset["stac_collection"], "test-collection")
        
        # Verify STAC properties are copied
        self.assertEqual(grq_dataset["metadata"]["platform"], "test-satellite")
        self.assertEqual(grq_dataset["metadata"]["instruments"], ["test-sensor"])
        self.assertEqual(grq_dataset["metadata"]["processing_level"], "L1C")
        
        # Verify asset URLs
        expected_urls = ["./data.tif", "./thumb.png"]
        self.assertEqual(grq_dataset["urls"], expected_urls)
        
        # Verify new required fields
        self.assertEqual(grq_dataset["images"], [])  # Empty as specified
        self.assertEqual(grq_dataset["prov"], {
            "workflow": "test-workflow",
            "software": "hysds-1.3.8", 
            "timestamp": "2023-01-01T12:00:00Z"
        })  # From mock context
        
        # Verify standard fields
        self.assertIn("creation_timestamp", grq_dataset)

    def test_create_grq_dataset_from_stac_item_no_datetime(self):
        """Test GRQ dataset creation with STAC item without datetime."""
        self.mock_stac_item.datetime = None
        
        grq_dataset = create_grq_dataset_from_stac_item(self.mock_stac_item, self.stac_job, self.mock_context)
        
        self.assertIsNone(grq_dataset["starttime"])
        self.assertIsNone(grq_dataset["endtime"])

    def test_resolve_asset_urls_relative_paths(self):
        """Test asset URL resolution from relative to absolute S3 URLs."""
        s3_base_url = "s3://test-bucket/catalog"
        catalog_dir = "/tmp/catalog"
        
        # Mock assets with relative paths
        mock_asset1 = Mock()
        mock_asset1.href = "./data.tif"
        mock_asset2 = Mock()
        mock_asset2.href = "./subfolder/thumb.png"
        mock_asset3 = Mock()
        mock_asset3.href = "https://external.com/absolute.tif"  # Absolute URL should remain unchanged
        
        mock_item = Mock()
        mock_item.assets = {
            "data": mock_asset1,
            "thumbnail": mock_asset2,
            "external": mock_asset3
        }
        
        resolve_asset_urls(mock_item, s3_base_url, catalog_dir)
        
        # Verify relative paths were converted
        self.assertEqual(mock_asset1.href, "s3://test-bucket/catalog/data.tif")
        self.assertEqual(mock_asset2.href, "s3://test-bucket/catalog/subfolder/thumb.png")
        
        # Verify absolute URL remained unchanged
        self.assertEqual(mock_asset3.href, "https://external.com/absolute.tif")

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.put')
    def test_index_collections_to_stac_api_success(self, mock_put, mock_app):
        """Test successful collection indexing to STAC API."""
        # Configure mock app
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com',
            'STAC_API_KEY': 'test-api-key',
            'STAC_API_TIMEOUT': 30
        }.get(key, default)
        
        # Configure successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_put.return_value = mock_response
        
        index_collections_to_stac_api(self.mock_catalog)
        
        # Verify API call
        expected_url = "https://stac-api.example.com/collections/test-collection"
        expected_headers = {
            "Content-Type": "application/json",
            "X-Api-Key": "test-api-key"
        }
        
        mock_put.assert_called_once_with(
            expected_url,
            json=self.mock_collection.to_dict(),
            headers=expected_headers,
            timeout=30
        )

    @patch('hysds.stac_processor.app')
    def test_index_collections_to_stac_api_no_url_configured(self, mock_app):
        """Test collection indexing with missing STAC API URL."""
        mock_app.conf.get.return_value = None
        
        with self.assertRaises(STACAPIError) as context:
            index_collections_to_stac_api(self.mock_catalog)
        
        self.assertIn("STAC_API_URL not configured", str(context.exception))

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.put')
    def test_index_collections_to_stac_api_request_error(self, mock_put, mock_app):
        """Test collection indexing with request error."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com',
            'STAC_API_TIMEOUT': 30
        }.get(key, default)
        
        # Configure request to raise exception
        import requests
        mock_put.side_effect = requests.RequestException("API Error")
        
        with self.assertRaises(STACAPIError) as context:
            index_collections_to_stac_api(self.mock_catalog)
        
        self.assertIn("Failed to upsert collection test-collection", str(context.exception))

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.requests.put')
    def test_index_items_to_stac_api_success(self, mock_put, mock_app):
        """Test successful item indexing to STAC API."""
        mock_app.conf.get.side_effect = lambda key, default=None: {
            'STAC_API_URL': 'https://stac-api.example.com',
            'STAC_API_TIMEOUT': 30
        }.get(key, default)
        
        # Configure successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_put.return_value = mock_response
        
        stac_items = [self.mock_stac_item.to_dict()]
        index_items_to_stac_api(stac_items)
        
        # Verify API call
        expected_url = "https://stac-api.example.com/collections/test-collection/items/test-item-1"
        expected_headers = {"Content-Type": "application/json"}
        
        mock_put.assert_called_once_with(
            expected_url,
            json=self.mock_stac_item.to_dict(),
            headers=expected_headers,
            timeout=30
        )

    @patch('hysds.stac_processor.app')
    def test_update_job_metrics_for_stac(self, mock_app):
        """Test job metrics update with STAC API URLs."""
        mock_app.conf.get.return_value = "https://stac-api.example.com"
        
        # Create mock GRQ datasets
        grq_datasets = [
            {
                "id": "dataset-1",
                "stac_item_id": "test-item-1",
                "stac_collection": "test-collection",
                "path": "/tmp/data1",
                "disk_usage": 1000000
            },
            {
                "id": "dataset-2", 
                "stac_item_id": "test-item-2",
                "stac_collection": "another-collection",
                "path": "/tmp/data2",
                "disk_usage": 2000000
            }
        ]
        
        stac_api_results = {"status": "success"}
        
        update_job_metrics_for_stac(self.stac_job, grq_datasets, stac_api_results)
        
        # Verify products_staged was updated
        products_staged = self.stac_job["job_info"]["metrics"]["products_staged"]
        self.assertEqual(len(products_staged), 2)
        
        # Verify first product
        product1 = products_staged[0]
        self.assertEqual(product1["id"], "test-item-1")
        self.assertEqual(product1["urls"], ["https://stac-api.example.com/collections/test-collection/items/test-item-1"])
        self.assertEqual(product1["dataset_type"], "test-collection")
        self.assertEqual(product1["dataset"], "stac_item")
        self.assertEqual(product1["disk_usage"], 1000000)
        self.assertEqual(product1["stac_collection"], "test-collection")
        
        # Verify second product
        product2 = products_staged[1]
        self.assertEqual(product2["id"], "test-item-2")
        self.assertEqual(product2["urls"], ["https://stac-api.example.com/collections/another-collection/items/test-item-2"])
        self.assertEqual(product2["dataset_type"], "another-collection")

    @patch('hysds.stac_processor.app')
    def test_job_metrics_stac_api_url_format(self, mock_app):
        """Test that job metrics contain STAC API URLs, not S3 URLs."""
        stac_api_url = "https://stac-api.example.com"
        mock_app.conf.get.return_value = stac_api_url
        
        # Create GRQ datasets with S3 URLs
        grq_datasets = [
            {
                "id": "item-1",
                "stac_item_id": "item-1",
                "stac_collection": "collection-a",
                "urls": ["s3://bucket/data1.tif", "s3://bucket/thumb1.png"],  # S3 URLs
                "path": "/tmp/data1",
                "disk_usage": 1000000
            },
            {
                "id": "item-2",
                "stac_item_id": "item-2", 
                "stac_collection": "collection-b",
                "urls": ["s3://bucket/data2.tif"],  # S3 URL
                "path": "/tmp/data2",
                "disk_usage": 2000000
            }
        ]
        
        # Empty job metrics initially
        job = {"job_info": {"metrics": {}}}
        
        update_job_metrics_for_stac(job, grq_datasets, {"status": "success"})
        
        # Verify products_staged contains STAC API URLs
        products_staged = job["job_info"]["metrics"]["products_staged"]
        self.assertEqual(len(products_staged), 2)
        
        # First product should have STAC API URL, not S3 URL
        product1 = products_staged[0]
        expected_stac_url = f"{stac_api_url}/collections/collection-a/items/item-1"
        self.assertEqual(product1["urls"], [expected_stac_url])
        self.assertNotIn("s3://", product1["urls"][0])
        
        # Second product should also have STAC API URL
        product2 = products_staged[1]
        expected_stac_url2 = f"{stac_api_url}/collections/collection-b/items/item-2"
        self.assertEqual(product2["urls"], [expected_stac_url2])
        self.assertNotIn("s3://", product2["urls"][0])

    @patch('hysds.stac_processor.app')
    def test_job_metrics_preserves_existing_products(self, mock_app):
        """Test that STAC metrics don't overwrite existing products_staged entries."""
        mock_app.conf.get.return_value = "https://stac-api.example.com"
        
        # Job with existing products_staged entries
        job = {
            "job_info": {
                "metrics": {
                    "products_staged": [
                        {
                            "id": "existing-product",
                            "urls": ["s3://bucket/existing.tif"],
                            "dataset_type": "existing_type"
                        }
                    ]
                }
            }
        }
        
        # Add STAC products
        grq_datasets = [
            {
                "id": "stac-item",
                "stac_item_id": "stac-item",
                "stac_collection": "stac-collection",
                "path": "/tmp/stac",
                "disk_usage": 500000
            }
        ]
        
        update_job_metrics_for_stac(job, grq_datasets, {"status": "success"})
        
        # Verify both existing and new products are present
        products_staged = job["job_info"]["metrics"]["products_staged"]
        self.assertEqual(len(products_staged), 2)
        
        # Existing product should still be there
        existing_product = next(p for p in products_staged if p["id"] == "existing-product")
        self.assertEqual(existing_product["urls"], ["s3://bucket/existing.tif"])
        
        # New STAC product should be added
        stac_product = next(p for p in products_staged if p["id"] == "stac-item")
        self.assertIn("stac-api.example.com", stac_product["urls"][0])

    @patch('hysds.stac_processor.app')
    def test_job_metrics_complete_product_structure(self, mock_app):
        """Test that job metrics contain all required product fields."""
        mock_app.conf.get.return_value = "https://stac-api.example.com"
        
        grq_datasets = [
            {
                "id": "complete-item",
                "stac_item_id": "complete-item",
                "stac_collection": "complete-collection",
                "path": "/tmp/complete/path",
                "disk_usage": 1234567
            }
        ]
        
        job = {"job_info": {"metrics": {}}}
        
        update_job_metrics_for_stac(job, grq_datasets, {"status": "success"})
        
        product = job["job_info"]["metrics"]["products_staged"][0]
        
        # Verify all expected fields are present
        required_fields = ["id", "urls", "dataset_type", "dataset", "path", "disk_usage", "stac_collection"]
        for field in required_fields:
            self.assertIn(field, product, f"Missing required field: {field}")
        
        # Verify field values
        self.assertEqual(product["id"], "complete-item")
        self.assertEqual(product["dataset_type"], "complete-collection")
        self.assertEqual(product["dataset"], "stac_item")
        self.assertEqual(product["path"], "/tmp/complete/path")
        self.assertEqual(product["disk_usage"], 1234567)
        self.assertEqual(product["stac_collection"], "complete-collection")
        
        # Verify URL structure
        expected_url = "https://stac-api.example.com/collections/complete-collection/items/complete-item"
        self.assertEqual(product["urls"], [expected_url])

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.write_to_object_store')
    @patch('hysds.stac_processor.Recognizer')
    def test_upload_catalog_to_s3_success(self, mock_recognizer_class, mock_write, mock_app):
        """Test successful S3 upload of catalog."""
        # Setup mock recognizer
        mock_recognizer = Mock()
        mock_recognizer.publishConfigured.return_value = True
        mock_recognizer.getPublishPath.return_value = "s3://test-bucket/catalog"
        mock_recognizer.getS3Keys.return_value = ("secret", "access")
        mock_recognizer.getS3Profile.return_value = None
        mock_recognizer_class.return_value = mock_recognizer
        
        mock_write.return_value = None
        
        catalog_dir = os.path.join(self.temp_dir, "catalog")
        os.makedirs(catalog_dir)
        
        result = upload_catalog_to_s3(self.mock_catalog, catalog_dir, self.stac_job, {})
        
        # Verify result
        self.assertEqual(result, "s3://test-bucket/catalog")
        
        # Verify recognizer was configured correctly
        mock_recognizer.setDataset.assert_called_once()
        dataset_arg = mock_recognizer.setDataset.call_args[0][0]
        self.assertEqual(dataset_arg["dataset_type"], "stac_catalog")
        self.assertEqual(dataset_arg["label"], "STAC Catalog: test-catalog")
        
        # Verify upload was called
        mock_write.assert_called_once_with(
            catalog_dir,
            "s3://test-bucket/catalog",
            params={"aws_access_key_id": "access", "aws_secret_access_key": "secret"},
            force=False
        )

    @patch('hysds.stac_processor.Recognizer')
    def test_upload_catalog_to_s3_no_publish_config(self, mock_recognizer_class):
        """Test S3 upload failure with no publish configuration."""
        mock_recognizer = Mock()
        mock_recognizer.publishConfigured.return_value = False
        mock_recognizer_class.return_value = mock_recognizer
        
        catalog_dir = os.path.join(self.temp_dir, "catalog")
        os.makedirs(catalog_dir)
        
        with self.assertRaises(STACValidationError) as context:
            upload_catalog_to_s3(self.mock_catalog, catalog_dir, self.stac_job, {})
        
        self.assertIn("No publish configuration found", str(context.exception))

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.index_collections_to_stac_api')
    @patch('hysds.stac_processor.index_items_to_stac_api')
    @patch('hysds.stac_processor.bulk_index_dataset')
    @patch('hysds.stac_processor.upload_catalog_to_s3')
    @patch('hysds.stac_processor.update_job_metrics_for_stac')
    def test_process_stac_catalog_success(self, mock_metrics, mock_upload, mock_bulk_index, 
                                        mock_index_items, mock_index_collections, mock_app):
        """Test complete STAC catalog processing."""
        # Configure mocks
        mock_app.conf.get.return_value = 1000  # STAC_BATCH_SIZE
        mock_upload.return_value = "s3://test-bucket/catalog"
        mock_bulk_index.return_value = None
        mock_index_items.return_value = None
        mock_index_collections.return_value = None
        mock_metrics.return_value = None
        
        result = process_stac_catalog(self.mock_catalog, "/tmp/catalog", self.stac_job, {})
        
        # Verify upload was called first
        mock_upload.assert_called_once_with(self.mock_catalog, "/tmp/catalog", self.stac_job, {})
        
        # Verify collections were indexed first
        mock_index_collections.assert_called_once_with(self.mock_catalog)
        
        # Verify app config was accessed
        mock_app.conf.get.assert_called()
        
        # Verify items were processed
        mock_bulk_index.assert_called_once()
        mock_index_items.assert_called_once()
        
        # Verify metrics were updated
        mock_metrics.assert_called_once()
        
        # Verify result structure
        self.assertEqual(len(result), 1)  # One item processed
        self.assertEqual(result[0]["id"], "test-item-1")
        self.assertEqual(result[0]["dataset_type"], "test-collection")

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.upload_catalog_to_s3')
    def test_process_stac_catalog_batch_processing(self, mock_upload, mock_app):
        """Test batch processing for large catalogs."""
        # Set small batch size for testing
        mock_app.conf.get.return_value = 2  # Small batch size
        mock_upload.return_value = "s3://test-bucket/catalog"
        
        # Create multiple mock items
        items = []
        for i in range(5):
            item = Mock()
            item.id = f"test-item-{i}"
            item.collection_id = "test-collection"
            item.geometry = {"type": "Point", "coordinates": [0, 0]}
            item.datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
            item.properties = {"test": f"value-{i}"}
            item.assets = {"data": Mock(href=f"./data{i}.tif")}
            item.to_dict.return_value = {"id": f"test-item-{i}", "collection": "test-collection"}
            items.append(item)
        
        self.mock_catalog.get_items.return_value = iter(items)
        
        with patch('hysds.stac_processor.bulk_index_dataset') as mock_bulk, \
             patch('hysds.stac_processor.index_items_to_stac_api') as mock_index_items, \
             patch('hysds.stac_processor.index_collections_to_stac_api') as mock_index_collections:
            
            result = process_stac_catalog(self.mock_catalog, "/tmp/catalog", self.stac_job, {})
            
            # Should have called bulk_index_dataset multiple times due to batching
            # 5 items with batch size 2 = 3 calls (2, 2, 1)
            self.assertEqual(mock_bulk.call_count, 3)
            self.assertEqual(mock_index_items.call_count, 3)
            
            # Verify collections were indexed once
            mock_index_collections.assert_called_once()
            
            # Should have processed all 5 items
            self.assertEqual(len(result), 5)

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.upload_catalog_to_s3')
    def test_large_catalog_memory_efficiency(self, mock_upload, mock_app):
        """Test memory-efficient processing of large catalogs using iterators."""
        mock_app.conf.get.return_value = 1000  # Large batch size
        mock_upload.return_value = "s3://test-bucket/catalog"
        
        # Create a large number of mock items (simulate memory efficiency test)
        large_item_count = 2500
        items = []
        for i in range(large_item_count):
            item = Mock()
            item.id = f"large-item-{i}"
            item.collection_id = "large-collection"
            item.geometry = {"type": "Point", "coordinates": [i % 360 - 180, (i % 180) - 90]}
            item.datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
            item.properties = {"index": i, "batch": i // 1000}
            item.assets = {"data": Mock(href=f"./data{i}.tif")}
            item.to_dict.return_value = {"id": f"large-item-{i}", "collection": "large-collection"}
            items.append(item)
        
        # Use iterator to simulate memory-efficient processing
        self.mock_catalog.get_items.return_value = iter(items)
        
        with patch('hysds.stac_processor.bulk_index_dataset') as mock_bulk, \
             patch('hysds.stac_processor.index_items_to_stac_api') as mock_index_items, \
             patch('hysds.stac_processor.index_collections_to_stac_api'):
            
            result = process_stac_catalog(self.mock_catalog, "/tmp/catalog", self.stac_job, {})
            
            # Verify processing completed for all items
            self.assertEqual(len(result), large_item_count)
            
            # Verify batch processing occurred (2500 items / 1000 batch = 3 batches)
            expected_batches = (large_item_count + 999) // 1000  # Ceiling division
            self.assertEqual(mock_bulk.call_count, expected_batches)
            self.assertEqual(mock_index_items.call_count, expected_batches)
            
            # Verify all items were processed correctly
            for i, grq_dataset in enumerate(result):
                self.assertEqual(grq_dataset["id"], f"large-item-{i}")
                self.assertEqual(grq_dataset["dataset_type"], "large-collection")

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.upload_catalog_to_s3')
    def test_configurable_batch_size_processing(self, mock_upload, mock_app):
        """Test that batch size configuration is respected."""
        # Test with small batch size
        small_batch_size = 3
        mock_app.conf.get.return_value = small_batch_size
        mock_upload.return_value = "s3://test-bucket/catalog"
        
        # Create 7 items to test batch boundaries
        item_count = 7
        items = []
        for i in range(item_count):
            item = Mock()
            item.id = f"batch-item-{i}"
            item.collection_id = "batch-collection"
            item.geometry = {"type": "Point", "coordinates": [0, 0]}
            item.datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
            item.properties = {"batch_index": i}
            item.assets = {"data": Mock(href=f"./data{i}.tif")}
            item.to_dict.return_value = {"id": f"batch-item-{i}", "collection": "batch-collection"}
            items.append(item)
        
        self.mock_catalog.get_items.return_value = iter(items)
        
        with patch('hysds.stac_processor.bulk_index_dataset') as mock_bulk, \
             patch('hysds.stac_processor.index_items_to_stac_api') as mock_index_items, \
             patch('hysds.stac_processor.index_collections_to_stac_api'):
            
            result = process_stac_catalog(self.mock_catalog, "/tmp/catalog", self.stac_job, {})
            
            # 7 items with batch size 3 = 3 batches (3, 3, 1)
            expected_batches = 3
            self.assertEqual(mock_bulk.call_count, expected_batches)
            self.assertEqual(mock_index_items.call_count, expected_batches)
            
            # Verify all items processed
            self.assertEqual(len(result), item_count)
            

    @patch('hysds.stac_processor.app')
    @patch('hysds.stac_processor.upload_catalog_to_s3')
    def test_batch_processing_progress_logging(self, mock_upload, mock_app):
        """Test that batch processing includes progress logging."""
        mock_app.conf.get.return_value = 2  # Small batch for testing
        mock_upload.return_value = "s3://test-bucket/catalog"
        
        # Create 5 items to generate multiple batches
        items = []
        for i in range(5):
            item = Mock()
            item.id = f"progress-item-{i}"
            item.collection_id = "progress-collection"
            item.geometry = {"type": "Point", "coordinates": [0, 0]}
            item.datetime = datetime(2023, 1, 1, tzinfo=timezone.utc)
            item.properties = {"progress": i}
            item.assets = {"data": Mock(href=f"./data{i}.tif")}
            item.to_dict.return_value = {"id": f"progress-item-{i}", "collection": "progress-collection"}
            items.append(item)
        
        self.mock_catalog.get_items.return_value = iter(items)
        
        with patch('hysds.stac_processor.bulk_index_dataset') as mock_bulk, \
             patch('hysds.stac_processor.index_items_to_stac_api'), \
             patch('hysds.stac_processor.index_collections_to_stac_api'), \
             patch('hysds.stac_processor.logger') as mock_logger:
            
            result = process_stac_catalog(self.mock_catalog, "/tmp/catalog", self.stac_job, {})
            
            # Verify bulk processing was called
            self.assertGreater(mock_bulk.call_count, 0)
            
            # Verify all items were processed
            self.assertEqual(len(result), 5)
            
            # Verify progress logging occurred (should have intermediate and final batch logs)
            info_calls = [call for call in mock_logger.info.call_args_list 
                         if 'batch' in str(call).lower()]
            
            # Should have at least some batch-related logging
            self.assertGreater(len(info_calls), 0)


if __name__ == '__main__':
    import unittest
    unittest.main()