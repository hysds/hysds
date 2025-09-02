"""
Tests for STAC Phase 1: Job-Spec Configuration & Core Detection

Tests the core STAC functionality including:
- Job-spec STAC enablement detection
- STAC catalog discovery and validation
- Asset existence validation
- Exception handling
"""

import json
import os
import sys
import tempfile
import shutil
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock, mock_open

# Mock hysds.celery before imports
sys.modules["hysds.celery"] = MagicMock()
sys.modules["opensearchpy"] = Mock()

from hysds.utils import (
    get_job_stac_enabled,
    find_stac_catalogs,
    validate_stac_assets_exist,
    STACValidationError,
    STACAPIError,
    AssetMissingError
)


class TestSTACPhase1Core(TestCase):
    """Test Phase 1 STAC core functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix="stac_test_")
        self.maxDiff = None

        # Sample job with STAC enabled
        self.stac_job = {
            "job_info": {
                "job_spec": {"stac_output": True},
                "job_id": "test-job-123",
                "job_dir": self.temp_dir
            }
        }

        # Sample job with STAC disabled
        self.traditional_job = {
            "job_info": {
                "job_spec": {"stac_output": False},
                "job_id": "test-job-456"
            }
        }

        # Sample job without STAC field
        self.legacy_job = {
            "job_info": {
                "job_spec": {},
                "job_id": "test-job-789"
            }
        }

        # Valid STAC catalog
        self.valid_catalog = {
            "type": "Catalog",
            "id": "test-catalog",
            "description": "Test STAC catalog",
            "stac_version": "1.0.0",
            "links": [
                {
                    "rel": "item",
                    "href": "./item1.json",
                    "type": "application/json"
                }
            ]
        }

        # Valid STAC item
        self.valid_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "test-item-1",
            "collection": "test-collection",
            "properties": {
                "datetime": "2023-01-01T12:00:00Z"
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
            },
            "assets": {
                "data": {
                    "href": "./data.tif",
                    "type": "image/tiff",
                    "roles": ["data"]
                },
                "thumbnail": {
                    "href": "./thumb.png",
                    "type": "image/png", 
                    "roles": ["thumbnail"]
                }
            }
        }

        # Valid STAC collection
        self.valid_collection = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-collection",
            "description": "Test collection",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {
                    "bbox": [[0, 0, 1, 1]]
                },
                "temporal": {
                    "interval": [["2023-01-01T00:00:00Z", "2023-12-31T23:59:59Z"]]
                }
            }
        }

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_get_job_stac_enabled_true(self):
        """Test STAC enabled detection when stac_output is True."""
        result = get_job_stac_enabled(self.stac_job)
        self.assertTrue(result)

    def test_get_job_stac_enabled_false(self):
        """Test STAC enabled detection when stac_output is False."""
        result = get_job_stac_enabled(self.traditional_job)
        self.assertFalse(result)

    def test_get_job_stac_enabled_missing_field(self):
        """Test STAC enabled detection when stac_output field is missing."""
        result = get_job_stac_enabled(self.legacy_job)
        self.assertFalse(result)  # Should default to False

    def test_get_job_stac_enabled_missing_job_spec(self):
        """Test STAC enabled detection with malformed job structure."""
        malformed_job = {"job_info": {}}
        result = get_job_stac_enabled(malformed_job)
        self.assertFalse(result)

    def test_get_job_stac_enabled_empty_job(self):
        """Test STAC enabled detection with empty job."""
        empty_job = {}
        result = get_job_stac_enabled(empty_job)
        self.assertFalse(result)

    def test_find_stac_catalogs_valid_catalog(self):
        """Test finding valid STAC catalogs."""

        # Create catalog file
        catalog_dir = os.path.join(self.temp_dir, "catalog1")
        os.makedirs(catalog_dir)
        catalog_file = os.path.join(catalog_dir, "catalog.json")
        
        with open(catalog_file, 'w') as f:
            json.dump(self.valid_catalog, f)

        # Test discovery
        catalogs = list(find_stac_catalogs(self.temp_dir))
        
        self.assertEqual(len(catalogs), 1)
        catalog_path, root_dir, catalog_obj = catalogs[0]
        self.assertEqual(catalog_path, catalog_file)
        self.assertEqual(root_dir, catalog_dir)
        self.assertIsNotNone(catalog_obj)  # Verify catalog object is returned


    @patch('hysds.utils.pystac', create=True)
    def test_find_stac_catalogs_invalid_catalog(self, mock_pystac):
        """Test handling of invalid STAC catalogs."""
        # Setup mock to raise validation error
        mock_pystac.Catalog.from_file.side_effect = Exception("Invalid STAC")

        # Create invalid catalog file
        catalog_dir = os.path.join(self.temp_dir, "invalid_catalog")
        os.makedirs(catalog_dir)
        catalog_file = os.path.join(catalog_dir, "catalog.json")
        
        with open(catalog_file, 'w') as f:
            json.dump({"invalid": "catalog"}, f)

        # Test discovery - should skip invalid catalogs
        catalogs = list(find_stac_catalogs(self.temp_dir))
        
        self.assertEqual(len(catalogs), 0)

    @patch('hysds.utils.pystac', create=True)
    def test_find_stac_catalogs_multiple_catalogs(self, mock_pystac):
        """Test finding multiple STAC catalogs."""
        # Setup mock
        mock_catalog1 = Mock()
        mock_catalog1.validate.return_value = None
        mock_catalog2 = Mock()
        mock_catalog2.validate.return_value = None
        
        mock_pystac.Catalog.from_file.side_effect = [mock_catalog1, mock_catalog2]

        # Create multiple catalog files
        for i in range(2):
            catalog_dir = os.path.join(self.temp_dir, f"catalog{i+1}")
            os.makedirs(catalog_dir)
            catalog_file = os.path.join(catalog_dir, "catalog.json")
            
            with open(catalog_file, 'w') as f:
                json.dump(self.valid_catalog, f)

        # Test discovery
        catalogs = list(find_stac_catalogs(self.temp_dir))
        
        self.assertEqual(len(catalogs), 2)


    def test_validate_stac_assets_exist_valid(self):
        """Test asset validation with all assets present."""
        # Create mock catalog with items
        mock_item1 = Mock()
        mock_item1.id = "item1"
        mock_item1.assets = {
            "data": Mock(href="./data1.tif"),
            "thumbnail": Mock(href="./thumb1.png")
        }

        mock_item2 = Mock()
        mock_item2.id = "item2"
        mock_item2.assets = {
            "data": Mock(href="./data2.tif")
        }

        mock_catalog = Mock()
        mock_catalog.get_all_items.return_value = [mock_item1, mock_item2]

        # Create asset files
        catalog_dir = os.path.join(self.temp_dir, "catalog")
        os.makedirs(catalog_dir)
        
        for asset_file in ["data1.tif", "thumb1.png", "data2.tif"]:
            asset_path = os.path.join(catalog_dir, asset_file)
            with open(asset_path, 'w') as f:
                f.write("mock asset data")

        # Should not raise exception
        validate_stac_assets_exist(mock_catalog, catalog_dir)

    def test_validate_stac_assets_exist_missing_assets(self):
        """Test asset validation with missing assets."""
        # Create mock catalog with items
        mock_item = Mock()
        mock_item.id = "item1"
        mock_item.assets = {
            "data": Mock(href="./missing_data.tif"),
            "thumbnail": Mock(href="./missing_thumb.png")
        }

        mock_catalog = Mock()
        mock_catalog.get_all_items.return_value = [mock_item]

        catalog_dir = os.path.join(self.temp_dir, "catalog")
        os.makedirs(catalog_dir)

        # Should raise AssetMissingError
        with self.assertRaises(AssetMissingError) as context:
            validate_stac_assets_exist(mock_catalog, catalog_dir)
        
        error_message = str(context.exception)
        self.assertIn("Missing assets", error_message)
        self.assertIn("item1", error_message)
        self.assertIn("./missing_data.tif", error_message)
        self.assertIn("./missing_thumb.png", error_message)

    def test_validate_stac_assets_exist_absolute_urls(self):
        """Test asset validation with absolute URLs (they are checked and will fail)."""
        # Create mock catalog with absolute URL assets
        mock_item = Mock()
        mock_item.id = "item1"
        mock_item.assets = {
            "data": Mock(href="https://example.com/data.tif"),
            "local": Mock(href="./local.tif")
        }

        mock_catalog = Mock()
        mock_catalog.get_all_items.return_value = [mock_item]

        catalog_dir = os.path.join(self.temp_dir, "catalog")
        os.makedirs(catalog_dir)
        
        # Create only the local asset
        local_path = os.path.join(catalog_dir, "local.tif")
        with open(local_path, 'w') as f:
            f.write("local asset data")

        # Should raise exception because absolute URLs are also validated
        with self.assertRaises(AssetMissingError) as context:
            validate_stac_assets_exist(mock_catalog, catalog_dir)
        
        error_message = str(context.exception)
        self.assertIn("Missing assets", error_message)
        self.assertIn("https://example.com/data.tif", error_message)

    def test_stac_exceptions_inheritance(self):
        """Test that STAC exceptions inherit from Exception properly."""
        self.assertTrue(issubclass(STACValidationError, Exception))
        self.assertTrue(issubclass(STACAPIError, Exception))
        self.assertTrue(issubclass(AssetMissingError, Exception))

        # Test exception creation
        validation_error = STACValidationError("Validation failed")
        api_error = STACAPIError("API communication failed")
        asset_error = AssetMissingError("Assets missing")

        self.assertEqual(str(validation_error), "Validation failed")
        self.assertEqual(str(api_error), "API communication failed")
        self.assertEqual(str(asset_error), "Assets missing")


if __name__ == '__main__':
    import unittest
    unittest.main()