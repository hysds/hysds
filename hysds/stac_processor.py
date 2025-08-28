import os
import json
import shutil
import requests
from datetime import datetime, timezone
from urllib.parse import urljoin

import pystac
from hysds.celery import app
from hysds.log_utils import logger
from hysds.recognize import Recognizer
from hysds.utils import STACAPIError, STACValidationError, get_disk_usage
from hysds.dataset_ingest_bulk import write_to_object_store, bulk_index_dataset, parse_iso8601


def process_stac_catalog(catalog, catalog_dir, job, ctx):
    """Process STAC catalog with per-item GRQ record creation using iterators for memory efficiency."""
    
    # Upload catalog directory to S3 first
    s3_base_url = upload_catalog_to_s3(catalog, catalog_dir, job, ctx)
    
    # Process items in batches to handle large catalogs efficiently
    batch_size = app.conf.get('STAC_BATCH_SIZE', 1000)
    grq_datasets = []
    stac_items_for_api = []
    all_grq_datasets = []  # Track all for rollback if needed
    
    # Use iterator to process one item at a time (memory efficient)
    item_iterator = catalog.get_items(recursive=True)
    
    for item in item_iterator:
        # Resolve relative asset URLs to absolute S3 URLs
        resolve_asset_urls(item, s3_base_url, catalog_dir)
        
        # Create synthetic GRQ dataset record
        grq_dataset = create_grq_dataset_from_stac_item(item, job)
        grq_datasets.append(grq_dataset)
        all_grq_datasets.append(grq_dataset)
        
        # Prepare for STAC API
        stac_items_for_api.append(item.to_dict())
        
        # Process in batches for large catalogs
        if len(grq_datasets) >= batch_size:
            bulk_index_dataset(app.conf.GRQ_UPDATE_URL_BULK, grq_datasets)
            index_items_to_stac_api(stac_items_for_api)
            logger.info(f"Processed batch of {len(grq_datasets)} STAC items")
            grq_datasets.clear()
            stac_items_for_api.clear()
    
    # Process any remaining items in the last batch
    if grq_datasets:
        bulk_index_dataset(app.conf.GRQ_UPDATE_URL_BULK, grq_datasets)
        index_items_to_stac_api(stac_items_for_api)
        logger.info(f"Processed final batch of {len(grq_datasets)} STAC items")
    
    # Index collections to STAC API
    index_collections_to_stac_api(catalog)
    
    # Update job metrics
    update_job_metrics_for_stac(job, all_grq_datasets, {"status": "success"})
    
    return all_grq_datasets


def create_grq_dataset_from_stac_item(stac_item, job):
    """Convert STAC Item to GRQ-compatible dataset record."""
    
    # Extract asset URLs (now absolute S3 URLs)
    asset_urls = [asset.href for asset in stac_item.assets.values()]
    
    return {
        "id": stac_item.id,
        "dataset": "stac_item",
        "dataset_type": stac_item.collection_id,  # Collection ID → dataset_type
        "dataset_level": "item",
        "version": "v1.0",
        "label": f"STAC Item: {stac_item.id}",
        
        # Spatial/temporal from STAC item
        "location": stac_item.geometry,
        "starttime": stac_item.datetime.isoformat() + "Z" if stac_item.datetime else None,
        "endtime": stac_item.datetime.isoformat() + "Z" if stac_item.datetime else None,
        
        # URLs point to S3 (same as traditional datasets)
        "urls": asset_urls,
        "browse_urls": [],  # Could extract from assets if needed
        
        # STAC-specific metadata
        "stac_version": "1.0.0",
        "stac_item_id": stac_item.id,
        "stac_collection": stac_item.collection_id,
        
        # Copy all STAC properties as metadata
        **stac_item.properties,
        
        # Standard HySDS fields
        "creation_timestamp": datetime.now(timezone.utc).isoformat() + "Z",
        "system_version": "hysds-1.3.8"
    }


def resolve_asset_urls(stac_item, s3_base_url, catalog_dir):
    """Convert relative asset hrefs to absolute S3 URLs."""
    for asset_key, asset in stac_item.assets.items():
        if asset.href.startswith('./'):
            # Relative path → absolute S3 URL
            relative_path = asset.href[2:]  # Remove './'
            asset.href = f"{s3_base_url}/{relative_path}"
        # Absolute URLs remain unchanged


def upload_catalog_to_s3(catalog, catalog_dir, job, ctx):
    """Upload catalog directory to S3 using existing HySDS infrastructure."""
    
    # Create synthetic recognizer for S3 upload
    catalog_id = catalog.id
    datasets_cfg_file = job["job_info"]["datasets_cfg_file"]
    
    # Use stac_catalog dataset type for recognition
    recognizer = Recognizer(datasets_cfg_file, catalog_dir, catalog_id, "stac_catalog")
    
    synthetic_dataset = {
        "version": "v1.0",
        "label": f"STAC Catalog: {catalog.id}",
        "dataset_type": "stac_catalog",
        "creation_timestamp": datetime.now(timezone.utc).isoformat() + "Z"
    }
    recognizer.setDataset(synthetic_dataset)
    
    if not recognizer.publishConfigured():
        raise STACValidationError("No publish configuration found for stac_catalog dataset type")
    
    # Upload using existing Osaka infrastructure
    pub_path_url = recognizer.getPublishPath()
    
    # Get S3 credentials
    s3_secret_key, s3_access_key = recognizer.getS3Keys()
    s3_profile = recognizer.getS3Profile()
    
    osaka_params = {}
    if s3_profile:
        osaka_params["profile_name"] = s3_profile
    elif s3_secret_key and s3_access_key:
        osaka_params["aws_access_key_id"] = s3_access_key
        osaka_params["aws_secret_access_key"] = s3_secret_key
    
    try:
        write_to_object_store(
            catalog_dir,
            pub_path_url,
            params=osaka_params,
            force=ctx.get("_force_ingest", False)
        )
        logger.info(f"Uploaded STAC catalog to {pub_path_url}")
        return pub_path_url
    except Exception as e:
        raise STACValidationError(f"Failed to upload STAC catalog to S3: {e}")


def index_collections_to_stac_api(catalog):
    """Index STAC collections to standard STAC API using PUT (idempotent)."""
    
    stac_api_url = app.conf.get('STAC_API_URL')
    stac_api_key = app.conf.get('STAC_API_KEY')
    
    if not stac_api_url:
        raise STACAPIError("STAC_API_URL not configured")
    
    # Prepare headers with optional authentication
    headers = {"Content-Type": "application/json"}
    if stac_api_key:
        headers["X-Api-Key"] = stac_api_key
    
    # Upsert all collections using standard STAC API endpoints
    collections = list(catalog.get_all_collections())
    logger.info(f"Upserting {len(collections)} collections to STAC API")
    
    for collection in collections:
        collection_url = urljoin(stac_api_url, f"collections/{collection.id}")
        try:
            response = requests.put(
                collection_url,
                json=collection.to_dict(),
                headers=headers,
                timeout=app.conf.get('STAC_API_TIMEOUT', 30)
            )
            response.raise_for_status()
            logger.debug(f"Successfully upserted collection: {collection.id}")
            
        except requests.RequestException as e:
            raise STACAPIError(f"Failed to upsert collection {collection.id}: {e}")


def index_items_to_stac_api(stac_items):
    """Index STAC items to standard STAC API using PUT (idempotent)."""
    
    stac_api_url = app.conf.get('STAC_API_URL')
    stac_api_key = app.conf.get('STAC_API_KEY')
    
    if not stac_api_url:
        raise STACAPIError("STAC_API_URL not configured")
    
    # Prepare headers with optional authentication
    headers = {"Content-Type": "application/json"}
    if stac_api_key:
        headers["X-Api-Key"] = stac_api_key
    
    # Upsert all items using standard STAC API endpoints
    logger.info(f"Upserting {len(stac_items)} items to STAC API")
    
    for item_dict in stac_items:
        item_id = item_dict["id"]
        collection_id = item_dict["collection"]
        item_url = urljoin(stac_api_url, f"collections/{collection_id}/items/{item_id}")
        
        try:
            response = requests.put(
                item_url,
                json=item_dict,
                headers=headers,
                timeout=app.conf.get('STAC_API_TIMEOUT', 30)
            )
            response.raise_for_status()
            logger.debug(f"Successfully upserted item: {item_id}")
            
        except requests.RequestException as e:
            raise STACAPIError(f"Failed to upsert item {item_id}: {e}")


def update_job_metrics_for_stac(job, grq_datasets, stac_api_results):
    """Update job metrics with STAC item information."""
    
    if "products_staged" not in job["job_info"]["metrics"]:
        job["job_info"]["metrics"]["products_staged"] = []
    
    stac_api_url = app.conf.get('STAC_API_URL', '')
    
    for grq_dataset in grq_datasets:
        item_id = grq_dataset["stac_item_id"]
        collection_id = grq_dataset["stac_collection"]
        
        # Job metrics contain STAC API URLs (not S3 URLs)
        stac_api_item_url = f"{stac_api_url}/collections/{collection_id}/items/{item_id}"
        
        job["job_info"]["metrics"]["products_staged"].append({
            "id": item_id,
            "urls": [stac_api_item_url],  # STAC API URL as requested
            "dataset_type": collection_id,
            "dataset": "stac_item",
            "path": grq_dataset.get("path", ""),
            "disk_usage": grq_dataset.get("disk_usage", 0),
            "stac_collection": collection_id
        })