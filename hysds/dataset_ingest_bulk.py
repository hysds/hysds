from future import standard_library

standard_library.install_aliases()

import json
import logging
import os
import re
import shutil
import traceback
from datetime import timezone, datetime
from glob import glob
from importlib import import_module
from io import StringIO
from subprocess import check_output
from tempfile import mkdtemp
from urllib.parse import urlparse

import backoff
import osaka.main
import requests
from billiard import Manager, get_context  # noqa
from billiard.pool import Pool, cpu_count  # noqa
from lxml.etree import parse

from hysds.celery import app
from hysds.log_utils import (
    backoff_max_tries,
    backoff_max_value,
    log_custom_event,
    log_prov_es,
    log_publish_prov_es,
    logger,
)
from hysds.orchestrator import do_submit_job
from hysds.recognize import Recognizer
from hysds.utils import (
    dataset_exists,
    find_non_localized_datasets,
    get_disk_usage,
    get_job_status,
    makedirs,
    is_task_finished,
    TaskNotFinishedException
)

FILE_RE = re.compile(r"file://(.*?)(/.*)$")
SCRIPT_RE = re.compile(r"script:(.*)$")
BROWSE_RE = re.compile(r"^(.+)\.browse\.png$")


class NoDedupJobFoundException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)


class NoClobberPublishContextException(Exception):
    pass


class NotAllProductsIngested(Exception):
    pass


def get_module(m):
    """Import module and return."""

    try:
        return import_module(m)
    except ImportError:
        logger.error(f'Failed to import module "{m}".')
        raise


def get_disk_usage(path, follow_symlinks=True):
    """Return disk usage size in bytes."""

    opts = "-sbL" if follow_symlinks else "-sb"
    size = 0
    try:
        size = int(check_output(["du", opts, path]).split()[0])
    except:
        pass
    return size


def get_remote_dav(url):
    """Get remote dir/file."""

    lpath = f"./{os.path.basename(url)}"
    if not url.endswith("/"):
        url += "/"
    parsed_url = urlparse(url)
    rpath = parsed_url.path
    r = requests.request("PROPFIND", url, verify=False)
    if r.status_code not in (200, 207):  # handle multistatus (207) as well
        logger.info(f"Got status code {r.status_code} trying to read {url}")
        logger.info(f"Content:\n{r.text}")
        r.raise_for_status()
    tree = parse(StringIO(r.content))
    makedirs(lpath)
    for elem in tree.findall("{DAV:}response"):
        collection = elem.find(
            "{DAV:}propstat/{DAV:}prop/{DAV:}resourcetype/{DAV:}collection"
        )
        if collection is not None:
            continue
        href = elem.find("{DAV:}href").text
        rel_path = os.path.relpath(href, rpath)
        file_url = os.path.join(url, rel_path)
        local_path = os.path.join(lpath, rel_path)
        local_dir = os.path.dirname(local_path)
        makedirs(local_dir)
        resp = requests.request("GET", file_url, verify=False, stream=True)
        if resp.status_code != 200:
            logger.info(f"Got status code {resp.status_code} trying to read {file_url}")
            logger.info(f"Content:\n{resp.text}")
        resp.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
    return os.path.abspath(lpath)


def verify_dataset(dataset):
    """Verify dataset JSON fields."""

    if "version" not in dataset:
        raise RuntimeError("Failed to find required field: version")
    for field in ("label", "location", "starttime", "endtime", "creation_timestamp"):
        if field not in dataset:
            logger.info(f"Optional field not found: {field}")


def delete_from_object_store(url, params=None):
    """
    Remove a dataset at (and below) the given url
    @param url - url to remove files (at and below)
    @param params - osaka.main.rmall parameters - https://github.com/hysds/osaka/blob/develop/osaka/main.py#L151
    """

    # set osaka params
    if params is None:
        params = {}

    osaka.main.rmall(url, params=params)


def write_to_object_store(
    path, url, params=None, force=False, publ_ctx_file=None, publ_ctx_url=None
):
    """
    Publish a dataset to the given url
    @param path - path of dataset to publish
    @param url - url to publish to
    @param params - osaka.main.put params - https://github.com/hysds/osaka/blob/develop/osaka/main.py#L29
    @param force - unpublish dataset first if exists
    @param publ_ctx_file - publish context file
    @param publ_ctx_url - url to publish context file to
    """

    # set osaka params
    if params is None:
        params = {}

    # force remove previous dataset if it exists?
    if force:
        try:
            delete_from_object_store(url, params=params)
        except:
            pass

    # write publish context file
    if publ_ctx_file is not None and publ_ctx_url is not None:
        try:
            osaka.main.put(publ_ctx_file, publ_ctx_url, params=params, noclobber=True)
        except osaka.utils.NoClobberException as e:
            raise NoClobberPublishContextException(
                f"Failed to clobber {publ_ctx_url} when noclobber is True."
            )

    # upload datasets
    for root, dirs, files in os.walk(path):
        for file in files:
            abs_path = os.path.join(root, file)
            rel_path = os.path.relpath(abs_path, path)
            dest_url = os.path.join(url, rel_path)
            logger.info(f"Uploading {abs_path} to {dest_url}.")
            osaka.main.put(abs_path, dest_url, params=params, noclobber=True)

def parse_iso8601(t):
    """Return datetime from ISO8601 string."""

    try:
        return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ")


def ingest_to_object_store(
    objectid, dsets_file, prod_path, job_path, dry_run=False, force=False
):
    """Run dataset ingest."""
    logger.info(f"datasets: {dsets_file}")
    logger.info(f"prod_path: {prod_path}")
    logger.info(f"job_path: {job_path}")
    logger.info(f"dry_run: {dry_run}")
    logger.info(f"force: {force}")

    # get default job path
    if job_path is None:
        job_path = os.getcwd()

    # detect job info
    job = {}
    job_json = os.path.join(job_path, "_job.json")
    if os.path.exists(job_json):
        with open(job_json) as f:
            try:
                job = json.load(f)
            except Exception as e:
                logger.warning(f"Failed to read job json:\n{str(e)}")
    task_id = job.get("task_id", None)
    payload_id = (
        job.get("job_info", {}).get("job_payload", {}).get("payload_task_id", None)
    )
    payload_hash = job.get("job_info", {}).get("payload_hash", None)
    logger.info(f"task_id: {task_id}")
    logger.info(f"payload_id: {payload_id}")
    logger.info(f"payload_hash: {payload_hash}")

    # get dataset
    if os.path.isdir(prod_path):
        local_prod_path = prod_path
    else:
        local_prod_path = get_remote_dav(prod_path)
    if not os.path.isdir(local_prod_path):
        raise RuntimeError(f"Failed to find local dataset directory: {local_prod_path}")

    # write publish context
    publ_ctx_name = "_publish.context.json"
    publ_ctx_dir = mkdtemp(prefix=".pub_context", dir=job_path)
    publ_ctx_file = os.path.join(publ_ctx_dir, publ_ctx_name)
    with open(publ_ctx_file, "w") as f:
        json.dump(
            {
                "payload_id": payload_id,
                "payload_hash": payload_hash,
                "task_id": task_id,
            },
            f,
            indent=2,
            sort_keys=True,
        )
    publ_ctx_url = None

    # dataset name
    pname = os.path.basename(local_prod_path)

    # dataset file
    dataset_file = os.path.join(local_prod_path, f"{pname}.dataset.json")

    # get dataset json
    with open(dataset_file) as f:
        dataset = json.load(f)
    logger.info(f"Loaded dataset JSON from file: {dataset_file}")

    # check minimum requirements for dataset JSON
    logger.info("Verifying dataset JSON...")
    verify_dataset(dataset)
    logger.info("Dataset JSON verfication succeeded.")

    # get version
    version = dataset["version"]

    # recognize
    r = Recognizer(dsets_file, local_prod_path, objectid, version)

    # get extractor
    extractor = r.getMetadataExtractor()
    if extractor is not None:
        match = SCRIPT_RE.search(extractor)
        if match:
            extractor = match.group(1)
    logger.info(f"Configured metadata extractor: {extractor}")

    # metadata file
    metadata_file = os.path.join(local_prod_path, f"{pname}.met.json")

    # metadata seed file
    seed_file = os.path.join(local_prod_path, "met.json")

    # metadata file already here
    if os.path.exists(metadata_file):
        with open(metadata_file) as f:
            metadata = json.load(f)
        logger.info(f"Loaded metadata from existing file: {metadata_file}")
    else:
        if extractor is None:
            logger.info("No metadata extraction configured. Setting empty metadata.")
            metadata = {}
        else:
            logger.info(f"Running metadata extractor {extractor} on {local_prod_path}")
            m = check_output([extractor, local_prod_path])
            logger.info(f"Output: {m.decode()}")

            # generate json to update metadata and urls
            metadata = json.loads(m)

            # set data_product_name
            metadata["data_product_name"] = objectid

            # merge with seed metadata
            if os.path.exists(seed_file):
                with open(seed_file) as f:
                    seed = json.load(f)
                metadata.update(seed)
                logger.info(f"Loaded seed metadata from file: {seed_file}")

            # write it out to file
            with open(metadata_file, "w") as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"Wrote metadata to {metadata_file}")

            # delete seed file
            if os.path.exists(seed_file):
                os.unlink(seed_file)
                logger.info(f"Deleted seed file {seed_file}.")

    # read context
    context_file = os.path.join(local_prod_path, f"{pname}.context.json")
    if os.path.exists(context_file):
        with open(context_file) as f:
            context = json.load(f)
        logger.info(f"Loaded context from existing file: {context_file}")
    else:
        context = {}

    # set metadata and dataset groups in recognizer
    r.setDataset(dataset)
    r.setMetadata(metadata)

    ipath = r.getIpath()  # get ipath
    level = r.getLevel()  # get level
    dtype = r.getType()  # get type

    # set product metrics
    prod_metrics = {"ipath": ipath, "path": local_prod_path}

    osaka_params = {}  # set osaka params

    # publish dataset
    if r.publishConfigured():
        logger.info("Dataset publish is configured.")

        # get publish path
        pub_path_url = r.getPublishPath()

        # get publish urls
        pub_urls = [i for i in r.getPublishUrls()]

        # get S3 profile name and api keys for dataset publishing
        s3_secret_key, s3_access_key = r.getS3Keys()
        s3_profile = r.getS3Profile()

        # S3 profile takes precedence over explicit api keys
        if s3_profile is not None:
            osaka_params["profile_name"] = s3_profile
        else:
            if s3_secret_key is not None and s3_access_key is not None:
                osaka_params["aws_access_key_id"] = s3_access_key
                osaka_params["aws_secret_access_key"] = s3_secret_key

        # get pub host and path
        logger.info(f"Configured pub host & path: {pub_path_url}")

        # check scheme
        if not osaka.main.supported(pub_path_url):
            raise RuntimeError(
                f"Scheme {urlparse(pub_path_url).scheme} is currently not supported."
            )

        # upload dataset to repo; track disk usage and start/end times of transfer
        prod_dir_usage = get_disk_usage(local_prod_path)
        tx_t1 = datetime.now(timezone.utc).replace(tzinfo=None)
        if dry_run:
            logger.info(f"Would've published {local_prod_path} to {pub_path_url}")
        else:
            publ_ctx_url = os.path.join(pub_path_url, publ_ctx_name)
            orig_publ_ctx_file = publ_ctx_file + ".orig"
            try:
                write_to_object_store(
                    local_prod_path,
                    pub_path_url,
                    params=osaka_params,
                    force=force,
                    publ_ctx_file=publ_ctx_file,
                    publ_ctx_url=publ_ctx_url,
                )
            except NoClobberPublishContextException as e:
                logger.warning(
                    f"A publish context file was found at {publ_ctx_url}. Retrieving."
                )
                osaka.main.get(publ_ctx_url, orig_publ_ctx_file, params=osaka_params)
                with open(orig_publ_ctx_file) as f:
                    orig_publ_ctx = json.load(f)
                logger.warning(
                    "original publish context: {}".format(
                        json.dumps(orig_publ_ctx, indent=2, sort_keys=True)
                    )
                )
                orig_payload_id = orig_publ_ctx.get("payload_id", None)
                orig_payload_hash = orig_publ_ctx.get("payload_hash", None)
                orig_task_id = orig_publ_ctx.get("task_id", None)
                logger.warning(f"orig payload_id: {orig_payload_id}")
                logger.warning(f"orig payload_hash: {orig_payload_hash}")
                logger.warning(f"orig task_id: {orig_task_id}")

                if orig_payload_id is None:
                    error_message = (
                        f"payload_id does not exist in {publ_ctx_url}. "
                        f"Cannot determine if we can force publish."
                    )
                    logger.error(error_message)
                    raise NoClobberPublishContextException(error_message) from e

                # overwrite if this job is a retry of the previous job
                if payload_id is not None and payload_id == orig_payload_id:
                    # We should check to see if the task_id of the job is different than the
                    # task_id in the publish_context file, the orig_task_id. If so,
                    # to mitigate race conditions, check to see if the orig_task_id is in a
                    # finished state before proceeding.
                    wait_for_task_to_finish = False
                    if orig_task_id and task_id and orig_task_id != task_id:
                        wait_for_task_to_finish = True
                        try:
                            status = is_task_finished(orig_task_id)
                            if status is True:
                                logger.info(f"Task {orig_task_id} is finished. Proceeding with force publish.")
                        except TaskNotFinishedException as te:
                            error_message = (
                                f"Task {orig_task_id} associated with {publ_ctx_url} still isn't finished: {str(te)}. "
                                f"Will not proceed with force publish."
                            )
                            logger.error(error_message)
                            raise TaskNotFinishedException(error_message) from e

                    # Check to see if the dataset exists. If so, then raise the error at this point
                    if dataset_exists(objectid):
                        error_message = f"Dataset already exists: {objectid}. No need to force publish."
                        logger.error(error_message)
                        raise NoClobberPublishContextException(error_message) from e

                    msg = (
                        "This job is a retry of a previous job that resulted "
                        + "in an orphaned dataset. Forcing publish."
                    )
                    event_type = "orphaned_dataset-retry_previous_failed"
                    if wait_for_task_to_finish is True:
                        event_type = "orphaned_dataset-retry_previous_failed_waited_for_other_task_to_finish"
                    logger.warning(msg)
                    log_custom_event(
                        event_type,
                        "clobber",
                        {
                            "orphan_info": {
                                "payload_id": payload_id,
                                "payload_hash": payload_hash,
                                "task_id": task_id,
                                "orig_payload_id": orig_payload_id,
                                "orig_payload_hash": orig_payload_hash,
                                "orig_task_id": orig_task_id,
                                "dataset_id": objectid,
                                "dataset_url": pub_path_url,
                                "msg": msg,
                            }
                        },
                    )
                else:
                    job_status = get_job_status(orig_payload_id)
                    logger.warning(f"orig job status: {job_status}")

                    # overwrite if previous job failed
                    if job_status == "job-failed":
                        msg = (
                            "Detected previous job failure that resulted in an "
                            + "orphaned dataset. Forcing publish."
                        )
                        logger.warning(msg)
                        log_custom_event(
                            "orphaned_dataset-job_failed",
                            "clobber",
                            {
                                "orphan_info": {
                                    "payload_id": payload_id,
                                    "payload_hash": payload_hash,
                                    "task_id": task_id,
                                    "orig_payload_id": orig_payload_id,
                                    "orig_payload_hash": orig_payload_hash,
                                    "orig_task_id": orig_task_id,
                                    "orig_status": job_status,
                                    "dataset_id": objectid,
                                    "dataset_url": pub_path_url,
                                    "msg": msg,
                                }
                            },
                        )
                    # If job is determined to be in a job-started state, do not force publish
                    elif job_status == "job-started":
                        error_message = (
                            f"job with payload_id={orig_payload_id} in {publ_ctx_url} has job_status='job-started'. "
                            f"Will not force publish to avoid possible clobbering."
                        )
                        logger.error(error_message)
                        raise NoClobberPublishContextException(error_message) from e
                    else:
                        # overwrite if dataset doesn't exist in grq
                        if not dataset_exists(objectid):
                            msg = "Detected orphaned dataset without ES doc. Forcing publish."
                            logger.warning(msg)
                            log_custom_event(
                                "orphaned_dataset-no_es_doc",
                                "clobber",
                                {
                                    "orphan_info": {
                                        "payload_id": payload_id,
                                        "payload_hash": payload_hash,
                                        "task_id": task_id,
                                        "dataset_id": objectid,
                                        "dataset_url": pub_path_url,
                                        "msg": msg,
                                    }
                                },
                            )
                        else:
                            error_message = f"Dataset already exists: {objectid}. No need to force publish."
                            logger.error(error_message)
                            raise NoClobberPublishContextException(error_message) from e
                write_to_object_store(
                    local_prod_path,
                    pub_path_url,
                    params=osaka_params,
                    force=True,
                    publ_ctx_file=publ_ctx_file,
                    publ_ctx_url=publ_ctx_url,
                )
            except osaka.utils.NoClobberException as e:
                if dataset_exists(objectid):
                    try:
                        osaka.main.rmall(publ_ctx_url, params=osaka_params)
                    except:
                        logger.warning(
                            "Failed to clean up publish context {} after attempting to clobber valid dataset.".format(
                                publ_ctx_url
                            )
                        )
                    error_message = f"Dataset already exists: {objectid}. No need to force publish.."
                    logger.error(error_message)
                    raise osaka.utils.NoClobberException(error_message) from e
                else:
                    msg = f"Detected orphaned dataset {objectid}, deleting from data store before re-publishing..."
                    logger.info(msg)
                    log_custom_event(
                        "orphaned_dataset-no_es_doc",
                        "clobber",
                        {
                            "orphan_info": {
                                "payload_id": payload_id,
                                "payload_hash": payload_hash,
                                "task_id": task_id,
                                "dataset_id": objectid,
                                "dataset_url": pub_path_url,
                                "msg": msg,
                            }
                        },
                    )
                    write_to_object_store(
                        local_prod_path,
                        pub_path_url,
                        params=osaka_params,
                        force=True,
                        publ_ctx_file=publ_ctx_file,
                        publ_ctx_url=publ_ctx_url,
                    )
        tx_t2 = datetime.now(timezone.utc).replace(tzinfo=None)
        tx_dur = (tx_t2 - tx_t1).total_seconds()

        # create PROV-ES JSON file for publish processStep
        prod_prov_es_file = os.path.join(
            local_prod_path, f"{os.path.basename(local_prod_path)}.prov_es.json"
        )
        pub_prov_es_bn = "publish.prov_es.json"
        if os.path.exists(prod_prov_es_file):
            pub_prov_es_file = os.path.join(local_prod_path, pub_prov_es_bn)
            prov_es_info = {}
            with open(prod_prov_es_file) as f:
                try:
                    prov_es_info = json.load(f)
                except Exception as e:
                    tb = traceback.format_exc()
                    raise RuntimeError(
                        f"Failed to load PROV-ES from {prod_prov_es_file}: {str(e)}\n{tb}"
                    )
            log_publish_prov_es(
                prov_es_info,
                pub_prov_es_file,
                local_prod_path,
                pub_urls,
                prod_metrics,
                objectid,
            )
            # upload publish PROV-ES file
            osaka.main.put(
                pub_prov_es_file,
                os.path.join(pub_path_url, pub_prov_es_bn),
                params=osaka_params,
                noclobber=False,
            )

        # save dataset metrics on size and transfer
        prod_metrics.update(
            {
                "url": urlparse(pub_path_url).path,
                "disk_usage": prod_dir_usage,
                "time_start": tx_t1.isoformat() + "Z",
                "time_end": tx_t2.isoformat() + "Z",
                "duration": tx_dur,
                "transfer_rate": prod_dir_usage / tx_dur,
                "pub_path_url": pub_path_url,
            }
        )
    else:
        logger.info("Dataset publish is not configured.")
        pub_urls = []

    # publish browse
    if r.browseConfigured():
        logger.info("Browse publish is configured.")

        # get browse path and urls
        browse_path = r.getBrowsePath()
        browse_urls = r.getBrowseUrls()

        # get S3 profile name and api keys for browse image publishing
        s3_secret_key_browse, s3_access_key_browse = r.getS3Keys("browse")
        s3_profile_browse = r.getS3Profile("browse")

        # set osaka params for browse
        osaka_params_browse = {}

        # S3 profile takes precedence over explicit api keys
        if s3_profile_browse is not None:
            osaka_params_browse["profile_name"] = s3_profile_browse
        else:
            if s3_secret_key_browse is not None and s3_access_key_browse is not None:
                osaka_params_browse["aws_access_key_id"] = s3_access_key_browse
                osaka_params_browse["aws_secret_access_key"] = s3_secret_key_browse

        # add metadata for all browse images and upload to browse location
        imgs_metadata = []
        imgs = glob(f"{local_prod_path}/*browse.png")
        for img in imgs:
            img_metadata = {"img": os.path.basename(img)}
            small_img = img.replace("browse.png", "browse_small.png")
            if os.path.exists(small_img):
                small_img_basename = os.path.basename(small_img)
                if browse_path is not None:
                    this_browse_path = os.path.join(browse_path, small_img_basename)
                    if dry_run:
                        logger.info(f"Would've uploaded {small_img} to {browse_path}")
                    else:
                        logger.info(f"Uploading {small_img} to {browse_path}")
                        osaka.main.put(
                            small_img,
                            this_browse_path,
                            params=osaka_params_browse,
                            noclobber=False,
                        )
            else:
                small_img_basename = None
            img_metadata["small_img"] = small_img_basename
            tooltip_match = BROWSE_RE.search(img_metadata["img"])
            if tooltip_match:
                img_metadata["tooltip"] = tooltip_match.group(1)
            else:
                img_metadata["tooltip"] = ""
            imgs_metadata.append(img_metadata)

        # sort browse images
        browse_sort_order = r.getBrowseSortOrder()
        if isinstance(browse_sort_order, list) and len(browse_sort_order) > 0:
            bso_regexes = [re.compile(i) for i in browse_sort_order]
            sorter = {}
            unrecognized = []
            for img in imgs_metadata:
                matched = None
                for i, bso_re in enumerate(bso_regexes):
                    if bso_re.search(img["img"]):
                        matched = img
                        sorter[i] = matched
                        break
                if matched is None:
                    unrecognized.append(img)
            imgs_metadata = [sorter[i] for i in sorted(sorter)]
            imgs_metadata.extend(unrecognized)
        prod_metrics.update({"browse_path": browse_path})
    else:
        logger.info("Browse publish is not configured.")
        browse_urls = []
        imgs_metadata = []

    # set update json
    update_json = {
        "id": objectid,
        "objectid": objectid,
        "metadata": metadata,
        "dataset": ipath.split("/")[1],
        "ipath": ipath,
        "system_version": version,
        "dataset_level": level,
        "dataset_type": dtype,
        "urls": pub_urls,
        "browse_urls": browse_urls,
        "images": imgs_metadata,
        "prov": context.get("_prov", {}),
    }
    update_json.update(dataset)

    # custom index specified?
    index = r.getIndex()
    if index is not None:
        update_json["index"] = index

    # update GRQ
    update_json["grq_index_result"] = {"index": index}

    # finish if dry run
    if dry_run:
        try:
            shutil.rmtree(publ_ctx_dir)
        except:
            pass
        return prod_metrics, update_json

    # cleanup publish context
    if publ_ctx_url is not None:
        try:
            osaka.main.rmall(publ_ctx_url, params=osaka_params)
        except:
            logger.warning(
                f"Failed to clean up publish context at {publ_ctx_url} on successful publish."
            )
    try:
        shutil.rmtree(publ_ctx_dir)
    except:
        pass

    return prod_metrics, update_json


@backoff.on_exception(
    backoff.expo,
    (NotAllProductsIngested, requests.RequestException),
    max_tries=backoff_max_tries,
    max_value=backoff_max_value,
)
def bulk_index_dataset(grq_update_url, update_jsons):
    """
    Call GRQ rest API to bulk index datasets to elasticsearch
    :param grq_update_url:  # GRQ rest API endpoint
    :param update_jsons: List[Dict] list of objects to
    :return: Dict[any]
    """
    r = requests.post(grq_update_url, verify=False, json=json.dumps(update_jsons))
    if not 200 <= r.status_code < 300:
        raise NotAllProductsIngested(r.text)
    return r.json()


def queue_dataset(dataset, update_json, queue_name):
    """Add dataset type and URL to queue."""
    payload = {"job_type": f"dataset:{dataset}", "payload": update_json}
    do_submit_job(payload, queue_name)


def init_pool_logger():
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(asctime)s: %(levelname)s/%(name)s] %(message)s")
    )
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)


def publish_files_wrapper(job, ctx, prod_dir, event=None):
    """
    publish single dataset given the product directory, can be used in both multiprocessing or synchronous
    :param job [Dict] - job object
    :param ctx [Dict] - job context
    :param prod_dir [Str] - str; product directory
    :param event [Event, Optional] - Event to halt tasks if previous failed, taken from multiprocessing Manager()
    """
    if event and event.is_set():
        logger.warning(f"Previous publish task failed, skipping {prod_dir}...")
        return

    try:
        # get job info
        job_dir = job["job_info"]["job_dir"]
        time_start_iso = job["job_info"]["time_start"]
        context_file = job["job_info"]["context_file"]
        datasets_cfg_file = job["job_info"]["datasets_cfg_file"]

        time_start = parse_iso8601(time_start_iso)  # time start

        # check for PROV-ES JSON from PGE; if exists, append related PROV-ES info;
        # also overwrite merged PROV-ES JSON file
        prod_id = os.path.basename(prod_dir)
        prov_es_file = os.path.join(prod_dir, f"{prod_id}.prov_es.json")
        prov_es_info = {}
        if os.path.exists(prov_es_file):
            with open(prov_es_file) as f:
                try:
                    prov_es_info = json.load(f)
                except Exception as e:
                    tb = traceback.format_exc()
                    raise RuntimeError(
                        f"Failed to log PROV-ES from {prov_es_file}: {str(e)}\n{tb}"
                    )
            log_prov_es(job, prov_es_info, prov_es_file)

        # copy _context.json
        prod_context_file = os.path.join(prod_dir, f"{prod_id}.context.json")
        shutil.copy(context_file, prod_context_file)

        # force ingest? (i.e. disable no-clobber)
        ingest_kwargs = {"force": False}
        if ctx.get("_force_ingest", False):
            logger.info("Flag _force_ingest set to True.")
            ingest_kwargs["force"] = True

        # upload
        tx_t1 = datetime.now(timezone.utc).replace(tzinfo=None)
        metrics, prod_json = ingest_to_object_store(
            *(
                prod_id,
                datasets_cfg_file,
                prod_dir,
                job_dir,
            ),
            **ingest_kwargs,
        )

        tx_t2 = datetime.now(timezone.utc).replace(tzinfo=None)
        tx_dur = (tx_t2 - tx_t1).total_seconds()
        prod_dir_usage = get_disk_usage(prod_dir)

        # set product provenance
        prod_prov = {
            "product_type": metrics["ipath"],
            "processing_start_time": time_start.isoformat() + "Z",
            "availability_time": tx_t2.isoformat() + "Z",
            "processing_latency": (tx_t2 - time_start).total_seconds() / 60.0,
            "total_latency": (tx_t2 - time_start).total_seconds() / 60.0,
        }
        prod_prov_file = os.path.join(prod_dir, f"{prod_id}.prod_prov.json")
        if os.path.exists(prod_prov_file):
            with open(prod_prov_file) as f:
                prod_prov.update(json.load(f))
        if "acquisition_start_time" in prod_prov:
            if "source_production_time" in prod_prov:
                prod_prov["ground_system_latency"] = (
                    parse_iso8601(prod_prov["source_production_time"])
                    - parse_iso8601(prod_prov["acquisition_start_time"])
                ).total_seconds() / 60.0
                prod_prov["total_latency"] += prod_prov["ground_system_latency"]
                prod_prov["access_latency"] = (
                    tx_t2 - parse_iso8601(prod_prov["source_production_time"])
                ).total_seconds() / 60.0
                prod_prov["total_latency"] += prod_prov["access_latency"]
        # write product provenance of the last product; not writing to an array under the
        # product because kibana table panel won't show them correctly:
        # https://github.com/elasticsearch/kibana/issues/998
        job["job_info"]["metrics"]["product_provenance"] = prod_prov

        product_staged_metadata = {
            "path": prod_dir,
            "disk_usage": prod_dir_usage,
            "time_start": tx_t1.isoformat() + "Z",
            "time_end": tx_t2.isoformat() + "Z",
            "duration": tx_dur,
            "transfer_rate": prod_dir_usage / tx_dur,
            "id": prod_json["id"],
            "urls": prod_json["urls"],
            "browse_urls": prod_json["browse_urls"],
            "dataset": prod_json["dataset"],
            "ipath": prod_json["ipath"],
            "system_version": prod_json["system_version"],
            "dataset_level": prod_json["dataset_level"],
            "dataset_type": prod_json["dataset_type"],
            "index": prod_json["grq_index_result"]["index"],
        }

        return prod_json, product_staged_metadata, metrics
    except Exception as e:
        if event:
            event.set()
        tb = traceback.format_exc()
        logger.error(tb)
        raise RuntimeError(f"Failed to publish {prod_dir}: {str(e)}\n{tb}")


def delete_files(metrics):
    if "pub_path_url" in metrics:
        logger.info(f"deleting {metrics['pub_path_url']}")
        delete_from_object_store(metrics["pub_path_url"])
    if "browse_path" in metrics:
        logger.info(f"deleting {metrics['browse_path']}")
        delete_from_object_store(metrics["browse_path"])


def publish_datasets_parallel(job, ctx):
    """Publish a dataset. Track metrics."""

    # if exit code of job command is non-zero, don't publish anything
    exit_code = job["job_info"]["status"]
    if exit_code != 0:
        logger.info(
            f"Job exited with exit code {exit_code}. Bypassing dataset publishing."
        )
        return True

    # if job command never ran, don't publish anything
    pid = job["job_info"]["pid"]
    if pid == 0:
        logger.info("Job command never ran. Bypassing dataset publishing.")
        return True

    job_dir = job["job_info"]["job_dir"]
    datasets_list = find_non_localized_datasets(job_dir)

    prods_ingested_to_obj_store = []
    published_prods = []  # find and publish

    async_tasks = []
    num_procs = min(max(cpu_count() - 2, 1), len(datasets_list))
    logger.info(f"multiprocessing procs used: {num_procs}")

    with get_context("spawn").Pool(
        num_procs, initializer=init_pool_logger
    ) as pool, Manager() as manager:
        event = manager.Event()
        for prod_dir in datasets_list:
            signal_file = os.path.join(
                prod_dir, ".localized"
            )  # skip if marked as localized input
            if os.path.exists(signal_file):
                logger.info(
                    f"Skipping publish of {prod_dir}. Marked as localized input."
                )
                continue
            async_task = pool.apply_async(
                publish_files_wrapper,
                args=(
                    job,
                    ctx,
                    prod_dir,
                ),
                kwds={"event": event},
            )
            async_tasks.append(async_task)
        pool.close()
        logger.info("Waiting for dataset publishing tasks to complete...")
        pool.join()
        logger.handlers.clear()  # removing the handler to prevent broken pipe error

    has_error, err = False, ""
    for t in async_tasks:
        if t.successful():
            result = t.get()
            if result:
                prods_ingested_to_obj_store.append(result)
        else:
            has_error = True
            logger.error(t._value)  # noqa
            err = t._value  # noqa

    if has_error is True:
        with get_context("spawn").Pool(num_procs, initializer=init_pool_logger) as pool:
            for _, _, metrics in prods_ingested_to_obj_store:
                pool.apply_async(delete_files, args=(metrics,))
            pool.close()
            logger.warning("Rolling back datasets (file) ingest...")
            pool.join()
            logger.handlers.clear()
        raise NotAllProductsIngested(f"Product failed to ingest to data store: {err}")

    if len(prods_ingested_to_obj_store) > 0:
        try:
            prod_jsons = [prod_json for prod_json, _, _ in prods_ingested_to_obj_store]
            logger.info(
                f"publishing {len(prods_ingested_to_obj_store)} dataset(s) to Elasticsearch"
            )
            bulk_index_dataset(app.conf.GRQ_UPDATE_URL_BULK, prod_jsons)
            published_prods.extend(prod_jsons)
        except Exception:
            with get_context("spawn").Pool(
                num_procs, initializer=init_pool_logger
            ) as pool:
                for _, _, metrics in prods_ingested_to_obj_store:
                    pool.apply_async(delete_files, args=(metrics,))
            pool.close()
            logger.error(
                "datasets failed to publish to Elasticsearch, deleting object(s) from data store"
            )
            pool.join()
            logger.handlers.clear()
            raise NotAllProductsIngested(
                f"Products failed to index to elasticsearch: {traceback.format_exc()}"
            )

        if "products_staged" not in job["job_info"]["metrics"]:
            job["job_info"]["metrics"]["products_staged"] = []

        for prod_json, metadata, _ in prods_ingested_to_obj_store:
            ipath = prod_json["ipath"]
            queue_dataset(ipath, prod_json, app.conf.DATASET_PROCESSED_QUEUE)
            job["job_info"]["metrics"]["products_staged"].append(metadata)
        logger.info(
            f"queued {len(prods_ingested_to_obj_store)} dataset(s) to {app.conf.DATASET_PROCESSED_QUEUE}"
        )

    # write published products to file
    pub_prods_file = os.path.join(job_dir, "_datasets.json")
    with open(pub_prods_file, "w") as f:
        json.dump(published_prods, f, indent=2, sort_keys=True)

    return True


def publish_datasets(job, ctx):
    """Publish a dataset. Track metrics."""

    # if exit code of job command is non-zero, don't publish anything
    exit_code = job["job_info"]["status"]
    if exit_code != 0:
        logger.info(
            f"Job exited with exit code {exit_code}. Bypassing dataset publishing."
        )
        return True

    # if job command never ran, don't publish anything
    pid = job["job_info"]["pid"]
    if pid == 0:
        logger.info("Job command never ran. Bypassing dataset publishing.")
        return True

    job_dir = job["job_info"]["job_dir"]
    dataset_directories = find_non_localized_datasets(job_dir)

    prods_ingested_to_obj_store = []
    published_prods = []  # find and publish

    try:
        for prod_dir in dataset_directories:
            signal_file = os.path.join(
                prod_dir, ".localized"
            )  # skip if marked as localized input
            if os.path.exists(signal_file):
                logger.info(
                    f"Skipping publish of {prod_dir}. Marked as localized input."
                )
                continue
            published_metadata = publish_files_wrapper(job, ctx, prod_dir)
            prods_ingested_to_obj_store.append(published_metadata)
    except Exception:
        logger.error(
            f"Product failed to ingest to data store: {traceback.format_exc()}"
        )
        for _, _, metrics in prods_ingested_to_obj_store:
            delete_files(metrics)
        raise NotAllProductsIngested(
            f"Product failed to ingest to data store: {traceback.format_exc()}"
        )

    if len(prods_ingested_to_obj_store) > 0:
        try:
            prod_jsons = [prod_json for prod_json, _, _ in prods_ingested_to_obj_store]
            logger.info(
                f"publishing {len(prods_ingested_to_obj_store)} dataset(s) to Elasticsearch"
            )
            bulk_index_dataset(app.conf.GRQ_UPDATE_URL_BULK, prod_jsons)
            published_prods.extend(prod_jsons)
        except Exception:
            for _, _, metrics in prods_ingested_to_obj_store:
                delete_files(metrics)
            raise NotAllProductsIngested(
                f"Products failed to index to elasticsearch: {traceback.format_exc()}"
            )

        if "products_staged" not in job["job_info"]["metrics"]:
            job["job_info"]["metrics"]["products_staged"] = []

        for prod_json, metadata, _ in prods_ingested_to_obj_store:
            ipath = prod_json["ipath"]
            queue_dataset(ipath, prod_json, app.conf.DATASET_PROCESSED_QUEUE)
            job["job_info"]["metrics"]["products_staged"].append(metadata)
        logger.info(
            f"queued {len(prods_ingested_to_obj_store)} dataset(s) to {app.conf.DATASET_PROCESSED_QUEUE}"
        )

    # write published products to file
    pub_prods_file = os.path.join(job_dir, "_datasets.json")
    with open(pub_prods_file, "w") as f:
        json.dump(published_prods, f, indent=2, sort_keys=True)

    return True
