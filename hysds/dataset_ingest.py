from future import standard_library

standard_library.install_aliases()
import json
import math
import os
import re
import shutil
import socket
import sys
import traceback
import types
from datetime import timezone, datetime
from glob import glob
from io import StringIO
from pprint import pformat, pprint
from subprocess import check_call, check_output
from tempfile import mkdtemp
from redis.exceptions import RedisError

from urllib.parse import urlparse

import backoff
import osaka
import requests
from fabric.api import env, get, put, run
from fabric.contrib.files import exists
from filechunkio import FileChunkIO
from lxml.etree import parse

import hysds
from hysds.celery import app
from hysds.log_utils import (
    backoff_max_tries,
    backoff_max_value,
    log_custom_event,
    log_prov_es,
    log_publish_prov_es,
    logger,
)

from hysds.publish_lock import PublishLock, DedupPublishLockFoundException
from hysds.recognize import Recognizer

from hysds.orchestrator import do_submit_job
from hysds.recognize import Recognizer
from hysds.utils import (
    dataset_exists,
    find_dataset_json,
    get_disk_usage,
    get_func,
    get_job_status,
    makedirs,
    parse_iso8601,
    is_task_finished,
    TaskNotFinishedException,
)

FILE_RE = re.compile(r"file://(.*?)(/.*)$")
SCRIPT_RE = re.compile(r"script:(.*)$")
BROWSE_RE = re.compile(r"^(.+)\.browse\.png$")


def verify_dataset(dataset):
    """Verify dataset JSON fields."""

    if "version" not in dataset:
        raise RuntimeError("Failed to find required field: version")
    for field in ("label", "location", "starttime", "endtime", "creation_timestamp"):
        if field not in dataset:
            logger.info(f"Optional field not found: {field}")


@backoff.on_exception(
    backoff.expo,
    (RuntimeError, requests.RequestException),
    max_tries=backoff_max_tries,
    max_value=backoff_max_value,
)
def index_dataset(grq_update_url, update_json):
    """Index dataset into GRQ ES."""

    r = requests.post(
        grq_update_url, verify=False, data={"dataset_info": json.dumps(update_json)}
    )
    if not 200 <= r.status_code < 300:
        raise RuntimeError(r.text)
    return r.json()


def queue_dataset(dataset, update_json, queue_name):
    """Add dataset type and URL to queue."""

    payload = {"job_type": f"dataset:{dataset}", "payload": update_json}
    do_submit_job(payload, queue_name)


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


def get_remote(host, rpath):
    """Get remote dir/file."""

    env.host_string = host
    env.abort_on_prompts = True
    r = get(rpath, ".")
    return os.path.abspath(f"./{os.path.basename(rpath)}")


def move_remote_path(host, src, dest):
    """Move remote directory safely."""

    env.host_string = host
    env.abort_on_prompts = True
    dest_dir = os.path.dirname(dest)
    if not exists(dest_dir):
        run(f"mkdir -p {dest_dir}")
    ret = run(f"mv -f {src} {dest}")
    return ret


def restage(host, src, dest, signal_file):
    """Restage dataset and create signal file."""

    env.host_string = host
    env.abort_on_prompts = True
    dest_dir = os.path.dirname(dest)
    if not exists(dest_dir):
        run(f"mkdir -p {dest_dir}")
    run(f"mv -f {src} {dest}")
    ret = run(f"touch {signal_file}")
    return ret


class NoClobberPublishContextException(Exception):
    pass


def publish_dataset(prod_dir, dataset_file, job, ctx):
    """Publish a dataset. Track metrics."""

    # get job info
    job_dir = job["job_info"]["job_dir"]
    time_start_iso = job["job_info"]["time_start"]
    context_file = job["job_info"]["context_file"]
    datasets_cfg_file = job["job_info"]["datasets_cfg_file"]

    # time start
    time_start = parse_iso8601(time_start_iso)

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
    tx_t1 = datetime.now(timezone.utc)

    metrics, prod_json = ingest(
        *(
            prod_id,
            datasets_cfg_file,
            app.conf.GRQ_UPDATE_URL,
            app.conf.DATASET_PROCESSED_QUEUE,
            prod_dir,
            job_dir,
        ),
        **ingest_kwargs,
    )
    tx_t2 = datetime.now(timezone.utc)
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

    job["job_info"]["metrics"]["products_staged"].append(
        {
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
    )

    return prod_json


def publish_datasets(job, ctx):
    """Perform dataset publishing if job exited with zero status code."""

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

    # get job info
    job_dir = job["job_info"]["job_dir"]

    # find and publish
    published_prods = []

    for dataset_file, prod_dir in find_dataset_json(job_dir):

        # skip if marked as localized input
        signal_file = os.path.join(prod_dir, ".localized")
        if os.path.exists(signal_file):
            logger.info(f"Skipping publish of {prod_dir}. Marked as localized input.")
            continue

        # publish
        prod_json = publish_dataset(prod_dir, dataset_file, job, ctx)

        # save json for published product
        published_prods.append(prod_json)

    # write published products to file
    pub_prods_file = os.path.join(job_dir, "_datasets.json")
    with open(pub_prods_file, "w") as f:
        json.dump(published_prods, f, indent=2, sort_keys=True)

    # signal run_job() to continue
    return True


# TODO: this used to be called publish_dataset()
def write_to_object_store(
    path, url, params=None, force=False, publ_ctx_file=None, publ_ctx_url=None
):
    """
    Publish a dataset to the given url
    @param path - path of dataset to publish
    @param url - url to publish to
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
            publish_lock = PublishLock()
            try:
                try:
                    publish_lock.acquire_lock(dest_url)
                except DedupPublishLockFoundException as dpe:
                    logger.warning(f"{str(dpe)}")
                logger.info("Uploading %s to %s." % (abs_path, dest_url))
                osaka.main.put(abs_path, dest_url, params=params, noclobber=True)
                try:
                    status = publish_lock.release()
                    if status is False:
                        logger.warning(f"No lock was found for {dest_url}")
                    else:
                        logger.info(f"Successfully released lock for {dest_url}")
                except RedisError as re:
                    logger.warning(
                        f"Redis error occurred while trying to release lock for {dest_url}: {str(re)}"
                    )
            finally:
                try:
                    publish_lock.close()
                except RedisError as re:
                    logger.warning(
                        f"Redis error occurred while trying to close client connection: {str(re)}"
                    )

# TODO: this used to be called unpublish_dataset()
def delete_from_object_store(url, params=None):
    """
    Remove a dataset at (and below) the given url
    @param url - url to remove files (at and below)
    """

    # set osaka params
    if params is None:
        params = {}

    osaka.main.rmall(url, params=params)


def ingest(
    objectid,
    dsets_file,
    grq_update_url,
    dataset_processed_queue,
    prod_path,
    job_path,
    dry_run=False,
    force=False,
):
    """Run dataset ingest."""
    logger.info("#" * 80)
    logger.info(f"datasets: {dsets_file}")
    logger.info(f"grq_update_url: {grq_update_url}")
    logger.info(f"dataset_processed_queue: {dataset_processed_queue}")
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

    # get ipath
    ipath = r.getIpath()

    # get level
    level = r.getLevel()

    # get type
    dtype = r.getType()

    # set product metrics
    prod_metrics = {"ipath": ipath, "path": local_prod_path}

    publish_context_lock = None

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

        # set osaka params
        osaka_params = {}

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
        tx_t1 = datetime.now(timezone.utc)
        if dry_run:
            logger.info(f"Would've published {local_prod_path} to {pub_path_url}")
        else:
            publ_ctx_url = os.path.join(pub_path_url, publ_ctx_name)
            orig_publ_ctx_file = publ_ctx_file + ".orig"
            try:
                # Acquire lock first before trying to write to the object store
                if task_id:
                    try:
                        publish_context_lock = PublishLock()
                        lock_status = publish_context_lock.acquire_lock(publish_url=publ_ctx_url)
                        if lock_status is True:
                            logger.info(f"Successfully acquired lock for {publ_ctx_url}")
                    except DedupPublishLockFoundException as dpe:
                        logger.error(f"{str(dpe)}")
                        raise NoClobberPublishContextException(f"{str(dpe)}")
                    except RedisError as re:
                        logger.warning(f"Redis error occurred while trying to acquire lock: {str(re)}")

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
                    if publish_context_lock:
                        try:
                            publish_context_lock.release()
                        except RedisError as re:
                            logger.warning(
                                f"Failed to release lock: {str(re)}"
                            )
                        try:
                            publish_context_lock.close()
                        except RedisError as re:
                            logger.warning(
                                f"Failed to release lock or close Redis client connection properly: {str(re)}"
                            )
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
                    if orig_task_id and task_id and orig_task_id != task_id:
                        try:
                            status = is_task_finished(orig_task_id)
                            if status is True:
                                logger.info(f"Task {orig_task_id} is finished. Proceeding with force publish.")
                            else:
                                logger.warning(
                                    f"Could not determine status of {orig_task_id}. Proceeding with force publish."
                                )
                        except TaskNotFinishedException as te:
                            error_message = (
                                f"Task {orig_task_id} still isn't finished: {str(te)}. "
                                f"Will not proceed with force publish."
                            )
                            logger.error(error_message)
                            raise TaskNotFinishedException(error_message) from e
                        except requests.exceptions.RequestException as re:
                            logger.warning(
                                f"Could not determine status of {orig_task_id} due to request exception: {str(re)}."
                                f" Proceeding with force publish."
                            )

                    # Check to see if the dataset exists. If so, then raise the error at this point
                    if dataset_exists(objectid):
                        error_message = f"Dataset already exists: {objectid}. No need to force publish."
                        logger.error(error_message)
                        if publish_context_lock:
                            try:
                                publish_context_lock.release()
                            except RedisError as re:
                                logger.warning(
                                    f"Failed to release lock: {str(re)}"
                                )
                            try:
                                publish_context_lock.close()
                            except RedisError as re:
                                logger.warning(
                                    f"Failed to release lock or close Redis client connection properly: {str(re)}"
                                )
                        raise NoClobberPublishContextException(error_message) from e

                    msg = (
                        "This job is a retry of a previous job that resulted "
                        + "in an orphaned dataset. Forcing publish."
                    )
                    logger.warning(msg)
                    log_custom_event(
                        "orphaned_dataset-retry_previous_failed",
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
                            f"Will not try and force publish as the other job with id {orig_payload_id} "
                            f"has job_status='job-started'"
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
                            if publish_context_lock:
                                try:
                                    publish_context_lock.release()
                                except RedisError as re:
                                    logger.warning(
                                        f"Failed to release lock: {str(re)}"
                                    )
                                try:
                                    publish_context_lock.close()
                                except RedisError as re:
                                    logger.warning(
                                        f"Failed to release lock or close Redis client connection properly: {str(re)}"
                                    )
                            raise

                # Let's try to acquire the lock again if we have not yet at this point
                if publish_context_lock.get_lock_status() is None or publish_context_lock.get_lock_status() is False:
                    try:
                        lock_status = publish_context_lock.acquire_lock(publish_url=publ_ctx_url)
                        if lock_status is True:
                            logger.info(
                                f"Successfully acquired lock prior to force publish for {publ_ctx_url}."
                            )
                    except Exception as e:
                        logger.warning(
                            f"Could not successfully acquire lock:\n{str(e)}.\nContinuing on with force publishing."
                        )
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
                    if publish_context_lock:
                        try:
                            publish_context_lock.release()
                        except RedisError as re:
                            logger.warning(
                                f"Failed to release lock: {str(re)}"
                            )
                        try:
                            publish_context_lock.close()
                        except RedisError as re:
                            logger.warning(
                                f"Failed to release lock or close Redis client connection properly: {str(re)}"
                            )
                    error_message = f"Dataset already exists: {objectid}. No need to force publish."
                    logger.error(error_message)
                    raise NoClobberPublishContextException(error_message) from e
                else:
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
                    # Let's try to acquire the lock again if we have not yet at this point
                    if publish_context_lock.get_lock_status() is None or publish_context_lock.get_lock_status() is False:
                        try:
                            lock_status = publish_context_lock.acquire_lock(
                                publish_url=publ_ctx_url,
                            )
                            if lock_status is True:
                                logger.info(
                                    f"Successfully acquired lock prior to force publish for {publ_ctx_url}."
                                )
                        except Exception as e:
                            logger.warning(
                                f"Could not successfully acquire lock:\n{str(e)}.\nContinuing on with force publishing."
                            )
                    write_to_object_store(
                        local_prod_path,
                        pub_path_url,
                        params=osaka_params,
                        force=True,
                        publ_ctx_file=publ_ctx_file,
                        publ_ctx_url=publ_ctx_url,
                    )
        tx_t2 = datetime.now(timezone.utc)
        tx_dur = (tx_t2 - tx_t1).total_seconds()

        # save dataset metrics on size and transfer
        prod_metrics.update(
            {
                "url": urlparse(pub_path_url).path,
                "disk_usage": prod_dir_usage,
                "time_start": tx_t1.isoformat() + "Z",
                "time_end": tx_t2.isoformat() + "Z",
                "duration": tx_dur,
                "transfer_rate": prod_dir_usage / tx_dur,
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
    # logger.info("update_json: %s" % pformat(update_json))

    # custom index specified?
    index = r.getIndex()
    if index is not None:
        update_json["index"] = index

    # update GRQ
    if dry_run:
        update_json["grq_index_result"] = {"index": index}
        logger.info(
            "Would've indexed doc at %s: %s"
            % (grq_update_url, json.dumps(update_json, indent=2, sort_keys=True))
        )
    else:
        res = index_dataset(grq_update_url, update_json)
        logger.info(f"res: {res}")
        update_json["grq_index_result"] = res

    # finish if dry run
    if dry_run:
        try:
            shutil.rmtree(publ_ctx_dir)
        except:
            pass
        return prod_metrics, update_json

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
                if publish_context_lock:
                    try:
                        publish_context_lock.release()
                    except RedisError as re:
                        logger.warning(
                            f"Failed to release lock: {str(re)}"
                        )
                    try:
                        publish_context_lock.close()
                    except RedisError as re:
                        logger.warning(
                            f"Failed to release lock or close Redis client connection properly: {str(re)}"
                        )
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

    # cleanup publish context
    if publ_ctx_url is not None:
        try:
            osaka.main.rmall(publ_ctx_url, params=osaka_params)
        except:
            logger.warn(
                f"Failed to clean up publish context at {publ_ctx_url} on successful publish."
            )
        if publish_context_lock:
            try:
                status = publish_context_lock.release()
                if status is False:
                    logger.warning(f"No lock was found for {publ_ctx_url}")
                else:
                    logger.info(f"Successfully released lock for {publ_ctx_url}")
            except RedisError as re:
                logger.warning(
                    f"Redis error occured while trying to release lock for {publ_ctx_url}: {str(re)}"
                )
            try:
                publish_context_lock.close()
            except RedisError as e:
                logger.warning(f"Failed to close Redis client connection properly: {str(e)}")
    try:
        shutil.rmtree(publ_ctx_dir)
    except:
        pass

    # queue data dataset
    queue_dataset(ipath, update_json, dataset_processed_queue)

    # return dataset metrics and dataset json
    return prod_metrics, update_json
