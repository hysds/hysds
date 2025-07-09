from future import standard_library

standard_library.install_aliases()

import copy
import json
import os
import pprint
import re
import socket
import sys
import time
import traceback
import uuid
from datetime import timezone, datetime
from functools import lru_cache
from importlib import reload
from inspect import getfullargspec as getargspec
from string import Template

import backoff
from celery import uuid

from hysds.celery import app
from hysds.job_worker import run_job
from hysds.log_utils import (
    backoff_max_tries,
    backoff_max_value,
    ensure_hard_time_limit_gap,
    log_job_status,
    logger,
)
from hysds.user_rules_dataset import queue_dataset_evaluation
from hysds.user_rules_job import queue_finished_job
from hysds.utils import (
    NoDedupJobFoundException,
    error_handler,
    get_payload_hash,
    get_short_error,
    query_dedup_job,
    datetime_iso_naive,
)

# error template
ERROR_TMPL = Template("Error queueing job from $orch_queue: $error")

# job type regex
JOB_TYPE_RE = re.compile(r"^\w+?:(.*)$")

# data type regex
DATA_TYPE_RE = re.compile(r"^dataset:\w+?::data/(.*)$")


def get_timestamp(fraction=True):
    """Return the current date and time formatted for a message header."""

    (year, month, day, hh, mm, ss, wd, y, z) = time.gmtime()
    d = datetime.now(timezone.utc)
    if fraction:
        s = f"{d.year:04d}{d.month:02d}{d.day:02d}T{d.hour:02d}{d.minute:02d}{d.second:02d}.{d.microsecond}Z"
    else:
        s = f"{d.year:04d}{d.month:02d}{d.day:02d}T{d.hour:02d}{d.minute:02d}{d.second:02d}Z"
    return s


@lru_cache(maxsize=32)
def get_function(func_str, add_to_sys_path=None):
    """Automatically parse a function call string to import any libraries
    and return a pointer to the function.  Define add_to_sys_path to prepend a
    path to the modules path."""

    # check if we have to import a module
    libmatch = re.match(r"^((?:\w|\.)+)\.\w+\(?.*$", func_str)
    if libmatch:
        import_lib = libmatch.group(1)
        if add_to_sys_path:
            exec(f"import sys; sys.path.insert(1,'{add_to_sys_path}')")
        exec(f"import {import_lib}")
        exec(f"reload({import_lib})")

    # check there are args
    args_match = re.search(r"\((\w+)\..+\)$", func_str)
    if args_match:
        import_lib2 = args_match.group(1)
        if add_to_sys_path:
            exec(f"import sys; sys.path.insert(1,'{add_to_sys_path}')")
        exec(f"import {import_lib2}")
        exec(f"reload({import_lib2})")

    # return function
    return eval(func_str)


def get_job_id(job_name):
    return f"{job_name}-{get_timestamp()}"


class OrchestratorExecutionError(Exception):
    def __init__(self, message, job_status):
        self.message = message
        self.job_status = job_status
        super().__init__(message, job_status)

    def job_status(self):
        return self.job_status


@app.task
def submit_job(j):
    """Submit HySDS job."""

    # get task_id and orchestrator queue
    task_id = submit_job.request.id
    orch_queue = submit_job.request.delivery_info.get("exchange", "unknown")

    # get container image name and url
    image_name = j.get("container_image_name", None)
    image_url = j.get("container_image_url", None)
    image_mapping = j.get("container_mappings", None)

    # get container runtime options
    runtime_options = j.get("runtime_options", None)

    # get hard/soft time limits
    time_limit = j.get("time_limit", None)
    soft_time_limit = j.get("soft_time_limit", None)

    # job dedup enabled?
    dedup = j.get("enable_dedup", True)

    # get priority
    priority = j.get("priority", None)
    if priority is None:
        priority = submit_job.request.delivery_info.get("priority")
        if priority is None:
            priority = 0

    # get tag
    tag = j.get("tag", None)

    # get username
    username = j.get("username", None)

    # default job json
    job = {
        "job_id": task_id,
        "name": task_id,
        "job_info": j,
    }

    current_time = datetime.now(timezone.utc)
    job["job_info"]["index"] = f"job_status-{current_time.strftime('%Y.%m.%d')}"

    # set job type
    if "job_type" in j:
        match = JOB_TYPE_RE.search(j["job_type"])
        job["type"] = match.group(1) if match else j["job_type"]

    # default context
    context = j.get("context", {})

    # get orchestrator configuration
    orch_cfg_file = os.environ.get("HYSDS_ORCHESTRATOR_CFG", None)
    if orch_cfg_file is None:
        error = "Environment variable HYSDS_ORCHESTRATOR_CFG is not set."
        error_info = ERROR_TMPL.substitute(orch_queue=orch_queue, error=error)
        job_status_json = {
            "uuid": job["job_id"],
            "job_id": job["job_id"],
            "payload_id": task_id,
            "status": "job-failed",
            "job": job,
            "context": context,
            "error": error_info,
            "short_error": get_short_error(error_info),
            "traceback": error_info,
        }
        log_job_status(job_status_json)
        raise OrchestratorExecutionError(error, job_status_json)

    # logger.info("HYSDS_ORCHESTRATOR_CFG:%s" % orch_cfg_file)
    if not os.path.exists(orch_cfg_file):
        error = f"Orchestrator configuration {orch_cfg_file} doesn't exist."
        error_info = ERROR_TMPL.substitute(orch_queue=orch_queue, error=error)
        job_status_json = {
            "uuid": job["job_id"],
            "job_id": job["job_id"],
            "payload_id": task_id,
            "status": "job-failed",
            "job": job,
            "context": context,
            "error": error_info,
            "short_error": get_short_error(error_info),
            "traceback": error_info,
        }
        log_job_status(job_status_json)
        raise OrchestratorExecutionError(error, job_status_json)

    with open(orch_cfg_file) as f:
        orch_cfg = json.load(f)

    # get job creators directory
    job_creators_dir = os.environ.get("HYSDS_JOB_CREATORS_DIR", None)
    if job_creators_dir is None:
        error = "Environment variable HYSDS_JOB_CREATORS_DIR is not set."
        error_info = ERROR_TMPL.substitute(orch_queue=orch_queue, error=error)
        job_status_json = {
            "uuid": job["job_id"],
            "job_id": job["job_id"],
            "payload_id": task_id,
            "status": "job-failed",
            "job": job,
            "context": context,
            "error": error_info,
            "short_error": get_short_error(error_info),
            "traceback": error_info,
        }
        log_job_status(job_status_json)
        raise OrchestratorExecutionError(error, job_status_json)
    # logger.info("HYSDS_JOB_CREATORS_DIR:%s" % job_creators_dir)

    # parse job configurations
    job_cfgs = {}
    for cfg in orch_cfg["configs"]:
        job_cfgs[cfg["job_type"]] = cfg["job_creators"]

    # check that we have info to create jobs
    if "job_type" not in j:
        error = "Invalid job spec. No 'job_type' specified."
        error_info = ERROR_TMPL.substitute(orch_queue=orch_queue, error=error)
        job_status_json = {
            "uuid": job["job_id"],
            "job_id": job["job_id"],
            "payload_id": task_id,
            "status": "job-failed",
            "job": job,
            "context": context,
            "error": error_info,
            "short_error": get_short_error(error_info),
            "traceback": error_info,
        }
        log_job_status(job_status_json)
        raise OrchestratorExecutionError(error, job_status_json)
    job_type = j["job_type"]
    job_queue = j.get("job_queue", None)

    if "payload" not in j:
        error = "Invalid job spec. No 'payload' specified."
        error_info = ERROR_TMPL.substitute(orch_queue=orch_queue, error=error)
        job_status_json = {
            "uuid": job["job_id"],
            "job_id": job["job_id"],
            "payload_id": task_id,
            "status": "job-failed",
            "job": job,
            "context": context,
            "error": error_info,
            "short_error": get_short_error(error_info),
            "traceback": error_info,
        }
        log_job_status(job_status_json)
        raise OrchestratorExecutionError(error, job_status_json)
    payload = j["payload"]
    # logger.info("got job_type: %s" % job_type)
    # logger.info("payload: %s" % payload)

    # set payload hash
    if j.get("payload_hash", None) is None:
        j["payload_hash"] = get_payload_hash(payload)
    payload_hash = j["payload_hash"]

    # do dedup
    if dedup is True:
        try:
            dj = query_dedup_job(payload_hash)
        except NoDedupJobFoundException as e:
            logger.info(str(e))
            dj = None
        if isinstance(dj, dict):
            dedup_msg = f"orchestrator found duplicate job {dj['_id']} with status {dj['status']}"
            job_status_json = {
                "uuid": job["job_id"],
                "job_id": job["job_id"],
                "payload_id": task_id,
                "payload_hash": payload_hash,
                "dedup": dedup,
                "dedup_job": dj["_id"],
                "status": "job-deduped",
                "job": job,
                "context": context,
                "dedup_msg": dedup_msg,
            }
            log_job_status(job_status_json)
            return [task_id]

    # if no explicit job or data type defined in orchestrator, add catch-all
    if job_type not in job_cfgs:
        # first check if data product type; if not then assume job type
        match = DATA_TYPE_RE.search(job_type)
        if match:
            return queue_dataset_evaluation(payload)
        else:
            match = JOB_TYPE_RE.search(job_type)
            jt = match.group(1) if match else job_type
            job_cfgs[job_type] = [
                {
                    "job_name": j.get("job_name", jt).replace(":", "__"),
                    "function": "utils.get_job_json",
                    "job_queues": [jt if job_queue is None else job_queue],
                }
            ]

    # get job json and queue jobs
    results = []
    for jc in job_cfgs[job_type]:
        func = get_function(jc["function"], add_to_sys_path=job_creators_dir)
        argspec = getargspec(func)
        try:
            if len(argspec.args) > 1 and "job_type" in argspec.args:
                match = JOB_TYPE_RE.search(job_type)
                jt = match.group(1) if match else job_type
                job = func(payload, jt)
            else:
                job = func(payload)
        except Exception as e:
            error = (
                f"Job creator function {jc['function']} failed to generate job JSON."
            )
            error_info = ERROR_TMPL.substitute(orch_queue=orch_queue, error=error)
            job_status_json = {
                "uuid": job["job_id"],
                "job_id": job["job_id"],
                "payload_id": task_id,
                "payload_hash": payload_hash,
                "dedup": dedup,
                "status": "job-failed",
                "job": {"job_id": task_id, "name": task_id, "job_info": j},
                "context": context,
                "error": error_info,
                "short_error": get_short_error(error_info),
                "traceback": traceback.format_exc(),
            }
            log_job_status(job_status_json)
            raise OrchestratorExecutionError(error, job_status_json)
        # logger.info("job: %s" % job)

        # set context
        job.setdefault("context", {}).update(context)

        # override hard/soft time limits and ensure gap
        soft_time_limit, time_limit = ensure_hard_time_limit_gap(
            jc.get("soft_time_limit", soft_time_limit), jc.get("time_limit", time_limit)
        )

        # queue jobs
        for queue in jc["job_queues"]:
            # copy job
            job_json = copy.deepcopy(job)

            # set job id
            if "name" in job:
                job_json["job_id"] = get_job_id(job["name"])
            else:
                job_json["job_id"] = get_job_id(jc["job_name"])
                job_json["name"] = job_json["job_id"]

            # set container image name, url, mappings, and runtime options
            if image_name is not None:
                job_json["container_image_name"] = image_name
            if image_url is not None:
                job_json["container_image_url"] = image_url
            if image_mapping is not None:
                job_json["container_mappings"] = image_mapping
            if runtime_options is not None:
                job_json["runtime_options"] = runtime_options

            # set priority
            job_json["priority"] = priority

            # set tag
            if "tag" not in job_json and tag is not None:
                job_json["tag"] = tag

            # set username
            if "username" not in job_json and username is not None:
                job_json["username"] = username

            # set job_info
            job_json["job_info"] = {
                "id": job_json["job_id"],
                "job_queue": queue,
                "time_queued": datetime_iso_naive(current_time) + "Z",
                "index": f"job_status-{current_time.strftime('%Y.%m.%d')}",
                "time_limit": time_limit,
                "soft_time_limit": soft_time_limit,
                "payload_hash": payload_hash,
                "dedup": dedup,
                "job_payload": {
                    "job_type": job_type,
                    "payload_task_id": task_id,
                },
            }

            # generate celery task id
            job_json["task_id"] = uuid()

            try:
                # submit job
                res = do_run_job(
                    job_json,
                    queue=queue,
                    time_limit=time_limit,
                    soft_time_limit=soft_time_limit,
                    priority=priority,
                )
                # log queued status
                job_status_json = {
                    "uuid": job_json["task_id"],
                    "job_id": job_json["job_id"],
                    "payload_id": task_id,
                    "payload_hash": payload_hash,
                    "dedup": dedup,
                    "status": "job-queued",
                    "job": job_json,
                }
                log_job_status(job_status_json)
                # append result
                results.append(job_json["task_id"])
            except Exception as e:
                # Set the job to job-failed if we could not queue up the job properly
                error_info = ERROR_TMPL.substitute(orch_queue=orch_queue, error=str(e))
                job_status_json = {
                    "uuid": job_json["task_id"],
                    "job_id": job_json["job_id"],
                    "payload_id": task_id,
                    "payload_hash": payload_hash,
                    "dedup": dedup,
                    "status": "job-failed",
                    "job": job_json,
                    "context": context,
                    "error": error_info,
                    "short_error": get_short_error(error_info),
                    "traceback": traceback.format_exc(),
                }
                log_job_status(job_status_json)
                queue_finished_job(task_id, index=job_json["job_info"]["index"])

    return results


@backoff.on_exception(
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def do_run_job(job_json, queue, time_limit, soft_time_limit, priority):
    """
    Run job wrapper with exponential backoff and full jitter.

    :param job_json:
    :param queue:
    :param time_limit:
    :param soft_time_limit:
    :param priority:
    :return:
    """
    return run_job.apply_async(
        (job_json,),
        queue=queue,
        time_limit=time_limit,
        soft_time_limit=soft_time_limit,
        priority=priority,
        task_id=job_json["task_id"],
    )


@backoff.on_exception(
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def do_submit_job(job_json, job_queue):
    """Submit job wrapper with exponential backoff and full jitter."""

    # list of allowed extensions
    extensions = [
        "priority",  # job priority
        # "expires",  # queue expiration; available in celery v4
    ]

    # set filtered extensions
    kwargs = {k: job_json[k] for k in job_json if k in extensions}

    # submit job
    return submit_job.apply_async((job_json,), queue=job_queue, **kwargs)
