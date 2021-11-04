from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import

from builtins import super
from builtins import dict
from builtins import open
from builtins import int
from builtins import str
from future import standard_library

standard_library.install_aliases()
import os
import sys
import time
import socket
import json
import traceback
import types
import requests
import shutil
import re
import shlex
import signal
from datetime import datetime
from itertools import compress
from subprocess import check_output, CalledProcessError
from celery.exceptions import SoftTimeLimitExceeded
from celery.signals import task_revoked

import hysds
from hysds.celery import app
from hysds.log_utils import (
    logger,
    log_job_status,
    log_job_info,
    get_job_status,
    log_task_worker,
    get_task_worker,
    get_worker_status,
    log_custom_event,
    is_revoked,
)

from hysds.utils import (
    disk_space_info,
    get_threshold,
    get_disk_usage,
    get_func,
    get_short_error,
    query_dedup_job,
    NoDedupJobFoundException,
    makedirs,
    find_dataset_json,
    find_cache_dir,
)
from hysds.container_utils import ensure_image_loaded, get_docker_params, get_docker_cmd
from hysds.pymonitoredrunner.MonitoredRunner import MonitoredRunner
from hysds.user_rules_job import queue_finished_job

from .base import Worker, WorkerExecutionError


@app.task
def run_job(job, queue_when_finished=True):
    """Function to execute a job."""

    if is_revoked(run_job.request.id):  # if job has been revoked in celery
        app.control.revoke(run_job.request.id, terminate=True)

    # write celery task id and delivery info
    job["task_id"] = run_job.request.id
    job["delivery_info"] = run_job.request.delivery_info
    job["celery_hostname"] = run_job.request.hostname  # TODO: add this here to use in the class

    worker = Worker(job)  # TODO: use a factory instead

    if app.conf.HYSDS_HANDLE_SIGNALS:
        worker.set_signal_handlers()

    if worker.redelivered_job_dup():
        logger.info("Encountered duplicate redelivered job:%s" % json.dumps(job))
        return

    log_task_worker(worker.task_id, worker.celery_hostname)  # set task worker

    workers_dir_abs = os.path.join(app.conf.ROOT_WORK_DIR, "workers")  # get workers dir
    worker.make_work_dir()

    worker.run_pre_job_steps_name_in_progress()  # runs most or all the previous steps
