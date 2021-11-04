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
from hysds.celery import app


class WorkerExecutionError(Exception):
    def __init__(self, message, job_status):
        self.message = message
        self.job_status = job_status
        super(WorkerExecutionError, self).__init__(message, job_status)

    def job_status(self):
        return self.job_status


class JobDedupedError(Exception):
    def __init__(self, message):
        self.message = message
        self.job_status = "job-deduped"
        super(JobDedupedError, self).__init__(message)

    def job_status(self):
        return self.job_status


class Worker:
    # RFC 1918 IPv4 address
    RFC_1918_RE = re.compile(r"^(?:10|127|172\.(?:1[6-9]|2[0-9]|3[01])|192\.168)\..*")

    # RFC 6598 IPv4 address (HC-280: Updated to support 100.64.x.x-100.127.x.x block)
    RFC_6598_RE = re.compile(r"^100\.(6[4-9]|[7-9][0-9]|1[0-1][0-9]|12[0-7])\..*")

    FACTS = None  # store facts

    FAILURE_RATE = app.conf.WORKER_CONTIGUOUS_FAILURE_THRESHOLD / app.conf.WORKER_CONTIGUOUS_FAILURE_TIME

    # facts to store
    FACTS_TO_TRACK = (
        "architecture", "domain",
        "ec2_instance_id", "ec2_instance_type", "ec2_placement_availability_zone", "ec2_public_hostname",
        "ec2_public_ipv4",
        "fqdn", "hardwaremodel", "hostname",
        "ipaddress", "ipaddress_eth0",
        "is_virtual",
        "memoryfree", "memorysize", "memorytotal",
        "operatingsystem", "operatingsystemrelease", "osfamily",
        "physicalprocessorcount", "processorcount",
        "swapfree", "swapsize",
        "uptime",
        "virtual",
    )

    SIG_NAMES = {
        1: "hangup",
        2: "interrupted",
        3: "quit",
        6: "aborted",
        9: "killed",
        15: "terminated",
    }

    JOB_QUEUED = "job-queued"
    JOB_FAILED = "job-failed"
    JOB_STARTED = "job-started"
    JOB_COMPLETED = "job-completed"
    JOB_DEDUPLICATED = "job-deduped"

    # built-in pre-processors
    PRE_PROCESSORS = (
        "hysds.utils.localize_urls",
        "hysds.utils.mark_localized_datasets",
        "hysds.utils.validate_checksum_files",
    )

    def __init__(self, job, **kwargs):
        """
        TODO: pay attention to signal handling
        :param job: Dict[any], job json from submit_job
        :param kwargs: additional information to pass into job_json
        """
        self.job = job  # dict type
        self.job_type = job["type"]  # str type
        self.job_id = job["job_id"]  # str type
        self.task_id = job["task_id"]  # str type: run_job.request.id
        self.delivery_info = job["delivery_info"]  # run_job.request.delivery_info

        self.payload_id = job["job_info"]["job_payload"]["payload_task_id"]  # get payload id
        self.payload_hash = job["job_info"]["payload_hash"]  # get payload hash
        self.celery_hostname = job["celery_hostname"]
        self.dedup = job["job_info"]["dedup"]  # get dedup flag
        self.context = job.get("context", {})  # get context

        self.time_split = time.gmtime()

        self.job_status_json = {
            "uuid": job["task_id"],
            "job_id": job["job_id"],
            "payload_id": self.payload_id,
            "payload_hash": self.payload_hash,
            "dedup": self.dedup,
            "status": self.JOB_FAILED,
            "job": job,
            "context": self.context,
            "celery_hostname": self.celery_hostname,
        }

        self.cmd_payload = self.job.get("params", {}).get("_command", None)  # get command payload
        logger.info("_command:%s" % self.cmd_payload)

        self.du_payload = self.job.get("params", {}).get("_disk_usage", None)  # get disk usage requirement
        logger.info("_disk_usage:%s" % self.du_payload)

        # get dependency images
        self.dependency_images = self.job.get("params", {}).get("job_specification", {}).get("dependency_images", [])
        logger.info("dependency_images:%s" % json.dumps(self.dependency_images))

        self.workers_dir = os.path.join(app.conf.ROOT_WORK_DIR, "workers")  # directory where all PGEs are located
        self.jd_file = os.path.join(self.workers_dir, "%s.failures.json" % self.celery_hostname)

        self.worker_cfg_file = os.environ.get("HYSDS_WORKER_CFG", None)
        self.worker_cfg = {}  # will be populated in validate_worker_config()
        self.work_cfgs = {}

        self.root_work_dir = None  # root directory: /data/work/
        self.cache_dir = None  # cache directory, ex. /data/work/cache/
        self.jobs_dir = None  # directory of all jobs, ex. /data/work/jobs/
        self.job_dir = None  # directory of the current job
        self.tasks_dir = None  # directory of tasks, ex. /data/work/tasks/
        self.jd_file = None  # <hostname>.failure.json file

        self.webdav_port = None
        self.webdav_url = None

        self.time_start = None  # job execution times
        self.time_end = None
        self.time_start_iso = None

        self.cmd_start = None  # command execution times
        self.cmd_end = None

        self.image_name = job.get("container_image_name", None)
        self.image_url = job.get("container_image_url", None)
        self.image_mappings = job.get("container_mappings", {})
        self.runtime_options = job.get("runtime_options", {})

        self.command = None
        self.cmd_line_list = None
        self.exec_env = dict(os.environ)

    def get_facts(self):
        """Return facts about worker instance."""

        # extract facts
        if self.FACTS is None:
            logger.info("Extracting FACTS")
            self.FACTS = {}
            try:
                facter_output = check_output(["facter", "--json"])
                facts = json.loads(facter_output)
                for fact_name in self.FACTS_TO_TRACK:
                    if fact_name in facts:
                        self.FACTS[fact_name] = facts[fact_name]
            except (OSError, CalledProcessError):
                pass

            # get public FQDN; regress to private hostname
            fqdn = None
            for fact in ("ec2_public_hostname", "ec2_public_ipv4", "fqdn"):
                if fact in self.FACTS:
                    fqdn = self.FACTS[fact]
                    break
            if fqdn is None:
                try:
                    fqdn = socket.getfqdn()
                except:
                    fqdn = ""
            self.FACTS["hysds_execute_node"] = fqdn

            # get public IPv4 address; regress to private address
            ip = None
            for fact in ("ec2_public_ipv4", "ipaddress_eth0", "ipaddress"):
                if fact in self.FACTS:
                    ip = self.FACTS[fact]
                    break
            if ip is None:
                try:
                    ip = socket.gethostbyname(socket.gethostname())
                except:
                    ip = ""
            if self.RFC_6598_RE.search(ip):
                self.FACTS["hysds_public_ip"] = ip
            else:
                dig_cmd = ["dig", "@resolver1.opendns.com", "+short", "myip.opendns.com"]
                try:
                    self.FACTS["hysds_public_ip"] = check_output(dig_cmd).strip().decode()
                except:
                    self.FACTS["hysds_public_ip"] = ip

        return self.FACTS

    @staticmethod
    def find_pge_metrics(work_dir):
        """Search for pge_metrics.json files."""

        met_re = re.compile(r"pge_metrics\.json$")
        for root, dirs, files in os.walk(work_dir, followlinks=True):
            files.sort()
            dirs.sort()
            for file in files:
                if met_re.search(file):
                    yield os.path.join(root, file)

    @staticmethod
    def find_usage_stats(work_dir):
        """Search for _docker_stats.json files."""

        stats_re = re.compile(r"_docker_stats\.json$")
        for root, dirs, files in os.walk(work_dir, followlinks=True):
            files.sort()
            dirs.sort()
            for file in files:
                if stats_re.search(file):
                    yield os.path.join(root, file)

    @staticmethod
    def cleanup_old_tasks(work_path, tasks_path, percent_free, threshold=10.0):
        """
        If free disk space is below percent threshold, start cleaning out old tasks.
        walks down os.walk(tasks_path)
            dont do anything if .done file is found
            clean the directory with shutil.rm(), then checks the work_path space usage afterwards
        rinse and repeat until percentage free > threshold
        :param work_path: str, path of work dir (ex. /data/work/...), used to track how much space is left on worker
        :param tasks_path: str, path of tasks (ex. /data/work/tasks/... ?)
        :param percent_free: float
        :param threshold: float (default 10.0)
        :return: float, percentage free space left
        """
        if percent_free <= threshold:
            logger.info("Searching for old task dirs to clean out to %02.2f%% free disk space." % threshold)
            for root, dirs, files in os.walk(tasks_path, followlinks=True):
                dirs.sort()  # TODO: is this sort needed? dirs isn't used after the sort
                if ".done" not in files:
                    continue
                logger.info("Cleaning out old task dir %s" % root)
                shutil.rmtree(root, ignore_errors=True)
                capacity, free, used, percent_free = disk_space_info(work_path)
                if percent_free > threshold:
                    logger.info("Successfully freed up disk space to %02.2f%%." % percent_free)
                    return percent_free
            if percent_free <= threshold:
                logger.warning("Failed to free up disk space to %02.2f%%." % threshold)
        return percent_free

    @staticmethod
    def evict_localize_cache(work_path, cache_path, percent_free, threshold=10.0):
        """
        If free disk space is below percent threshold, start evicting cache directories.
        :param work_path: str, path of work dir (ex. /data/work/...), used to track how much space is left on worker
        :param cache_path: (ex. /data/work/cache/...)
        :param percent_free: float
        :param threshold: float (default 10.0)
        :return:
        """

        if percent_free <= threshold:
            logger.info("Evicting cached dirs to clean out to %02.2f%% free disk space." % threshold)
            for timestamp, signal_file, cache_dir in find_cache_dir(cache_path):
                logger.info("Cleaning out cache dir %s" % cache_dir)
                shutil.rmtree(cache_dir, ignore_errors=True)
                capacity, free, used, percent_free = disk_space_info(work_path)
                if percent_free <= threshold:
                    continue
                logger.info("Successfully freed up disk space to %02.2f%%." % percent_free)
                return percent_free
            if percent_free <= threshold:
                logger.warning("Failed to free up disk space to %02.2f%%." % threshold)
        return percent_free

    @staticmethod
    def cleanup_old_jobs(work_path, jobs_path, percent_free, threshold=10.0, zero_status_only=False):
        """
        If free disk space is below percent threshold, start cleaning out old jobs.
        :param work_path: str, path of work dir (ex. /data/work/...), used to track how much space is left on worker
        :param jobs_path: str, path of tasks (ex. /data/work/jobs/... ?)
        :param percent_free: float
        :param threshold: float (default 10.0)
        :param zero_status_only:
        :return: float, percentage free space left
        """

        # TODO: how the job works (i think)
        #   walks down os.walk(jobs_path)
        #       dont do anything if .done or _context.json or _job.json file is found
        #       clean the directory with shutil.rm, then checks the work_path space usage afterwards
        #   rinse and repeat until percentage free >= threshold

        if percent_free <= threshold:
            logger.info("Searching for old job dirs to clean out to %02.2f%% free disk space." % threshold)
            for root, dirs, files in os.walk(jobs_path, followlinks=True):
                dirs.sort()  # TODO: is this sort needed? dirs isn't used after the sort
                if ".done" not in files or "_context.json" not in files or "_job.json" not in files:
                    continue
                # cleanup all or only jobs with exit code 0?
                if zero_status_only:
                    exit_code_file = os.path.join(root, "_exit_code")
                    if os.path.exists(exit_code_file):
                        with open(exit_code_file) as f:
                            exit_code = f.read()
                            try:
                                exit_code = int(exit_code)
                            except Exception as e:
                                logger.error("Failed to read exit code: %s %s" % (exit_code, str(e)))
                                exit_code = 1
                            if exit_code != 0:
                                continue

                logger.info("Cleaning out old job dir %s" % root)
                shutil.rmtree(root, ignore_errors=True)
                capacity, free, used, percent_free = disk_space_info(work_path)
                if percent_free <= threshold:
                    continue
                logger.info("Successfully freed up disk space to %02.2f%%." % percent_free)
                return percent_free
            if percent_free <= threshold:
                logger.warning("Failed to free up disk space to %02.2f%%." % threshold)
        return percent_free

    @classmethod
    def cleanup(cls, work_path, jobs_path, tasks_path, cache_path, threshold=10.0):
        """
        If free disk space <= percent threshold, start cleaning out old job and task directories & cached products
        :param work_path:
        :param jobs_path:
        :param tasks_path:
        :param cache_path:
        :param threshold:
        :return:
        """
        # log initial disk stats
        capacity, free, used, percent_free = disk_space_info(work_path)
        logger.info("Free disk space for %s: %02.2f%% (%dGB free/%dGB total)"
                    % (work_path, percent_free, free / 1024 ** 3, capacity / 1024 ** 3))
        logger.info("Configured free disk space threshold for this job type is %02.2f%%." % threshold)

        if percent_free > threshold:
            logger.info("No cleanup needed.")
            return

        percent_free = cls.cleanup_old_tasks(work_path, tasks_path, percent_free, threshold)  # cleanup tasks
        percent_free = cls.cleanup_old_jobs(work_path, jobs_path, percent_free, threshold, True)  # cleanup success jobs

        # cleanup jobs with non-zero exit codes if free disk space not met
        if percent_free <= threshold:
            percent_free = cls.cleanup_old_jobs(work_path, jobs_path, percent_free, threshold)

        # cleanup cached products if free disk space not met
        if percent_free <= threshold:
            cls.evict_localize_cache(work_path, cache_path, percent_free, threshold)

        # log final disk stats
        capacity, free, used, percent_free = disk_space_info(work_path)
        logger.info("Final free disk space for %s: %02.2f%% (%dGB free/%dGB total)"
                    % (work_path, percent_free, free / 1024 ** 3, capacity / 1024 ** 3))

    @staticmethod
    def shutdown_worker(celery_hostname):
        """Gracefully shutdown a celery worker."""
        app.control.broadcast("shutdown", destination=[celery_hostname])

    def job_drain_detected(self):
        """
        Detect if job drain is occurring
        :return: Boolean
        """
        status = self.job_status_json["status"]
        if status in ("job-completed", "job-deduped") and os.path.exists(self.jd_file):
            logger.info("Clearing job drain detector: %s" % self.jd_file)
            try:
                os.unlink(self.jd_file)
            except:
                pass
        elif status == self.JOB_FAILED:
            if os.path.exists(self.jd_file):
                with open(self.jd_file) as f:
                    jd = json.load(f)

                pid = os.getpid()
                if pid != jd["pid"]:  # reset if new worker process detected
                    jd = {
                        "count": 0,
                        "starttime": time.time(),
                        "pid": pid
                    }
                logger.info("Loaded job drain detector: %s" % self.jd_file)
                logger.info("Initial job drain detector payload: %s" % json.dumps(jd))
                logger.info("Incrementing failure count")
                jd["count"] += 1

                with open(self.jd_file, "w") as f:
                    json.dump(jd, f, indent=2, sort_keys=True)

                logger.info("Updated job drain detector: %s" % self.jd_file)
                fn = time.time()
                diff = fn - jd["starttime"]
                rate = jd["count"] / diff

                logger.info("Time diff since first failure (secs): %s" % diff)
                logger.info("Consecutive failure threshold: %s" % app.conf.WORKER_CONTIGUOUS_FAILURE_THRESHOLD)
                logger.info("Consecutive failure count: %s" % jd["count"])
                logger.info("Failure rate threshold (failures/sec): %s" % self.FAILURE_RATE)
                logger.info("Failure rate (failures/sec): %s" % rate)

                if jd["count"] > app.conf.WORKER_CONTIGUOUS_FAILURE_THRESHOLD and rate >= self.FAILURE_RATE:
                    logger.info("Job drain detected.")
                    return True
            else:
                jd = {
                    "count": 1,
                    "starttime": time.time(),
                    "pid": os.getpid()
                }
                with open(self.jd_file, "w") as f:
                    json.dump(jd, f, indent=2, sort_keys=True)
                logger.info("Created job drain detector: %s" % self.jd_file)
                logger.info("Job drain detector payload: %s" % json.dumps(jd))
        return False

    def redelivered_job_dup(self):
        """Return True if job is a duplicate redelivered job. False otherwise."""

        # task_id = self.job["task_id"]
        redelivered = self.job.get("delivery_info", {}).get("redelivered", False)
        status = get_job_status(self.task_id)
        logger.info("redelivered_job_dup: redelivered:%s status:%s" % (redelivered, status))

        if redelivered:
            # if job-started, give process_events time to process
            if status == "job-started":
                logger.info("Allowing process_events time to process")
                time.sleep(60)
                status = get_job_status(self.task_id)
                logger.info("redelivered_job_dup: redelivered:%s status:%s" % (redelivered, status))

            # perform dedup
            if status == "job-started":
                prev_worker = get_task_worker(self.task_id)
                prev_worker_status = get_worker_status(prev_worker)
                return False if prev_worker_status is None else True
            elif status == "job-completed":
                return True
            else:
                return False
        else:
            return False

    def fail_job(self, error):
        """
        Log failed job, detect/handle job drain and raise error.
        :param error: str
        :return: None, raise WorkerExecutionError
        """
        self.job_status_json["status"] = self.JOB_FAILED
        self.job_status_json["error"] = error
        self.job_status_json["short_error"] = get_short_error(error)
        self.job_status_json["traceback"] = error

        def_err = "Unspecified worker execution error."
        log_job_status(self.job_status_json)
        if self.job_drain_detected():
            log_custom_event("worker_anomaly", "job_drain", self.job_status_json)
            try:
                os.unlink(self.jd_file)
            except:
                pass
            self.shutdown_worker(self.job_status_json["celery_hostname"])
            time.sleep(30)  # give ample time for shutdown to come back
        raise WorkerExecutionError(self.job_status_json.get("error", def_err), self.job_status_json)

    def signal_handler(self, signum, frame):
        """
        when a signal error occurs, this function will log the job status
        https://docs.python.org/3/library/signal.html#signal.signal
        :param signum: signal type, ex. signal.SIGINT
        :param frame: current stack frame (None or a frame object)
        """
        error = "Signal handler for run_job() caught signal %d." % signum
        self.job_status_json["status"] = "job-%s" % self.SIG_NAMES.get(signum, "sig-%d" % signum)
        self.job_status_json["error"] = error
        self.job_status_json["signum"] = signum
        self.job_status_json["short_error"] = get_short_error(error)
        self.job_status_json["traceback"] = error
        log_job_status(self.job_status_json)
        raise WorkerExecutionError(error, self.job_status_json)

    def set_signal_handlers(self):
        """
        attaches the signal_handler() method (to the main thread) when any signals are raised
        https://docs.python.org/3/library/signal.html#signal.signal
        """
        signal_list = [signal.SIGHUP, signal.SIGINT, signal.SIGQUIT, signal.SIGABRT, signal.SIGTERM]
        for s in signal_list:
            signal.signal(s, self.signal_handler)

    def make_work_dir(self):
        """make work directory, ex. /data/work/..."""
        try:
            makedirs(self.workers_dir)
        except Exception as e:
            error = str(e)
            self.job_status_json["error"] = error
            self.job_status_json["short_error"] = get_short_error(error)
            self.job_status_json["traceback"] = traceback.format_exc()
            log_job_status(self.job_status_json)
            raise WorkerExecutionError(error, self.job_status_json)

    def validate_datasets_config(self):
        """Get datasets config"""

        datasets_cfg_file = os.environ.get("HYSDS_DATASETS_CFG", None)
        if datasets_cfg_file is None:
            error = "Environment variable HYSDS_DATASETS_CFG is not set."
            self.fail_job(error)
        self.job["job_info"]["datasets_cfg_file"] = datasets_cfg_file

        logger.info("HYSDS_DATASETS_CFG:%s" % datasets_cfg_file)
        if not os.path.exists(datasets_cfg_file):
            error = "Datasets configuration %s doesn't exist." % datasets_cfg_file
            self.fail_job(error)

    def validate_worker_config(self):
        """
        set and validate the worker_cfg (worker config) dictionary
        TODO: whenever we fail a job, should we exit out of the function, maybe return False
        """
        if self.worker_cfg_file is not None:
            error = "Environment variable HYSDS_WORKER_CFG is not set or job has no command payload."
            if not os.path.exists(self.worker_cfg_file) or self.cmd_payload is None:
                self.fail_job(error)
            else:
                try:
                    with open(self.worker_cfg_file) as f:
                        self.worker_cfg = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError, Exception):
                    self.fail_job(error)
        else:  # build worker config on-the-fly for command payload
            cmd_payload_list = shlex.split(self.cmd_payload)
            self.worker_cfg = {
                "configs": [{
                    "type": self.job_type,
                    "command": {
                        "path": cmd_payload_list[0],
                        "options": [],
                        "arguments": cmd_payload_list[1:],
                        "env": [],
                    },
                    "dependency_images": self.dependency_images,
                }]
            }

        if "HYSDS_ROOT_WORK_DIR" in os.environ:
            self.worker_cfg["root_work_dir"] = os.environ["HYSDS_ROOT_WORK_DIR"]
        if "HYSDS_WEBDAV_PORT" in os.environ:
            self.worker_cfg["webdav_port"] = os.environ["HYSDS_WEBDAV_PORT"]
        if "HYSDS_WEBDAV_URL" in os.environ:
            self.worker_cfg["webdav_url"] = os.environ["HYSDS_WEBDAV_URL"]
        if self.du_payload is not None:
            self.worker_cfg["configs"][0]["disk_usage"] = self.du_payload

        # structure of work_cfgs: { <job.type>: { ... }, <job.type>: { ... }, ... }
        self.work_cfgs = {cfg["type"]: cfg for cfg in self.worker_cfg["configs"]}
        if self.job_type not in self.work_cfgs:
            error = "No work configuration for type '%s'." % self.job_type
            self.fail_job(error)

        self.root_work_dir = self.worker_cfg.get("root_work_dir", app.conf.ROOT_WORK_DIR)
        self.tasks_dir = os.path.join(self.root_work_dir, "tasks")

        self.webdav_port = str(self.worker_cfg.get("webdav_port", app.conf.WEBDAV_PORT))
        self.webdav_url = self.worker_cfg.get("webdav_url", app.conf.get("WEBDAV_URL"))

        self.command = self.work_cfgs[self.job_type]["command"]
        self.job["command"] = self.command

    def make_cache_dir(self):
        self.cache_dir = os.path.join(self.root_work_dir, "cache")
        try:
            makedirs(self.cache_dir)
        except Exception as e:
            error = str(e)
            self.fail_job(error)

    def make_job_dir(self):
        """
        Set the job directory variable and create the job directory for the PGE
        Update the job_info dictionary with the job directory and metrics
        """
        self.jobs_dir = os.path.join(self.root_work_dir, "jobs")
        yr, mo, dy, hr, mi, se, wd, y, z = self.time_split
        self.job_dir = os.path.join(
            self.root_work_dir,
            self.jobs_dir,
            "%04d" % yr,
            "%02d" % mo,
            "%02d" % dy,
            "%02d" % hr,
            "%02d" % mi,
            self.job_id,
        )
        try:
            makedirs(self.job_dir)
        except Exception as e:
            error = str(e)
            self.fail_job(error)

        self.job["job_info"].update(
            {
                "job_dir": self.job_dir,
                "metrics": {
                    "inputs_localized": [],
                    "products_staged": [],
                    "job_dir_size": 0,
                    "usage_stats": [],
                },
            }
        )

    def create_job_running_file(self):
        """Create the .running file in the job directory"""
        job_running_file = os.path.join(self.job_dir, ".running")
        try:
            with open(job_running_file, "w") as f:
                f.write("%sZ\n" % datetime.utcnow().isoformat())
        except Exception as e:
            error = str(e)
            self.fail_job(error)

    def retrieve_facts(self):
        # get worker instance facts
        try:
            facts = self.get_facts()
            self.job["job_info"]["facts"] = facts  # add facts and IP info to job info
            self.job["job_info"]["execute_node"] = facts["hysds_execute_node"]
            self.job["job_info"]["public_ip"] = facts["hysds_public_ip"]
        except Exception as e:
            error = str(e)
            self.fail_job(error)

        # get availability zone, instance id and type
        # TODO: maybe this can be separated into a different class (since its difference between AWS GCP and Azure)
        # for md_url, md_name in (
        #         (AZ_INFO, "ec2_placement_availability_zone"),
        #         (INS_ID_INFO, "ec2_instance_id"),
        #         (INS_TYPE_INFO, "ec2_instance_type"),
        # ):
        #     try:
        #         r = requests.get(md_url, timeout=1)
        #         if r.status_code == 200:
        #             facts[md_name] = r.content.decode()
        #     except:
        #         pass

    def set_webdav_url(self):
        """
        add webdav url to job dir
        """
        if self.webdav_url is None:
            self.webdav_url = "http://%s:%s" % (self.job["job_info"]["public_ip"], self.webdav_port)

        yr, mo, dy, hr, mi, se, wd, y, z = self.time_split
        self.job["job_info"]["job_url"] = os.path.join(
            self.webdav_url,
            self.jobs_dir,
            "%04d" % yr,
            "%02d" % mo,
            "%02d" % dy,
            "%02d" % hr,
            "%02d" % mi,
            self.job_id,
        )

    def set_container_info(self):
        """
        Set or overwrite container image name, url and mappings if defined in work config
        """
        self.container_image_name = self.work_cfgs[self.job_type].get("container_image_name", None)
        self.container_image_url = self.work_cfgs[self.job_type].get("container_image_url", None)
        self.container_mappings = self.work_cfgs[self.job_type].get("container_mappings", {})
        self.runtime_options = self.work_cfgs[self.job_type].get("runtime_options", {})

        if self.container_image_name is not None and self.container_image_url is not None:
            self.job["container_image_name"] = self.container_image_name
            self.job["container_image_url"] = self.container_image_url
            self.job["container_mappings"] = self.container_mappings
            self.job["runtime_options"] = self.runtime_options
            logger.info("Setting image %s (%s) from worker cfg" % (self.container_image_name, self.container_image_url))
            logger.info("Using container mappings: %s" % self.container_mappings)
            logger.info("Using runtime options: %s" % self.runtime_options)

        # TODO: work on this part
        # set or overwrite dependency images
        dep_imgs = self.work_cfgs[self.job_type].get("dependency_images", [])
        if len(dep_imgs) > 0:
            self.job["dependency_images"] = dep_imgs
            logger.info("Setting dependency_images from worker configuration: %s" % self.job["dependency_images"])

    def write_context_file(self):
        """write empty context to file"""
        try:
            if len(self.context) > 0:
                self.context = {"context": self.context}
            self.context.update(self.job["params"])
            self.context["job_priority"] = self.job.get("priority", None)
            self.context["container_image_name"] = self.job.get("container_image_name", None)
            self.context["container_image_url"] = self.job.get("container_image_url", None)
            self.context["container_mappings"] = self.job.get("container_mappings", {})
            self.context["runtime_options"] = self.job.get("runtime_options", {})
            self.context["_prov"] = {"wasGeneratedBy": "task_id:{}".format(self.job["task_id"])}
            context_file = os.path.join(self.job_dir, "_context.json")

            with open(context_file, "w") as f:
                json.dump(self.context, f, indent=2, sort_keys=True)

            self.job["job_info"]["context_file"] = context_file
        except Exception as e:
            error = str(e)
            self.fail_job(error)

    def write_job_json_file(self):
        try:
            job_file = os.path.join(self.job_dir, "_job.json")
            with open(job_file, "w") as f:
                json.dump(self.job, f, indent=2, sort_keys=True)
        except Exception as e:
            error = str(e)
            self.fail_job(error)

    def run_pre_processor_steps(self):
        """
        :return:
        """
        pass

    def check_job_dedup(self):
        """
        do dedup check first
        :return:
        """
        if self.dedup is True:
            try:
                dj = query_dedup_job(self.payload_hash, filter_id=self.task_id, states=["job-started", "job-completed"],
                                     is_worker=True)
            except NoDedupJobFoundException as e:
                logger.info(str(e))
                dj = None

            if isinstance(dj, dict):
                error = "verdi worker found duplicate job %s with status %s" % (dj["_id"], dj["status"])
                # dedupJob = dj["_id"]
                raise JobDedupedError(error)

    def set_image_info(self):
        if self.image_name is not None:
            image_info = ensure_image_loaded(self.image_name, self.image_url, self.cache_dir)
            self.job["container_image_id"] = image_info["Id"]
            self.context["container_image_id"] = self.job["container_image_id"]
        for i, dep_img in enumerate(self.job.get("dependency_images", [])):
            dep_image_info = ensure_image_loaded(
                dep_img["container_image_name"],
                dep_img["container_image_url"],
                self.cache_dir,
            )
            dep_img["container_image_id"] = dep_image_info["Id"]
            ctx_dep_img = self.context["job_specification"]["dependency_images"][i]
            ctx_dep_img["container_image_id"] = dep_img["container_image_id"]

        # update context file with image ids
        context_file = os.path.join(self.job_dir, "_context.json")
        with open(context_file, "w") as f:
            json.dump(self.context, f, indent=2, sort_keys=True)

    def run_pre_job_steps_name_in_progress(self):
        """
        runs all the pre pre processor steps
        from the run_job function
            - makes work directory; /data/work/
            - validates worker_cfg data and setting the root work directory, etc.
            - make cache directory; /data/work/cache/
            - cleans up the work directory to allocates enough space needed on the worker node
            - creates job directory; /data/work/jobs/<yyyy>/<mm>/<dd>/<hh>/<mm>/<s>/...
            - creates a .running file in the job work directory to show that the job is ready to start
            - runs the get_facts() function to get node/worker information
        :return:
        """
        if self.redelivered_job_dup() is True:
            logger.info("Encountered duplicate redelivered job:%s" % json.dumps(self.job))
            # return {
            #     "uuid": self.task_id,
            #     "job_id": self.job_id,
            #     "payload_id": self.payload_id,
            #     "payload_hash": self.payload_hash,
            #     "dedup": self.dedup,
            #     "status": "job-deduped",
            #     "celery_hostname": self.celery_hostname,
            # }
            return

        log_task_worker(self.task_id, self.celery_hostname)
        self.make_work_dir()
        self.validate_worker_config()
        self.make_cache_dir()

        disk_usage = self.work_cfgs[self.job_type].get("disk_usage", self.du_payload)
        threshold = 10.0 if disk_usage is None else get_threshold(self.root_work_dir, disk_usage)
        self.cleanup(self.root_work_dir, self.jobs_dir, self.tasks_dir, self.cache_dir, threshold=threshold)

        self.make_job_dir()
        self.create_job_running_file()

        self.get_facts()

        self.set_webdav_url()
        self.set_container_info()
        self.write_context_file()
        self.write_job_json_file()

        self.check_job_dedup()
        self.set_image_info()

    def run_pre_processing_steps(self):
        """
        Run pre-processing steps
            - iterates through the function names from PRE_PROCESSORS
            - retrieves the function logic through get_func() and executes it
        """
        disable_pre = (
            self.job.get("params", {})
                .get("job_specification", {})
                .get("disable_pre_builtins", False)
        )
        pre_processors = [] if disable_pre else list(self.PRE_PROCESSORS)
        pre_processors.extend(
            self.job.get("params", {}).get("job_specification", {}).get("pre", [])
        )
        pre_processor_sigs = []
        for pre_processor in pre_processors:
            func = get_func(pre_processor)
            logger.info("Running pre-processor: %s" % pre_processor)
            pre_processor_sigs.append(func(self.job, self.context))

    def set_command_list(self):
        self.cmd_line_list = [self.job["command"]["path"]]

        for opt in self.job["command"]["options"]:
            self.cmd_line_list.append(opt)

        for arg in self.job["command"]["arguments"]:
            match_arg = re.search(r"^\$(\w+)$", arg)
            if match_arg:
                arg = self.job["params"][match_arg.group(1)]
            if isinstance(arg, (list, tuple)):
                self.cmd_line_list.extend(arg)
            else:
                self.cmd_line_list.append(arg)
