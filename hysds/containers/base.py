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

import os
import time
import shutil
import traceback
import json
import socket
import re
from subprocess import check_output, CalledProcessError
import signal

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


class ContainerBase:
    # RFC 1918 IPv4 address
    RFC_1918_RE = re.compile(r"^(?:10|127|172\.(?:1[6-9]|2[0-9]|3[01])|192\.168)\..*")

    # RFC 6598 IPv4 address (HC-280: Updated to support 100.64.x.x-100.127.x.x block)
    RFC_6598_RE = re.compile(r"^100\.(6[4-9]|[7-9][0-9]|1[0-1][0-9]|12[0-7])\..*")

    FACTS = None  # store facts

    FAILURE_RATE = app.conf.WORKER_CONTIGUOUS_FAILURE_THRESHOLD / app.conf.WORKER_CONTIGUOUS_FAILURE_TIME

    # facts to store
    FACTS_TO_TRACK = (
        "architecture",
        "domain",
        "ec2_instance_id",
        "ec2_instance_type",
        "ec2_placement_availability_zone",
        "ec2_public_hostname",
        "ec2_public_ipv4",
        "fqdn",
        "hardwaremodel",
        "hostname",
        "ipaddress",
        "ipaddress_eth0",
        "is_virtual",
        "memoryfree",
        "memorysize",
        "memorytotal",
        "operatingsystem",
        "operatingsystemrelease",
        "osfamily",
        "physicalprocessorcount",
        "processorcount",
        "swapfree",
        "swapsize",
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

    def __init__(self, job, **kwargs):
        """
        TODO: pay attention to signal handling
        :param job: Dict[any], job json from submit_job
        :param kwargs: additional information to pass into job_json
        """
        self.job = job
        self.task_id = job["task_id"]  # run_job.request.id
        self.delivery_info = job["delivery_info"]  # run_job.request.delivery_info

        self.payload_id = job["job_info"]["job_payload"]["payload_task_id"]  # get payload id
        self.payload_hash = job["job_info"]["payload_hash"]  # get payload hash
        self.celery_hostname = job["celery_hostname"]
        self.dedup = job["job_info"]["dedup"]  # get dedup flag
        self.context = job.get("context", {})  # get context

        self.job_status_json = {
            "uuid": job["task_id"],
            "job_id": job["job_id"],
            "payload_id": self.payload_id,
            "payload_hash": self.payload_hash,
            "dedup": self.dedup,
            "status": "job-failed",
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
        self.job_drain_file = os.path.join(self.workers_dir, "%s.failures.json" % self.celery_hostname)

    def add_task_metadata(self):
        pass

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
        :param work_path: str, path of work dir (ex. /data/work/...), used to track how much space is left on worker
        :param tasks_path: str, path of tasks (ex. /data/work/tasks/... ?)
        :param percent_free: float
        :param threshold: float (default 10.0)
        :return: float, percentage free space left
        """

        # TODO: how the job works (i think)
        #   walks down os.walk(tasks_path)
        #       dont do anything if .done file is found
        #       clean the directory with shutil.rm(), then checks the work_path space usage afterwards
        #   rinse and repeat until percentage free >= threshold

        if percent_free <= threshold:
            logger.info("Searching for old task dirs to clean out to %02.2f%% free disk space." % threshold)
            for root, dirs, files in os.walk(tasks_path, followlinks=True):
                dirs.sort()  # TODO: is this sort needed? dirs isn't used after the sort
                if ".done" not in files:
                    continue
                logger.info("Cleaning out old task dir %s" % root)
                shutil.rmtree(root, ignore_errors=True)
                capacity, free, used, percent_free = disk_space_info(work_path)
                if percent_free <= threshold:
                    continue
                logger.info("Successfully freed up disk space to %02.2f%%." % percent_free)
                return percent_free
            # TODO: maybe some duplicated logic here?
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
        logger.info(
            "Free disk space for %s: %02.2f%% (%dGB free/%dGB total)"
            % (work_path, percent_free, free / 1024 ** 3, capacity / 1024 ** 3)
        )
        logger.info("Configured free disk space threshold for this job type is %02.2f%%." % threshold)

        # cleanup needed?
        if percent_free > threshold:
            logger.info("No cleanup needed.")
            return

        # cleanup tasks
        percent_free = cls.cleanup_old_tasks(work_path, tasks_path, percent_free, threshold)

        # cleanup jobs with zero exit code
        percent_free = cls.cleanup_old_jobs(work_path, jobs_path, percent_free, threshold, True)

        # cleanup jobs with non-zero exit codes if free disk space not met
        if percent_free <= threshold:
            percent_free = cls.cleanup_old_jobs(work_path, jobs_path, percent_free, threshold)

        # cleanup cached products if free disk space not met
        if percent_free <= threshold:
            cls.evict_localize_cache(work_path, cache_path, percent_free, threshold)

        # log final disk stats
        capacity, free, used, percent_free = disk_space_info(work_path)
        logger.info(
            "Final free disk space for %s: %02.2f%% (%dGB free/%dGB total)"
            % (work_path, percent_free, free / 1024 ** 3, capacity / 1024 ** 3)
        )

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
        if status in ("job-completed", "job-deduped") and os.path.exists(self.job_drain_file):
            logger.info("Clearing job drain detector: %s" % self.job_drain_file)
            try:
                os.unlink(self.job_drain_file)
            except:
                pass
        elif status == "job-failed":
            if os.path.exists(self.job_drain_file):
                with open(self.job_drain_file) as f:
                    jd = json.load(f)

                pid = os.getpid()
                if pid != jd["pid"]:  # reset if new worker process detected
                    jd = {
                        "count": 0,
                        "starttime": time.time(),
                        "pid": pid
                    }
                logger.info("Loaded job drain detector: %s" % self.job_drain_file)
                logger.info("Initial job drain detector payload: %s" % json.dumps(jd))
                logger.info("Incrementing failure count")
                jd["count"] += 1

                with open(self.job_drain_file, "w") as f:
                    json.dump(jd, f, indent=2, sort_keys=True)

                logger.info("Updated job drain detector: %s" % self.job_drain_file)
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
                with open(self.job_drain_file, "w") as f:
                    json.dump(jd, f, indent=2, sort_keys=True)
                logger.info("Created job drain detector: %s" % self.job_drain_file)
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

    def fail_job(self):
        """Log failed job, detect/handle job drain and raise error."""

        def_err = "Unspecified worker execution error."
        log_job_status(self.job_status_json)
        if self.job_drain_detected():
            log_custom_event("worker_anomaly", "job_drain", self.job_status_json)
            try:
                os.unlink(self.job_drain_file)
            except:
                pass
            self.shutdown_worker(self.job_status_json["celery_hostname"])
            time.sleep(30)  # give ample time for shutdown to come back
        raise WorkerExecutionError(self.job_status_json.get("error", def_err), self.job_status_json)

    def signal_handler(self, signum, frame):
        """
        when a signal error occurs, this function will log the job to redis -> logstash -> es
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
        signal_list = [signal.SIGHUP, signal.SIGINT, signal.SIGQUIT, signal.SIGABRT, signal.SIGTERM]
        for s in signal_list:
            signal.signal(s, self.signal_handler)

    def make_work_dir(self):
        """make work directory"""
        try:
            makedirs(self.workers_dir)
        except Exception as e:
            error = str(e)
            self.job_status_json["error"] = error
            self.job_status_json["short_error"] = get_short_error(error)
            self.job_status_json["traceback"] = traceback.format_exc()

            log_job_status(self.job_status_json)
            raise WorkerExecutionError(error, self.job_status_json)

    def validate_worker_config(self):
        worker_cfg_file = os.environ.get("HYSDS_WORKER_CFG", None)

        if worker_cfg_file is None and self.cmd_payload is None:  # TODO: figure out where to put cmd payload
            error = "Environment variable HYSDS_WORKER_CFG is not set or job has no command payload."

            self.fail_job()
            self.job_status_json["status"] = "job-failed"
            self.job_status_json["error"] = error
            self.job_status_json["short_error"] = get_short_error(error)
            self.job_status_json["traceback"] = error

    # # TODO: guess its not needed here, can move this to job_worker.py
    # @app.task
    # def run_job(self, job, queue_when_finished=True):
    #     """
    #     Executes a job
    #     TODO: original function is very big, will need to break up into smaller manageable functions
    #     :param job: Dict[any], job metadata ex: {"job_info": {"job_payload": {"payload_task_id": { ... }}}}
    #     :param queue_when_finished: Boolean, will queue the job for user rules evaluation when completed
    #         TODO: i think this can be moved to a class variable(?) since its used only once in the OG run_job function
    #     :return:
    #     """
