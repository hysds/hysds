from __future__ import absolute_import

import os, sys, time, socket, json, traceback, types, requests, shutil, re, shlex
import signal
from datetime import datetime
from subprocess import check_output, CalledProcessError
from celery.exceptions import SoftTimeLimitExceeded
from celery.signals import task_revoked

import hysds
from hysds.celery import app
from hysds.log_utils import (logger, log_job_status, log_job_info, get_job_status, 
log_task_worker, get_task_worker, get_worker_status, log_custom_event)

from hysds.utils import (disk_space_info, get_threshold, get_disk_usage, get_func,
get_short_error, query_dedup_job, makedirs, find_dataset_json, find_cache_dir)
from hysds.container_utils import ensure_image_loaded, get_docker_params, get_docker_cmd
from hysds.pymonitoredrunner.MonitoredRunner import MonitoredRunner
from hysds.user_rules_job import queue_finished_job


# built-in pre-processors
PRE_PROCESSORS = (
    'hysds.utils.localize_urls',
    'hysds.utils.mark_localized_datasets',
)

# built-in post-processors
POST_PROCESSORS = (
    'hysds.utils.publish_datasets',
)

# signal names
SIG_NAMES = {
    1:  "hangup",
    2:  "interrupted",
    3:  "quit",
    6:  "aborted",
    9:  "killed",
    15: "terminated",
}

# instance metadata urls
AZ_INFO = "http://169.254.169.254/latest/meta-data/placement/availability-zone"
INS_TYPE_INFO = "http://169.254.169.254/latest/meta-data/instance-type"
INS_ID_INFO = "http://169.254.169.254/latest/meta-data/instance-id"

# store facts
FACTS = None

# facts to store
FACTS_TO_TRACK = (
    'architecture',
    'domain',
    'ec2_instance_id',
    'ec2_instance_type',
    'ec2_placement_availability_zone',
    'ec2_public_hostname',
    'ec2_public_ipv4',
    'fqdn',
    'hardwaremodel',
    'hostname',
    'ipaddress',
    'ipaddress_eth0',
    'is_virtual',
    'memoryfree',
    'memorysize',
    'memorytotal',
    'operatingsystem',
    'operatingsystemrelease',
    'osfamily',
    'physicalprocessorcount',
    'processorcount',
    'swapfree',
    'swapsize',
    'uptime',
    'virtual',
) 

# RFC 1918 IPv4 address
RFC_1918_RE = re.compile(r'^(?:10|127|172\.(?:1[6-9]|2[0-9]|3[01])|192\.168)\..*')

# RFC 6598 IPv4 address
RFC_6598_RE = re.compile(r'^100\.64\..*')

# job failure rate for job drain detection
FAILURE_RATE = app.conf.WORKER_CONTIGUOUS_FAILURE_THRESHOLD / app.conf.WORKER_CONTIGUOUS_FAILURE_TIME


def get_facts():
    """Return facts about worker instance."""

    # extract facts
    global FACTS
    if FACTS is None:
        logger.info("Extracting FACTS")
        FACTS = {}
        try:
            facter_output = check_output(['facter', '--json'])
            facts = json.loads(facter_output)
            for fact_name in FACTS_TO_TRACK:
                if fact_name in facts:
                    FACTS[fact_name] = facts[fact_name]
        except (OSError, CalledProcessError): pass

        # get public FQDN; regress to private hostname
        fqdn = None
        for fact in ('ec2_public_hostname', 'ec2_public_ipv4', 'fqdn'):
            if fact in FACTS:
                fqdn = FACTS[fact]
                break
        if fqdn is None:
            try: fqdn = socket.getfqdn()
            except: fqdn = ''
        FACTS['hysds_execute_node'] = fqdn

        # get public IPv4 address; regress to private address
        ip = None
        for fact in ('ec2_public_ipv4', 'ipaddress_eth0', 'ipaddress'):
            if fact in FACTS:
                ip = FACTS[fact]
                break
        if ip is None:
            try: ip = socket.gethostbyname(socket.gethostname())
            except: ip = ''
        if RFC_6598_RE.search(ip):
            FACTS['hysds_public_ip'] = ip
        else:
            dig_cmd = ['dig', '@resolver1.opendns.com', '+short', 'myip.opendns.com']
            try: FACTS['hysds_public_ip'] = check_output(dig_cmd).strip()
            except: FACTS['hysds_public_ip'] = ip

    return FACTS
    

def find_pge_metrics(work_dir):
    """Search for pge_metrics.json files."""

    met_re = re.compile(r'pge_metrics\.json$')
    for root, dirs, files in os.walk(work_dir, followlinks=True):
        files.sort()
        dirs.sort()
        for file in files:
            if met_re.search(file): yield os.path.join(root, file)


def cleanup(work_path, jobs_path, tasks_path, cache_path, threshold=10.):
    """If free disk space is below percent threshold, start cleaning out old job
       and task directories and cached products."""

    # log initial disk stats
    capacity, free, used, percent_free = disk_space_info(work_path)
    logger.info("Free disk space for %s: %02.2f%% (%dGB free/%dGB total)" % 
                (work_path, percent_free, free/1024**3, capacity/1024**3))
    logger.info("Configured free disk space threshold for this job type is %02.2f%%." % threshold)

    # cleanup needed?
    if percent_free > threshold:
        logger.info("No cleanup needed.")
        return

    # cleanup tasks
    percent_free = cleanup_old_tasks(work_path, tasks_path, percent_free, threshold)

    # cleanup jobs with zero exit code
    percent_free = cleanup_old_jobs(work_path, jobs_path, percent_free, threshold, True)

    # cleanup jobs with non-zero exit codes if free disk space not met
    if percent_free <= threshold:
        percent_free = cleanup_old_jobs(work_path, jobs_path, percent_free, threshold)

    # cleanup cached products if free disk space not met
    if percent_free <= threshold:
        evict_localize_cache(work_path, cache_path, percent_free, threshold)

    # log final disk stats
    capacity, free, used, percent_free = disk_space_info(work_path)
    logger.info("Final free disk space for %s: %02.2f%% (%dGB free/%dGB total)" % 
                (work_path, percent_free, free/1024**3, capacity/1024**3))


def cleanup_old_tasks(work_path, tasks_path, percent_free, threshold=10.):
    """If free disk space is below percent threshold, start cleaning out old tasks."""

    if percent_free <= threshold:
        logger.info("Searching for old task dirs to clean out to %02.2f%% free disk space." % threshold)
        for root, dirs, files in os.walk(tasks_path, followlinks=True):
            dirs.sort()
            if '.done' not in files: continue
            logger.info("Cleaning out old task dir %s" % root)
            shutil.rmtree(root, ignore_errors=True)
            capacity, free, used, percent_free = disk_space_info(work_path)
            if percent_free <= threshold: continue
            logger.info("Successfully freed up disk space to %02.2f%%." % percent_free)
            return percent_free
        if percent_free <= threshold:
            logger.info("Failed to free up disk space to %02.2f%%." % threshold)
    return percent_free
            
    
def cleanup_old_jobs(work_path, jobs_path, percent_free, threshold=10., zero_status_only=False):
    """If free disk space is below percent threshold, start cleaning out old jobs."""

    if percent_free <= threshold:
        logger.info("Searching for old job dirs to clean out to %02.2f%% free disk space." % threshold)
        for root, dirs, files in os.walk(jobs_path, followlinks=True):
            dirs.sort()
            if '.done' not in files or '_context.json' not in files or '_job.json' not in files: continue
            # cleanup all or only jobs with exit code 0?
            if zero_status_only:
                exit_code_file = os.path.join(root, '_exit_code')
                if os.path.exists(exit_code_file):
                    with open(exit_code_file) as f:
                        exit_code = f.read()
                    try: exit_code = int(exit_code)
                    except Exception, e:
                        logger.info("Failed to read exit code: %s" % exit_code)
                        exit_code = 1
                    if exit_code != 0: continue

            logger.info("Cleaning out old job dir %s" % root)
            shutil.rmtree(root, ignore_errors=True)
            capacity, free, used, percent_free = disk_space_info(work_path)
            if percent_free <= threshold: continue
            logger.info("Successfully freed up disk space to %02.2f%%." % percent_free)
            return percent_free
        if percent_free <= threshold:
            logger.info("Failed to free up disk space to %02.2f%%." % threshold)
    return percent_free
            
    
def evict_localize_cache(work_path, cache_path, percent_free, threshold=10.):
    """If free disk space is below percent threshold, start evicting cache directories."""

    if percent_free <= threshold:
        logger.info("Evicting cached dirs to clean out to %02.2f%% free disk space." % threshold)
        for timestamp, signal_file, cache_dir in find_cache_dir(cache_path):
            logger.info("Cleaning out cache dir %s" % cache_dir)
            shutil.rmtree(cache_dir, ignore_errors=True)
            capacity, free, used, percent_free = disk_space_info(work_path)
            if percent_free <= threshold: continue
            logger.info("Successfully freed up disk space to %02.2f%%." % percent_free)
            return percent_free
        if percent_free <= threshold:
            logger.info("Failed to free up disk space to %02.2f%%." % threshold)
    return percent_free
            
    
def redelivered_job_dup(job):
    """Return True if job is a duplicate redelivered job. False otherwise."""

    task_id = job['task_id']
    redelivered = job.get('delivery_info', {}).get('redelivered', False)
    status = get_job_status(task_id) 
    logger.info("redelivered_job_dup: redelivered:%s status:%s" % (redelivered, status))
    if redelivered:
        # if job-started, give process_events time to process
        if status == 'job-started':
            logger.info("Allowing process_events time to process")
            time.sleep(60)
            status = get_job_status(task_id) 
            logger.info("redelivered_job_dup: redelivered:%s status:%s" % (redelivered, status))

        # perform dedup
        if status == 'job-started':
            prev_worker = get_task_worker(task_id)
            prev_worker_status = get_worker_status(prev_worker)
            if prev_worker_status is None: return False
            else: return True
        elif status == 'job-completed': return True
        else: return False
    else: return False


class WorkerExecutionError(Exception):

    def __init__(self, message, job_status):
        self.message = message
        self.job_status = job_status
        super(WorkerExecutionError, self).__init__(message, job_status)

    def job_status(self): return self.job_status


class JobDedupedError(Exception):

    def __init__(self, message):
        self.message = message
        self.job_status = 'job-deduped'
        super(JobDedupedError, self).__init__(message)

    def job_status(self): return self.job_status


def shutdown_worker(celery_hostname):
    """Gracefully shutdown a celery worker."""

    app.control.broadcast('shutdown', destination=[celery_hostname])


def job_drain_detected(job_status_json, jd_file):
    """Detect if job drain is occurring."""

    status = job_status_json['status']
    if status in ('job-completed', 'job-deduped') and os.path.exists(jd_file):
        logger.info("Clearing job drain detector: %s" % jd_file)
        try: os.unlink(jd_file)
        except: pass
    elif status == 'job-failed':
        if os.path.exists(jd_file):
            with open(jd_file) as f:
                jd = json.load(f)
            pid = os.getpid()
            if pid != jd['pid']: # reset if new worker process detected
                jd = { 'count': 0, 'starttime': time.time(), 'pid': pid }
            logger.info("Loaded job drain detector: %s" % jd_file)
            logger.info("Initial job drain detector payload: %s" % json.dumps(jd))
            jd['count'] += 1
            logger.info("Incremented failure count")
            with open(jd_file, 'w') as f:
                json.dump(jd, f, indent=2, sort_keys=True)
            logger.info("Updated job drain detector: %s" % jd_file)
            fn = time.time()
            diff = fn - jd['starttime']
            rate = jd['count']/diff
            logger.info("Time diff since first failure (secs): %s" % diff)
            logger.info("Consecutive failure threshold: %s" % \
                        app.conf.WORKER_CONTIGUOUS_FAILURE_THRESHOLD)
            logger.info("Consecutive failure count: %s" % jd['count'])
            logger.info("Failure rate threshold (failures/sec): %s" % FAILURE_RATE)
            logger.info("Failure rate (failures/sec): %s" % rate)
            if jd['count'] > app.conf.WORKER_CONTIGUOUS_FAILURE_THRESHOLD and \
               rate >= FAILURE_RATE:
                logger.info("Job drain detected.")
                return True
        else:
            jd = { 'count': 1, 'starttime': time.time(), 'pid': os.getpid() }
            with open(jd_file, 'w') as f:
                json.dump(jd, f, indent=2, sort_keys=True)
            logger.info("Created job drain detector: %s" % jd_file)
            logger.info("Job drain detector payload: %s" % json.dumps(jd))
    return False


def fail_job(job_status_json, jd_file):
    """Log failed job, detect/handle job drain and raise error."""

    def_err = "Unspecified worker execution error."
    log_job_status(job_status_json)
    if job_drain_detected(job_status_json, jd_file):
        log_custom_event('worker_anomaly', 'job_drain', job_status_json)
        try: os.unlink(jd_file)
        except: pass
        shutdown_worker(job_status_json['celery_hostname'])
        time.sleep(30) # give ample time for shutdown to come back
    raise(WorkerExecutionError(job_status_json.get('error', def_err),
                               job_status_json))


@app.task
def run_job(job, queue_when_finished=True):
    """Function to execute a job."""

    # get payload id
    payload_id = job['job_info']['job_payload']['payload_task_id']

    # get payload hash
    payload_hash = job['job_info']['payload_hash']

    # get dedup flag
    dedup = job['job_info']['dedup']

    # job status json
    job_status_json = {}

    # write celery task id and delivery info
    job['task_id'] = run_job.request.id
    job['delivery_info'] = run_job.request.delivery_info

    # get context
    context = job.get('context', {})

    # hysds signal handler
    def handler(signum, frame):
        status = "job-%s" % SIG_NAMES.get(signum, "sig-%d" % signum)
        error = "Signal handler for run_job() caught signal %d." % signum
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': status,
                            'job': job,
                            'context': context,
                            'error': error,
                            'signum': signum,
                            'short_error': get_short_error(error),
                            'traceback': error,
                            'celery_hostname': run_job.request.hostname }
        log_job_status(job_status_json)
        raise(WorkerExecutionError(error, job_status_json))

    # install hysds signal handler?
    if app.conf.HYSDS_HANDLE_SIGNALS:
        signal.signal(signal.SIGHUP, handler)
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGQUIT, handler)
        signal.signal(signal.SIGABRT, handler)
        signal.signal(signal.SIGTERM, handler)
    
    # redelivered job dedup
    if redelivered_job_dup(job):
        logger.info("Encountered duplicate redelivered job:%s" % json.dumps(job))
        return { 'uuid': job['task_id'],
                 'job_id': job['job_id'],
                 'payload_id': payload_id,
                 'payload_hash': payload_hash,
                 'dedup': dedup,
                 'status': 'job-deduped',
                 'celery_hostname': run_job.request.hostname }

    # set task worker
    log_task_worker(job['task_id'], run_job.request.hostname)

    # get command payload
    cmd_payload = job.get('params', {}).get('_command', None)
    logger.info("_command:%s" % cmd_payload)

    # get disk usage requirement
    du_payload = job.get('params', {}).get('_disk_usage', None)
    logger.info("_disk_usage:%s" % du_payload)

    # get depedency images
    dependency_images = job.get('params', {}).get('job_specification', {}).get('dependency_images', [])
    logger.info("dependency_images:%s" % json.dumps(dependency_images, indent=2))

    # get workers dir
    workers_dir = "workers"
    workers_dir_abs = os.path.join(app.conf.ROOT_WORK_DIR, workers_dir)
    try: makedirs(workers_dir_abs)
    except Exception, e:
        error = str(e)
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': traceback.format_exc(),
                            'celery_hostname': run_job.request.hostname }
        log_job_status(job_status_json)
        raise(WorkerExecutionError(error, job_status_json))

    # set job drain detector file
    jd_file = os.path.join(workers_dir_abs, '%s.failures.json' % run_job.request.hostname)

    # get worker config
    worker_cfg_file = os.environ.get('HYSDS_WORKER_CFG', None)
    if worker_cfg_file is None and cmd_payload is None:
        error = "Environment variable HYSDS_WORKER_CFG is not set or " + \
                "job has no command payload."
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': error,
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)

    # extract worker config or build it if command payload was sent
    logger.info("HYSDS_WORKER_CFG:%s" % worker_cfg_file)
    if worker_cfg_file is not None:
        if not os.path.exists(worker_cfg_file):
            error = "Worker configuration %s doesn't exist." % worker_cfg_file
            job_status_json = { 'uuid': job['task_id'],
                                'job_id': job['job_id'],
                                'payload_id': payload_id,
                                'payload_hash': payload_hash,
                                'dedup': dedup,
                                'status': 'job-failed',
                                'job': job,
                                'context': context,
                                'error': error,
                                'short_error': get_short_error(error),
                                'traceback': error,
                                'celery_hostname': run_job.request.hostname }
            fail_job(job_status_json, jd_file)
        else:
            try:
                with open(worker_cfg_file) as f:
                    worker_cfg = json.load(f)
            except Exception, e:
                error = str(e)
                job_status_json = { 'uuid': job['task_id'],
                                    'job_id': job['job_id'],
                                    'payload_id': payload_id,
                                    'payload_hash': payload_hash,
                                    'dedup': dedup,
                                    'status': 'job-failed',
                                    'job': job,
                                    'context': context,
                                    'error': error,
                                    'short_error': get_short_error(error),
                                    'traceback': traceback.format_exc(),
                                    'celery_hostname': run_job.request.hostname }
                fail_job(job_status_json, jd_file)
    else:
        # build worker config on-the-fly for command payload
        cmd_payload_list = shlex.split(cmd_payload)
        worker_cfg = {
            'configs': [
                {
                    'type': job['type'],
                    'command': {
                        'path': cmd_payload_list[0],
                        'options': [],
                        'arguments': cmd_payload_list[1:],
                        'env': [],
                    },
                    'dependency_images': dependency_images,
                }
            ]
        }
        if 'HYSDS_ROOT_WORK_DIR' in os.environ:
            worker_cfg['root_work_dir'] = os.environ['HYSDS_ROOT_WORK_DIR']
        if 'HYSDS_WEBDAV_PORT' in os.environ:
            worker_cfg['webdav_port'] = os.environ['HYSDS_WEBDAV_PORT']
        if 'HYSDS_WEBDAV_URL' in os.environ:
            worker_cfg['webdav_url'] = os.environ['HYSDS_WEBDAV_URL']
        if du_payload is not None:
            worker_cfg['configs'][0]['disk_usage'] = du_payload
        
    # get datasets config
    datasets_cfg_file = os.environ.get('HYSDS_DATASETS_CFG', None)
    if datasets_cfg_file is None:
        error = "Environment variable HYSDS_DATASETS_CFG is not set."
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': error,
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)

    logger.info("HYSDS_DATASETS_CFG:%s" % datasets_cfg_file)
    if not os.path.exists(datasets_cfg_file):
        error = "Datasets configuration %s doesn't exist." % datasets_cfg_file
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': error,
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)

    # build work configs
    work_cfgs = {}
    for cfg in worker_cfg['configs']:
        work_cfgs[cfg['type']] = cfg

    # other settings
    if run_job.request.delivery_info is None: job_queue = None
    else: job_queue = run_job.request.delivery_info['routing_key'] 
    root_work_dir = worker_cfg.get('root_work_dir', app.conf.ROOT_WORK_DIR)
    webdav_port = str(worker_cfg.get('webdav_port', app.conf.WEBDAV_PORT))
    webdav_url = worker_cfg.get('webdav_url', app.conf.get('WEBDAV_URL'))

    # job execution times
    time_start = datetime.utcnow()
    time_end = None
    time_start_iso = time_start.isoformat() + 'Z'

    # command execution times
    cmd_start = None
    cmd_end = None

    # get work config
    if job['type'] not in work_cfgs:
        error = "No work configuration for type '%s'." % job['type']
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': error,
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)

    command = work_cfgs[job['type']]['command']
    job['command'] = command

    # get job id
    job_id = job['job_id']

    # get cache dir
    cache_dir = "cache"
    cache_dir_abs = os.path.join(root_work_dir, cache_dir)
    try: makedirs(cache_dir_abs)
    except Exception, e:
        error = str(e)
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': traceback.format_exc(),
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)

    # get jobs dir
    jobs_dir = "jobs"
    jobs_dir_abs = os.path.join(root_work_dir, jobs_dir)

    # get tasks dir
    tasks_dir = "tasks"
    tasks_dir_abs = os.path.join(root_work_dir, tasks_dir)

    # get disk usage requirement and compute threshold
    disk_usage = work_cfgs[job['type']].get('disk_usage', du_payload)
    if disk_usage is None: threshold = 10.
    else: threshold = get_threshold(root_work_dir, disk_usage)

    # check disk usage for root work dir;
    # cleanup old work and cached product directories
    cleanup(root_work_dir, jobs_dir_abs, tasks_dir_abs, cache_dir_abs, threshold=threshold)

    # create work directory
    yr, mo, dy, hr, mi, se, wd, y, z = time.gmtime()
    job_dir = os.path.join(root_work_dir, jobs_dir,
                           "%04d" % yr, "%02d" % mo, "%02d" % dy,
                           "%02d" % hr, "%02d" % mi, job_id)
    try: makedirs(job_dir)
    except Exception, e:
        error = str(e)
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': traceback.format_exc(),
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)

    # write job's running file to reserve space for job's done file later
    job_running_file = os.path.join(job_dir, '.running')
    try:
        with open(job_running_file, 'w') as f:
            f.write("%sZ\n" % datetime.utcnow().isoformat())
    except Exception, e:
        error = str(e)
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': traceback.format_exc(),
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)

    # get job's .done file
    job_done_file = os.path.join(job_dir, '.done')

    # add info for traceability and metrics
    job['job_info'].update({
        'job_dir': job_dir,
        'metrics': {
            'inputs_localized': [],
            'products_staged': [],
            'job_dir_size': 0
        }
    })

    # get worker instance facts
    try: facts = get_facts()
    except Exception, e:
        error = str(e)
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': traceback.format_exc(),
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)

    # get availability zone, instance id and type
    for md_url, md_name in ((AZ_INFO, 'ec2_placement_availability_zone'),
                            (INS_ID_INFO, 'ec2_instance_id'),
                            (INS_TYPE_INFO, 'ec2_instance_type')):
        try:
            r = requests.get(md_url, timeout=1)
            if r.status_code == 200:
                facts[md_name] = r.content
        except: pass

    # add facts and IP info to job info
    job['job_info']['facts'] = facts
    job['job_info']['execute_node'] = facts['hysds_execute_node']
    job['job_info']['public_ip'] = facts['hysds_public_ip']

    # add webdav url to job dir
    if webdav_url is None:
        webdav_url = "http://%s:%s" % (job['job_info']['public_ip'], webdav_port)
    job['job_info']['job_url'] = os.path.join(webdav_url, jobs_dir, "%04d" % yr,
                                              "%02d" % mo, "%02d" % dy, "%02d" % hr,
                                              "%02d" % mi, job_id)

    # set or overwrite container image name, url and mappings if defined in work config
    container_image_name = work_cfgs[job['type']].get('container_image_name', None)
    container_image_url = work_cfgs[job['type']].get('container_image_url', None)
    container_mappings = work_cfgs[job['type']].get('container_mappings', {})
    if container_image_name is not None and container_image_url is not None:
        job['container_image_name'] = container_image_name
        job['container_image_url'] = container_image_url
        job['container_mappings'] = container_mappings
        logger.info("Setting image %s (%s) from worker configuration"
                    % (job['container_image_name'], job['container_image_url']))
        logger.info("Using container mappings: %s"
                    % json.dumps(job['container_mappings'], indent=2))

    # set or overwrite dependency images
    dep_imgs = work_cfgs[job['type']].get('dependency_images', [])
    if len(dep_imgs) > 0:
        job['dependency_images'] = dep_imgs
        logger.info("Setting dependency_images from worker configuration: %s"
                    % json.dumps(job['dependency_images'], indent=2))

    # write empty context to file
    try:
        if len(context) > 0: context = { 'context': context }
        context.update(job['params'])
        context['job_priority'] = job.get('priority', None)
        context['container_image_name'] = job.get('container_image_name', None)
        context['container_image_url'] = job.get('container_image_url', None)
        context['container_mappings'] = job.get('container_mappings', {})
        context['_prov'] = { 'wasGeneratedBy': job['task_id'] }
        context_file = os.path.join(job_dir, '_context.json')
        with open(context_file, 'w') as f:
            json.dump(context, f, indent=2, sort_keys=True)
        job['job_info']['context_file'] = context_file
        job['job_info']['datasets_cfg_file'] = datasets_cfg_file
    except Exception, e:
        error = str(e)
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': traceback.format_exc(),
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)
    #logger.info(" Wrote context json file: %s" % context_file)

    # write job to file
    try:
        job_file = os.path.join(job_dir, '_job.json')
        with open(job_file, 'w') as f:
            json.dump(job, f, indent=2, sort_keys=True)
    except Exception, e:
        error = str(e)
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': traceback.format_exc(),
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)
    #logger.info(" Wrote job json file: %s" % job_file)

    # run file localization and job execution
    dedupJob = None
    monitoredRunner = None
    try:
        # do dedup check first
        if dedup is True:
            dj = query_dedup_job(payload_hash, filter_id=job['task_id'],
                                 states=['job-started', 'job-completed'])
            if isinstance(dj, dict):
                error = "verdi worker found duplicate job %s with status %s" % (dj['_id'], dj['status'])
                dedupJob = dj['_id']
                raise(JobDedupedError(error))

        # set status to job-started
        job['job_info']['time_start'] = time_start_iso
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-started',
                            'job': job,
                            'context': context,
                            'celery_hostname': run_job.request.hostname }
        log_job_status(job_status_json)

        # check if containers need to be loaded
        image_name = job.get('container_image_name', None)
        image_url = job.get('container_image_url', None)
        image_mappings = job.get('container_mappings', {})
        if image_name is not None:
            image_info = ensure_image_loaded(image_name, image_url, cache_dir_abs)
            job['container_image_id'] = image_info['Id']
            context['container_image_id'] = job['container_image_id']
        for i, dep_img in enumerate(job.get('dependency_images', [])):
            dep_image_info = ensure_image_loaded(dep_img['container_image_name'], 
                                                 dep_img['container_image_url'],
                                                 cache_dir_abs)
            dep_img['container_image_id'] = dep_image_info['Id']
            ctx_dep_img = context['job_specification']['dependency_images'][i]
            ctx_dep_img['container_image_id'] = dep_img['container_image_id']

        # update context file with image ids
        with open(context_file, 'w') as f:
            json.dump(context, f, indent=2, sort_keys=True)

        # run pre-processing steps
        disable_pre = job.get('params', {}).get('job_specification', {}).get('disable_pre_builtins', False)
        pre_processors = [] if disable_pre else list(PRE_PROCESSORS)
        pre_processors.extend(job.get('params', {}).get('job_specification', {}).get('pre', []))
        pre_processor_sigs = []
        for pre_processor in pre_processors:
            func = get_func(pre_processor)
            logger.info("Running pre-processor: %s" % pre_processor)
            pre_processor_sigs.append(func(job, context))

        # run real-time monitor
        cmdLineList = [job['command']['path']]
        for opt in job['command']['options']:
            cmdLineList.append(opt)
        for arg in job['command']['arguments']:
            matchArg = re.search(r'^\$(\w+)$', arg)
            if matchArg: arg = job['params'][matchArg.group(1)]
            if isinstance(arg, (types.ListType, types.TupleType)):
                cmdLineList.extend(arg)
            else: cmdLineList.append(arg)
        execEnv = dict(os.environ)
        for env in job['command']['env']:
            execEnv[env['key']] = env['value']
        logger.info(" cmdLineList: %s" % cmdLineList)

        # check if job needs to run in a container
        docker_params = {}
        if image_name is not None:
            # get docker params
            docker_params[image_name] = get_docker_params(image_name, image_url, 
                                                          image_mappings, root_work_dir, 
                                                          job_dir)

            # get command-line list
            cmdLineList = get_docker_cmd(docker_params[image_name], cmdLineList)
            logger.info(" docker cmdLineList: %s" % cmdLineList)

        # build docker params for dependency containers
        for dep_img in job.get('dependency_images', []):
            docker_params[dep_img['container_image_name']] = \
                get_docker_params(dep_img['container_image_name'],
                                  dep_img['container_image_url'],
                                  dep_img['container_mappings'],
                                  root_work_dir, job_dir)

        # dump docker params to file
        try:
            docker_params_file = os.path.join(job_dir, '_docker_params.json')
            with open(docker_params_file, 'w') as f:
                json.dump(docker_params, f, indent=2, sort_keys=True)
        except Exception, e:
            tb = traceback.format_exc()
            err = "Failed to dump docker params to file %s: %s\n%s" % (docker_params_file, str(e), tb)
            raise(RuntimeError(err))

        # make sure command-line list items are string
        cmdLineList = [str(i) for i in cmdLineList]
        cmdLine = " ".join(cmdLineList)
        logger.info(" cmdLine: %s" % cmdLine)

        # dump run script for rerun
        run_script = os.path.join(job_dir, "_run.sh")
        with open(run_script, 'w') as f:
            f.write("#!/bin/bash\n\n")
            # dump entire env for info
            for env_var, env_val in execEnv.iteritems():
                f.write("#%s=%s\n" % (env_var, env_val))
            f.write("\n")
            # dump job env for execution
            for env in job['command']['env']:
                f.write("export %s=%s\n" % (env['key'], env['value']))
            f.write("\n%s\n" % cmdLine)
        try: os.chmod(run_script, 0755)
        except: pass

        # command execution start time
        cmd_start = datetime.utcnow()
        cmd_start_iso = cmd_start.isoformat() + 'Z'
        job['job_info']['cmd_start'] = cmd_start_iso

        # if all pre-processors signaled True, run command
        if all(pre_processor_sigs):
            logger.info("Pre-processing steps all signaled continuation.")

            # use pymonitoredrunner by default
            monitoredRunner = MonitoredRunner(cmdLineList, job_dir, execEnv,
                                              app.conf.PYMONITOREDRUNNER_CFG, job_id)
            monitoredRunner.start()

            # wait for completion
            monitoredRunner.join()
            status = monitoredRunner.getExitCode()
            pid = monitoredRunner.getPid()
        else:
            no_cont = list(compress(pre_processors, [not i for i in pre_processor_sigs]))
            logger.info("Pre-processing steps that didn't signal continuation: %s" % ", ".join(no_cont))
            status = 0
            pid = 0

        # command execution end time and duration
        cmd_end = datetime.utcnow()
        job['job_info']['cmd_end'] = cmd_end.isoformat() + 'Z'
        job['job_info']['cmd_duration'] = (cmd_end - cmd_start).total_seconds()

        # write status, stderr, and stdout to job json
        job['job_info']['status'] = status
        job['job_info']['stdout'] = ''
        job['job_info']['stderr'] = ''
        job['job_info']['pid'] = pid
        logger.info(" status: %s" % status)

        # save job directory size
        job['job_info']['metrics']['job_dir_size'] = get_disk_usage(job_dir)

        # update context
        with open(context_file) as f:
            context.update(json.load(f))
        #logger.info(" Updated context from json file: %s" % context_file)

        # write job to file
        with open(job_file, 'w') as f:
            json.dump(job, f, indent=2, sort_keys=True)
        #logger.info(" Updated job json file: %s" % job_file)

        # handle non-zero exit status
        if status != 0:
            if status is None:
                raise(RuntimeError("Failed to get exit status."))
            else: raise(RuntimeError("Got non-zero exit code: %d" % status))

        # check for metrics from PGE
        for pge_metrics_file in find_pge_metrics(job_dir):
            pge_metrics = {}
            with open(pge_metrics_file) as f:
                try: pge_metrics = json.load(f)
                except Exception, e:
                    tb = traceback.format_exc()
                    err = "Failed to load PGE-generated metrics from %s: %s\n%s" % (pge_metrics_file, str(e), tb)
                    raise(RuntimeError(err))

            # append input localization metrics
            job['job_info']['metrics']['inputs_localized'].extend(pge_metrics.get('download', []))

        # add prov associations
        if len(job['job_info']['metrics']['inputs_localized']) > 0:
            context['_prov']['wasDerivedFrom'] = [i['url'] for i in job['job_info']['metrics']['inputs_localized']]

            # update context file with prov associations
            with open(context_file, 'w') as f:
                json.dump(context, f, indent=2, sort_keys=True)

        # save job duration
        time_end = datetime.utcnow()
        job['job_info']['time_end'] = time_end.isoformat() + 'Z'
        job['job_info']['duration'] = (time_end - time_start).total_seconds()

        # set completed status
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-completed',
                            'job': job,
                            'context': context,
                            'celery_hostname': run_job.request.hostname }

    except Exception, e:
        # log error
        error = str(e)
        tb = traceback.format_exc()
        logger.info(" Got error: %s\n%s" % (error, tb))

        # process id
        pid = None
        if monitoredRunner is not None:
            pid = monitoredRunner.getPid()

        # if soft time limit exceeded, send SIGTERM to process group
        if isinstance(e, SoftTimeLimitExceeded):
            if pid is not None:
                try: os.killpg(pid, signal.SIGTERM)
                except Exception, e2:
                    logger.info(" Got error trying to send " +
                                "SIGTERM to %d: %s\n%s" % 
                                (pid, str(e2), traceback.format_exc()))
                time.sleep(5) # give some time for procs to terminate

            # get status
            if monitoredRunner is not None and os.path.exists(monitoredRunner.getExitCodeFile()):
                status = monitoredRunner.getExitCode()
            else: status = -15
            job['job_info']['status'] = status
            job['job_info']['stdout'] = ''
            job['job_info']['stderr'] = ''
            logger.info(" status: %s" % status)

            # append error to job's stderr file
            with open(os.path.join(job_dir, '_stderr.txt'), 'a') as f:
                f.write("\nSoft time limit (%ds) exceeded for %s.\n" %
                        (job['job_info']['soft_time_limit'], job['job_id']))
    
        # if cmd_end not set and cmd_start was, do it now
        if cmd_end is None and cmd_start is not None:
            cmd_end = datetime.utcnow()
            job['job_info']['cmd_end'] = cmd_end.isoformat() + 'Z'
            job['job_info']['cmd_duration'] = (cmd_end - cmd_start).total_seconds()

        # if exit code of pymonitoredrunner is 0, set to 1
        if 'status' not in job['job_info'] or job['job_info']['status'] == 0:
            job['job_info']['status'] = 1

        # save process id
        job['job_info']['pid'] = pid

        # save job directory size
        job['job_info']['metrics']['job_dir_size'] = get_disk_usage(job_dir)

        # update context
        with open(context_file) as f:
            context.update(json.load(f))
        #logger.info(" Updated context from json file: %s" % context_file)

        # overwrite error if _alt_error.txt was dumped
        alt_error_file = os.path.join(job_dir, '_alt_error.txt')
        if os.path.exists(alt_error_file):
            with open(alt_error_file) as f:
                error = f.read()
                logger.info("Got alternate error message: %s" % error)

        # overwrite traceback if _alt_traceback.txt was dumped
        alt_tb_file = os.path.join(job_dir, '_alt_traceback.txt')
        if os.path.exists(alt_tb_file):
            with open(alt_tb_file) as f:
                tb = f.read()
                logger.info("Got alternate traceback: %s" % tb)

        # if time_end not set, do it now
        if time_end is None:
            time_end = datetime.utcnow()
            job['job_info']['time_end'] = time_end.isoformat() + 'Z'
            job['job_info']['duration'] = (time_end - time_start).total_seconds()

        # set failed/deduped status
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'dedup_job': dedupJob,
                            'job': job,
                            'context': context,
                            'celery_hostname': run_job.request.hostname }
        if isinstance(e, JobDedupedError):
            job_status_json['status'] = 'job-deduped'
            job_status_json['dedup_msg'] = error
        else:
            job_status_json['status'] = 'job-failed'
            job_status_json['error'] = error
            job_status_json['short_error'] = get_short_error(error)
            job_status_json['traceback'] = tb

    # run post-processing steps
    try:
        disable_post = job.get('params', {}).get('job_specification', {}).get('disable_post_builtins', False)
        post_processors = [] if disable_post else list(POST_PROCESSORS)
        post_processors.extend(job.get('params', {}).get('job_specification', {}).get('post', []))
        post_processor_sigs = []
        for post_processor in post_processors:
            func = get_func(post_processor)
            logger.info("Running post-processor: %s" % post_processor)
            post_processor_sigs.append(func(job, context))

        # if not all post-processors signaled True, log them out
        if not all(post_processor_sigs):
            no_cont = list(compress(post_processors, [not i for i in post_processor_sigs]))
            logger.info("Post-processing steps that didn't signal continuation: %s" % ", ".join(no_cont))
    except Exception, e:
        error = str(e)
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': traceback.format_exc(),
                            'celery_hostname': run_job.request.hostname }

    # close up job execution
    try:
        # log job info metrics
        log_job_info(job)

        # log final job status
        log_job_status(job_status_json)

        # transition running file to done file and queue finished job
        os.rename(job_running_file, job_done_file)
        with open(job_done_file, 'w') as f:
            f.write("%sZ\n" % datetime.utcnow().isoformat())

        # queue job finished for user rules processing
        if queue_when_finished == True: queue_finished_job(payload_id)
    except Exception, e:
        error = str(e)
        job_status_json = { 'uuid': job['task_id'],
                            'job_id': job['job_id'],
                            'payload_id': payload_id,
                            'payload_hash': payload_hash,
                            'dedup': dedup,
                            'status': 'job-failed',
                            'job': job,
                            'context': context,
                            'error': error,
                            'short_error': get_short_error(error),
                            'traceback': traceback.format_exc(),
                            'celery_hostname': run_job.request.hostname }
        fail_job(job_status_json, jd_file)

    # raise worker execution error
    if job_status_json['status'] == 'job-failed':
        fail_job(job_status_json, jd_file)
 
    # return basic job status
    return { 'uuid': job['task_id'],
             'job_id': job['job_id'],
             'payload_id': payload_id,
             'payload_hash': payload_hash,
             'dedup': dedup,
             'status': job_status_json['status'],
             'job_url': job['job_info']['job_url'],
             'celery_hostname': run_job.request.hostname }


def set_revoked_job_done(root_work, job_id):
    """Find job work dir by job id."""

    for root, dirs, files in os.walk(root_work, followlinks=True):
        if job_id in dirs:
            job_dir = os.path.join(root, job_id)
            job_running_file = os.path.join(job_dir, '.running')
            job_done_file = os.path.join(job_dir, '.done')
            if not os.path.exists(job_done_file):
                logger.info("No job done file found: %s" % job_done_file)
                if os.path.exists(job_running_file):
                    os.rename(job_running_file, job_done_file)
                    logger.info("Renamed %s to %s." % (job_running_file, job_done_file))
                with open(job_done_file, 'w') as f:
                    f.write("%sZ\n" % datetime.utcnow().isoformat())
                logger.info("Wrote timestamp to %s." % job_done_file)
            return
        else: continue
    logger.info("No work directory found for job_id %s." % job_id)


@task_revoked.connect(sender=run_job)
def task_revoked_handler(*args, **kwargs):
    """Handler for tasks that are revoked."""

    if kwargs.get('signum', None) is None:
        signum = 0 # queued task revocation
    else:
        signum = int(kwargs['signum']) # running task revocation
    request = kwargs['request']
    job = request.args[0]
    payload_id = job['job_info']['job_payload']['payload_task_id']
    payload_hash = job['job_info']['payload_hash']
    dedup = job['job_info']['dedup']
    context = job.get('context', {})
    error = "Job was revoked with signal %d." % signum
    job_status_json = { 'uuid': job['task_id'],
                        'job_id': job['job_id'],
                        'payload_id': payload_id,
                        'payload_hash': payload_hash,
                        'dedup': dedup,
                        'status': 'job-revoked',
                        'job': job,
                        'context': context,
                        'error': error,
                        'signum': signum,
                        'short_error': get_short_error(error),
                        'traceback': error,
                        'celery_hostname': request.hostname }
    log_job_status(job_status_json)
    set_revoked_job_done(app.conf.ROOT_WORK_DIR, job['job_id'])
