#!/usr/bin/env python
import json, requests, logging, traceback, types, msgpack, re
from pprint import pformat
from datetime import datetime
from celery.result import AsyncResult
from redis import ConnectionPool, StrictRedis

from hysds.celery import app
from hysds.orchestrator import submit_job
from hysds.log_utils import log_job_status, JOB_STATUS_KEY_TMPL, WORKER_STATUS_KEY_TMPL
from hysds.utils import get_short_error


log_format = "[%(asctime)s: %(levelname)s/process_events] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# redis connection pool
POOL = None

# regex for orchestrator tasks and workers
ORCH_HOST_RE = re.compile(r'^celery@orchestrator')
ORCH_NAME_RE = re.compile(r'^hysds.orchestrator.submit_job')

# regex for task-failed errors that won't be updated in ES because
# worker had no chance to send update
TASK_FAILED_RE = re.compile(r'^(WorkerLostError|TimeLimitExceeded)')

# regex for extracting type and hostname from worker
HOSTNAME_RE = re.compile(r'^celery@(.+?)\..+$')


def set_redis_pool():
    """Set redis connection pool for status updates."""

    global POOL
    if POOL is None:
        POOL = ConnectionPool.from_url(app.conf.REDIS_UNIX_DOMAIN_SOCKET)


def parse_job_type(event):
    """Extract resource's job type."""

    # parse job type from worker task events
    hostname = event.get('hostname', "")
    match = HOSTNAME_RE.search(hostname)
    if match: return match.group(1)

    # parse job type from orchestrator task events
    job_type = "unknown"
    try:
        payload = eval(event['args'])[0]
        job_type = payload['type']
    except Exception, e:
        logging.error("Got exception trying to parse job type for %s: %s\n%s\n%s"
                      % (hostname, str(e), json.dumps(event, indent=2),
                         traceback.format_exc()))
    return job_type


def log_task_event(event_type, event, uuid=[]):
    """Print task event."""

    set_redis_pool()
    global POOL
    info = { 'resource': 'task',
             'type': parse_job_type(event),
             'status': event_type,
             'celery_hostname': event.get('hostname', None),
             'uuid': uuid,
             '@version': '1',
             '@timestamp': "%sZ" % datetime.utcnow().isoformat(),
             'event': event }

    # send update to redis
    r = StrictRedis(connection_pool=POOL)
    r.rpush(app.conf.REDIS_JOB_STATUS_KEY, msgpack.dumps(info))

    # print log
    try: logging.info("hysds.task_event:%s" % json.dumps(info))
    except Exception, e:
        logging.error("Got exception trying to log task event: %s" % str(e))


def log_worker_event(event_type, event, uuid=[]):
    """Print worker event."""

    set_redis_pool()
    global POOL
    info = { 'resource': 'worker',
             'type': parse_job_type(event),
             'status': event_type,
             'celery_hostname': event['hostname'],
             'uuid': uuid,
             '@version': '1',
             '@timestamp': "%sZ" % datetime.utcnow().isoformat(),
             'event': event }

    # send update to redis
    r = StrictRedis(connection_pool=POOL)
    r.rpush(app.conf.REDIS_JOB_STATUS_KEY, msgpack.dumps(info))

    # print log
    try: logging.info("hysds.worker_event:%s" % json.dumps(info))
    except Exception, e:
        logging.error("Got exception trying to log worker event: %s" % str(e))


def log_worker_status(worker, status):
    """Print worker status."""

    set_redis_pool()
    global POOL

    # send update to redis; set at the heartbeat-interval of celery workers
    r = StrictRedis(connection_pool=POOL)
    r.setex(WORKER_STATUS_KEY_TMPL % worker, 60, status) 

    # print log
    try: logging.info("hysds.worker_status:%s:%s" % (worker, status))
    except Exception, e:
        logging.error("Got exception trying to log worker status: %s" % str(e))


def event_monitor(app):
    state = app.events.State()

    def task_sent(event):
        state.event(event)
        if ORCH_HOST_RE.search(event['hostname']) or \
           ORCH_NAME_RE.search(event['name']): return
        log_task_event('task-sent', event, uuid=event['uuid'])

    def task_received(event):
        state.event(event)
        if ORCH_HOST_RE.search(event['hostname']): return
        log_task_event('task-received', event, uuid=event['uuid'])

    def task_started(event):
        state.event(event)
        if ORCH_HOST_RE.search(event['hostname']): return
        log_task_event('task-started', event, uuid=event['uuid'])

    def task_succeeded(event):
        set_redis_pool()
        global POOL
        state.event(event)
        if ORCH_HOST_RE.search(event['hostname']): return
        log_task_event('task-succeeded', event, uuid=event['uuid'])

    def task_failed(event):
        state.event(event)
        uuid = event['uuid']
        exc = event.get('exception', "")
        if isinstance(exc, types.StringTypes):
            match = TASK_FAILED_RE.search(exc)
            if match:
                short_error = match.group(1)
                es_url = "%s/job_status-current/job/%s" % (app.conf['JOBS_ES_URL'], uuid)
                r = requests.get(es_url)
                if r.status_code != 200:
                    logging.error("Failed to query for task UUID %s: %s" % (uuid, r.content))
                    return
                res = r.json()
                job_status = res['_source']
                job_status['status'] = 'job-failed'
                job_status['error'] = exc
                job_status['short_error'] = short_error
                job_status['traceback'] = event.get('traceback', "")
                time_end = datetime.utcnow().isoformat() + 'Z'
                job_status.setdefault('job', {}).setdefault('job_info', {})['time_end'] = time_end
                log_job_status(job_status)
        log_task_event('task-failed', event, uuid=event['uuid'])

    def task_retried(event):
        state.event(event)
        if ORCH_HOST_RE.search(event['hostname']): return
        log_task_event('task-retried', event, uuid=event['uuid'])

    def task_revoked(event):
        state.event(event)
        if ORCH_HOST_RE.search(event['hostname']): return
        log_task_event('task-revoked', event, uuid=event['uuid'])

    def worker_online(event):
        state.event(event)
        if ORCH_HOST_RE.search(event['hostname']): return
        log_worker_status(event['hostname'], event['type'])
        log_worker_event('worker-online', event) 

    def worker_offline(event):
        set_redis_pool()
        global POOL
        rd = StrictRedis(connection_pool=POOL)
        state.event(event)
        if ORCH_HOST_RE.search(event['hostname']): return
        rd.delete([WORKER_STATUS_KEY_TMPL % event['hostname']])
        time_end = datetime.utcnow().isoformat() + 'Z'
        query = {
            "query" : {
                "filtered" : {
                    "query" : {
                        "bool": {
                            "must": [
                                { "term": { "celery_hostname": event['hostname'] } },
                                #{ "term": { "status": 'job-started' } }
                            ]
                        }
                    }
                }
            }
        }
        job_status_jsons = []
        #logging.error("query:\n%s" % json.dumps(query, indent=2))
        es_url = "%s/job_status-current/_search?search_type=scan&scroll=60m&size=100" % app.conf['JOBS_ES_URL']
        try:
            r = requests.post(es_url, data=json.dumps(query))
            r.raise_for_status()
            scan_result = r.json()
            scroll_id = scan_result['_scroll_id']
            while True:
                r = requests.post('%s/_search/scroll?scroll=60m' % app.conf['JOBS_ES_URL'], data=scroll_id)
                res = r.json()
                scroll_id = res['_scroll_id']
                if len(res['hits']['hits']) == 0: break
                for hit in res['hits']['hits']:
                    job_status_jsons.append(hit['_source'])
            #logging.error("job_status_jsons:\n%s" % job_status_jsons)
            uuids = []
            for job_status_json in job_status_jsons:
                # continue if real-time job status is still job-started
                if rd.get(JOB_STATUS_KEY_TMPL % job_status_json['uuid']) != "job-started":
                    continue
                job_status_json['status'] = 'job-offline'
                job_status_json['error'] = 'Received worker-offline event during job execution.'
                job_status_json['short_error'] = 'worker-offline'
                job_status_json.setdefault('job', {}).setdefault('job_info', {})['time_end'] = time_end
                log_job_status(job_status_json)
                uuids.append(job_status_json['uuid'])
            log_worker_event('worker-offline', event, uuid=uuids)
        except Exception, e:
            logging.error("Got exception trying to update task events for " + \
                          "offline worker %s: %s\n%s" % (event['hostname'], str(e), 
                                                         traceback.format_exc()))

    def worker_heartbeat(event):
        state.event(event)
        if ORCH_HOST_RE.search(event['hostname']): return
        log_worker_status(event['hostname'], event['type'])
        log_worker_event('worker-heartbeat', event) 

    def any_event(event):
        state.event(event)
        logging.info('EVENT: %s' % pformat(event))

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            'task-sent': task_sent,
            'task-received': task_received,
            'task-started': task_started,
            'task-succeeded': task_succeeded,
            'task-failed': task_failed,
            'task-retried': task_retried,
            'task-revoked': task_revoked,
            'worker-online': worker_online,
            'worker-offline': worker_offline,
            'worker-heartbeat': worker_heartbeat,
            #'*': any_event,
        })
        recv.capture(limit=None, timeout=None, wakeup=True)

if __name__ == '__main__':
    event_monitor(app)
