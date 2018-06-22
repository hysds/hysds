from __future__ import absolute_import

import os, re, json, copy, socket, msgpack, traceback, types, backoff
from datetime import datetime
from uuid import uuid4
from redis import BlockingConnectionPool, StrictRedis, RedisError

from celery.utils.log import get_task_logger

import hysds
from hysds.celery import app

from prov_es.model import get_uuid, ProvEsDocument


#logger
logger = get_task_logger(__name__)

# redis connection pools
JOB_STATUS_POOL = None
JOB_INFO_POOL = None
WORKER_STATUS_POOL = None
EVENT_STATUS_POOL = None

# job status key template
JOB_STATUS_KEY_TMPL = "hysds-job-status-%s"

# worker status key template
WORKER_STATUS_KEY_TMPL = "hysds-worker-status-%s"

# task worker key template
TASK_WORKER_KEY_TMPL = "hysds-task-worker-%s"


def backoff_max_value():
    """Return max value for backoff."""
    return app.conf.BACKOFF_MAX_VALUE


def backoff_max_tries():
    """Return max tries for backoff."""
    return app.conf.BACKOFF_MAX_TRIES


def hard_time_limit_gap():
    """Return minimum gap time after soft time limit."""
    return app.conf.HARD_TIME_LIMIT_GAP


def ensure_hard_time_limit_gap(soft_time_limit, time_limit):
    """Ensure hard time limit gap."""

    gap = hard_time_limit_gap()
    if soft_time_limit is not None and (time_limit is None or
                                        time_limit <= soft_time_limit+gap):
        time_limit = soft_time_limit + gap
    return soft_time_limit, time_limit


def set_redis_job_status_pool():
    """Set redis connection pool for job status."""

    global JOB_STATUS_POOL
    if JOB_STATUS_POOL is None:
        JOB_STATUS_POOL = BlockingConnectionPool.from_url(app.conf.REDIS_JOB_STATUS_URL)


def set_redis_job_info_pool():
    """Set redis connection pool for job info metrics."""

    global JOB_INFO_POOL
    if JOB_INFO_POOL is None:
        JOB_INFO_POOL = BlockingConnectionPool.from_url(app.conf.REDIS_JOB_INFO_URL)


def set_redis_worker_status_pool():
    """Set redis connection pool for worker status."""

    global WORKER_STATUS_POOL
    if WORKER_STATUS_POOL is None:
        WORKER_STATUS_POOL = BlockingConnectionPool.from_url(app.conf.REDIS_JOB_STATUS_URL)


def set_redis_event_status_pool():
    """Set redis connection pool for event status."""

    global EVENT_STATUS_POOL
    if EVENT_STATUS_POOL is None:
        EVENT_STATUS_POOL = BlockingConnectionPool.from_url(app.conf.REDIS_JOB_STATUS_URL)


@backoff.on_exception(backoff.expo,
                      RedisError,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def log_task_worker(task_id, worker):
    """Log task worker for task ID in redis."""
    
    set_redis_worker_status_pool()
    global WORKER_STATUS_POOL

    # set task worker for task ID
    r = StrictRedis(connection_pool=WORKER_STATUS_POOL)
    r.setex(TASK_WORKER_KEY_TMPL % task_id,
            app.conf.HYSDS_JOB_STATUS_EXPIRES,
            worker)


@backoff.on_exception(backoff.expo,
                      RedisError,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def get_task_worker(task_id):
    """Retrieve task worker by task ID from redis."""
    
    set_redis_worker_status_pool()
    global WORKER_STATUS_POOL

    # retrieve task worker
    r = StrictRedis(connection_pool=WORKER_STATUS_POOL)
    return r.get(TASK_WORKER_KEY_TMPL % task_id)


@backoff.on_exception(backoff.expo,
                      RedisError,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def get_worker_status(worker):
    """Retrieve worker status by worker ID from redis."""
    
    set_redis_worker_status_pool()
    global WORKER_STATUS_POOL

    # retrieve worker status
    r = StrictRedis(connection_pool=WORKER_STATUS_POOL)
    return r.get(WORKER_STATUS_KEY_TMPL % worker)


@backoff.on_exception(backoff.expo,
                      RedisError,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def get_job_status(task_id):
    """Retrieve job status by task ID from redis."""
    
    set_redis_job_status_pool()
    global JOB_STATUS_POOL

    # retrieve job status
    r = StrictRedis(connection_pool=JOB_STATUS_POOL)
    return r.get(JOB_STATUS_KEY_TMPL % task_id)


@backoff.on_exception(backoff.expo,
                      RedisError,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def log_job_status(job):
    """Print job status."""
    
    set_redis_job_status_pool()
    global JOB_STATUS_POOL
    job['resource'] = 'job'
    job['type'] = job.get('job', {}).get('type', 'unknown')
    job['@version'] = '1'
    job['@timestamp'] = "%sZ" % datetime.utcnow().isoformat()
    if 'tag' in job.get('job', {}):
        tags = job.setdefault('tags', [])
        if isinstance(tags, types.StringTypes): tags = [ tags ]
        tags.append(job['job']['tag'])
        job['tags'] = tags

    # send update to redis
    r = StrictRedis(connection_pool=JOB_STATUS_POOL)
    r.setex(JOB_STATUS_KEY_TMPL % job['uuid'],
            app.conf.HYSDS_JOB_STATUS_EXPIRES,
            job['status']) # for dedup
    r.rpush(app.conf.REDIS_JOB_STATUS_KEY, msgpack.dumps(job)) # for ES
    logger.info("job_status_json:%s" % json.dumps(job))


@backoff.on_exception(backoff.expo,
                      RedisError,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def log_job_info(job):
    """Print job info."""
    
    set_redis_job_info_pool()
    global JOB_INFO_POOL
    filtered_info = {}
    for info in ('job_info', 'job_id', 'task_id', 'delivery_info', 'tag',
                 'priority', 'container_image_name', 'container_image_url',
                 'name'):
        if info in job: filtered_info[info] = job[info]
    job_info = { 'type': 'job_info',
                 '@version': '1',
                 '@timestamp': "%sZ" % datetime.utcnow().isoformat(),
                 'job': filtered_info,
                 'job_type': job['type'] }

    # send update to redis
    r = StrictRedis(connection_pool=JOB_INFO_POOL)
    r.rpush(app.conf.REDIS_JOB_INFO_KEY, msgpack.dumps(job_info))
    logger.info("job_info_json:%s" % json.dumps(job_info))


@backoff.on_exception(backoff.expo,
                      RedisError,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def log_custom_event(event_type, event_status, event, tags=[], hostname=None):
    """Log custom event."""

    set_redis_event_status_pool()
    global EVENT_STATUS_POOL

    uuid = str(uuid4())
    if hostname is None:
        try: hostname = socket.getfqdn()
        except:
            try: hostname = socket.gethostbyname(socket.gethostname())
            except: hostname = ''
    info = { 'resource': 'event',
             'type': event_type,
             'status': event_status,
             '@timestamp': "%sZ" % datetime.utcnow().isoformat(),
             'hostname': hostname,
             'uuid': uuid,
             'tags': tags,
             '@version': '1',
             'event': event }

    # send update to redis
    r = StrictRedis(connection_pool=EVENT_STATUS_POOL)
    r.rpush(app.conf.REDIS_JOB_STATUS_KEY, msgpack.dumps(info))
    logger.info("hysds.custom_event:%s" % json.dumps(info))
    return uuid


def log_prov_es(job, prov_es_info, prov_es_file):
    """Log PROV-ES document. Create temp PROV-ES document to populate 
       attributes that only the worker has access to (e.g. PID)."""

    # create PROV-ES doc to generate attributes that only verdi know
    ps_id = "hysds:%s" % get_uuid(job['job_id'])
    bundle_id = "hysds:%s" % get_uuid('bundle-%s' % job['job_id'])
    doc = ProvEsDocument()

    # get bundle
    #bndl = doc.bundle(bundle_id)
    bndl = None

    # create sofware agent
    sa_label = "hysds:pge_wrapper/%s/%d/%s" % (job['job_info']['execute_node'],
                                               job['job_info']['pid'],
                                               datetime.utcnow().isoformat())
    sa_id = "hysds:%s" % get_uuid(sa_label)
    doc.softwareAgent(sa_id, str(job['job_info']['pid']),
                      job['job_info']['execute_node'],
                      role=job.get('username', None),
                      label=sa_label, bundle=bndl)

    # create processStep
    doc.processStep(ps_id, job['job_info']['cmd_start'],
                    job['job_info']['cmd_end'], [], sa_id,
                    None, [], [], bundle=bndl,
                    prov_type="hysds:%s" % job['type'])

    # get json
    pd = json.loads(doc.serialize()) 

    # update software agent and process step
    if 'bundle' in prov_es_info:
        if len(prov_es_info['bundle']) == 1:
            bundle_id_orig = prov_es_info['bundle'].keys()[0]

            # update software agent
            prov_es_info['bundle'][bundle_id_orig].setdefault('agent', {}).update(pd['bundle'][bundle_id]['agent'])

            # update wasAssociatedWith
            prov_es_info['bundle'][bundle_id_orig].setdefault('wasAssociatedWith', {}).update(pd['bundle'][bundle_id]['wasAssociatedWith'])

            # update activity
            if 'activity' in prov_es_info['bundle'][bundle_id_orig]:
                if len(prov_es_info['bundle'][bundle_id_orig]['activity']) == 1:
                    ps_id_orig = prov_es_info['bundle'][bundle_id_orig]['activity'].keys()[0]
                    prov_es_info['bundle'][bundle_id_orig]['activity'][ps_id_orig]['prov:startTime'] = pd['bundle'][bundle_id]['activity'][ps_id]['prov:startTime']
                    prov_es_info['bundle'][bundle_id_orig]['activity'][ps_id_orig]['prov:endTime'] = pd['bundle'][bundle_id]['activity'][ps_id]['prov:endTime']
                    prov_es_info['bundle'][bundle_id_orig]['activity'][ps_id_orig]['hysds:job_id'] = job['job_id']
                    prov_es_info['bundle'][bundle_id_orig]['activity'][ps_id_orig]['hysds:job_type'] = job['type']
                    prov_es_info['bundle'][bundle_id_orig]['activity'][ps_id_orig]['hysds:job_url'] = job['job_info']['job_url']
                    prov_es_info['bundle'][bundle_id_orig]['activity'][ps_id_orig]['hysds:mozart_url'] = app.conf.MOZART_URL
                    if 'prov:type' not in prov_es_info['bundle'][bundle_id_orig]['activity'][ps_id_orig]:
                        prov_es_info['bundle'][bundle_id_orig]['activity'][ps_id_orig]['prov:type'] = pd['bundle'][bundle_id]['activity'][ps_id]['prov:type']

                    # update wasAssociatedWith activity ids
                    for waw_id in prov_es_info['bundle'][bundle_id_orig]['wasAssociatedWith']:
                        if prov_es_info['bundle'][bundle_id_orig]['wasAssociatedWith'][waw_id]['prov:activity'] == ps_id:
                            prov_es_info['bundle'][bundle_id_orig]['wasAssociatedWith'][waw_id]['prov:activity'] = ps_id_orig
                else: prov_es_info['bundle'][bundle_id_orig]['activity'].update(pd['bundle'][bundle_id]['activity'])
            else: prov_es_info['bundle'][bundle_id_orig]['activity'] = pd['bundle'][bundle_id]['activity']
    else:
        # update software agent
        prov_es_info.setdefault('agent', {}).update(pd['agent'])
    
        # update wasAssociatedWith
        prov_es_info.setdefault('wasAssociatedWith', {}).update(pd['wasAssociatedWith'])
    
        # update process step
        if 'activity' in prov_es_info:
            if len(prov_es_info['activity']) == 1:
                ps_id_orig = prov_es_info['activity'].keys()[0]
                prov_es_info['activity'][ps_id_orig]['prov:startTime'] = pd['activity'][ps_id]['prov:startTime']
                prov_es_info['activity'][ps_id_orig]['prov:endTime'] = pd['activity'][ps_id]['prov:endTime']
                prov_es_info['activity'][ps_id_orig]['hysds:job_id'] = job['job_id']
                prov_es_info['activity'][ps_id_orig]['hysds:job_type'] = job['type']
                prov_es_info['activity'][ps_id_orig]['hysds:job_url'] = job['job_info']['job_url']
                prov_es_info['activity'][ps_id_orig]['hysds:mozart_url'] = app.conf.MOZART_URL
                if 'prov:type' not in prov_es_info['activity'][ps_id_orig]:
                    prov_es_info['activity'][ps_id_orig]['prov:type'] = pd['activity'][ps_id]['prov:type']

                # update wasAssociatedWith activity ids
                for waw_id in prov_es_info['wasAssociatedWith']:
                    if prov_es_info['wasAssociatedWith'][waw_id]['prov:activity'] == ps_id:
                        prov_es_info['wasAssociatedWith'][waw_id]['prov:activity'] = ps_id_orig
            else: prov_es_info['activity'].update(pd['activity'])
        else: prov_es_info['activity'] = pd['activity']
            
    # write prov
    with open(prov_es_file, 'w') as f:
        json.dump(prov_es_info, f, indent=2)


def log_publish_prov_es(prov_es_info, prov_es_file, prod_path, pub_urls,
                        prod_metrics, objectid):
    """Log publish step in PROV-ES document."""

    # create PROV-ES doc
    doc = ProvEsDocument(namespaces=prov_es_info['prefix'])

    # get bundle
    #bndl = doc.bundle(bundle_id)
    bndl = None

    # add input entity
    execute_node = socket.getfqdn()
    prod_url = "file://%s%s" % (execute_node, prod_path)
    input_id = "hysds:%s" % get_uuid(prod_url)
    input_ent = doc.granule(input_id, None, [prod_url], [], None, None, None,
                            label=os.path.basename(prod_url), bundle=bndl)

    # add output entity
    output_id = "hysds:%s" % get_uuid(pub_urls[0])
    output_ent = doc.product(output_id, None, [pub_urls[0]], [], None, None,
                             None, label=objectid, bundle=bndl)

    # software and algorithm
    algorithm = "eos:product_publishing"
    software_version = hysds.__version__
    software_title = "%s v%s" % (hysds.__description__, software_version)
    software = "eos:HySDS-%s" % software_version
    software_location = hysds.__url__
    doc.software(software, [algorithm], software_version, label=software_title,
                 location=software_location, bundle=bndl)

    # create sofware agent
    pid = os.getpid()
    sa_label = "hysds:publish_dataset/%s/%d/%s" % (execute_node, pid,
                                                   prod_metrics['time_start'])
    sa_id = "hysds:%s" % get_uuid(sa_label)
    doc.softwareAgent(sa_id, str(pid), execute_node, role="invoked",
                      label=sa_label, bundle=bndl)

    # create processStep
    job_id = "publish_dataset-%s" % os.path.basename(prod_path)
    doc.processStep("hysds:%s" % get_uuid(job_id), prod_metrics['time_start'],
                    prod_metrics['time_end'], [software], sa_id, None,
                    [input_id], [output_id], label=job_id, bundle=bndl,
                    prov_type="hysds:publish_dataset")

    # get json
    pd = json.loads(doc.serialize()) 

    # update input entity
    orig_ent = prov_es_info.get('entity', {}).get(input_id, {})
    pd['entity'][input_id].update(orig_ent)

    # update output entity
    for attr in orig_ent:
        if attr in ('prov:location', 'prov:label', 'prov:type'): continue
        pd['entity'][output_id][attr] = orig_ent[attr]

    # write prov
    with open(prov_es_file, 'w') as f:
        json.dump(pd, f, indent=2)
