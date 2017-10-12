#!/usr/bin/env python
"""
Sync jobs stuck in job-queued or job-started state that may have been
de-synchronized. It searches through task statuses stored in ES for
final celery task status and resynchronizes job status. If no task
status exists in ES, it queries the celery task metadata store in redis
for task status. If none exists, the job is offlined.
"""

import os, sys, requests, json, logging, argparse
from redis import ConnectionPool, StrictRedis
from kombu.serialization import loads, registry, prepare_accept_content

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/offline_orphaned_jobs] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# redis connection pool
POOL = None


def set_redis_pool():
    """Set redis connection pool for status updates."""

    global POOL
    if POOL is None:
        POOL = ConnectionPool.from_url(app.conf.REDIS_UNIX_DOMAIN_SOCKET)


def offline_orphaned_jobs(es_url, dry_run=False):
    """Set jobs with job-queued or job-started state to job-offline
       if not synced with celery task state."""
    
    # get redis connection
    set_redis_pool()
    global POOL
    rd = StrictRedis(connection_pool=POOL)

    # get celery task result serializer
    content_type, content_encoding, encoder = registry._encoders[app.conf.CELERY_RESULT_SERIALIZER]
    accept = prepare_accept_content(app.conf.CELERY_ACCEPT_CONTENT)
    logging.info("content_type: {}".format(content_type))
    logging.info("content_encoding: {}".format(content_encoding))
    logging.info("encoder: {}".format(encoder))
    logging.info("accept: {}".format(accept))

    # query
    query = {
        "query": {
            "bool": {
                "must": [
                    {   
                        "terms": {
                            "status": [ "job-started", "job-queued" ]
                        }
                    }
                ]
            }
        },
        "_source": [ "status", "tags", "uuid" ]
    }
    url_tmpl = "{}/job_status-current/_search?search_type=scan&scroll=10m&size=100"
    r = requests.post(url_tmpl.format(es_url), data=json.dumps(query))
    if r.status_code != 200:
        logging.error("Failed to query ES. Got status code %d:\n%s" %
                      (r.status_code, json.dumps(query, indent=2)))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']

    # get list of results
    results = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']: results.append(hit)


    # check for celery state
    for res in results:
        id = res['_id']
        src = res.get('_source', {})
        status = src['status']
        tags = src.get('tags', [])
        task_id = src['uuid']

        # check celery task status in ES
        task_query = {
            "query": {
                "term": {
                    "_id": task_id
                }
            },
            "_source": [ "status" ]
        }
        r = requests.post('%s/task_status-current/task/_search' % es_url,
                          data=json.dumps(task_query))
        if r.status_code != 200:
            logging.error("Failed to query ES. Got status code %d:\n%s" %
                          (r.status_code, json.dumps(task_query, indent=2)))
            continue
        task_res = r.json()
        if task_res['hits']['total'] > 0:
            task_info = task_res['hits']['hits'][0]
            if task_info['_source']['status'] == 'task-failed': updated_status = 'job-failed'
            elif task_info['_source']['status'] == 'task-succeeded': updated_status = 'job-completed'
            elif task_info['_source']['status'] in ('task-sent', 'task-started'): continue
            else:
                logging.error("Cannot handle task status %s for %s." % (task_info['_source']['status'], task_id))
                continue
            if dry_run:
                logging.info("Would've update job status to %s for %s." % (updated_status, task_id))
            else:
                new_doc = {
                    "doc": { "status": updated_status },
                    "doc_as_upsert": True
                }
                r = requests.post('%s/job_status-current/job/%s/_update' % (es_url, id),
                                  data=json.dumps(new_doc))
                result = r.json()
                if r.status_code != 200:
                    logging.error("Failed to update tags for %s. Got status code %d:\n%s" %
                                  (id, r.status_code, json.dumps(result, indent=2)))
                r.raise_for_status()
                logging.info("Set job %s to %s." % (id, updated_status))
            continue

        # get celery task metadata in redis
        task_meta = loads(rd.get('celery-task-meta-%s' % task_id),
                          content_type=content_type,
                          content_encoding=content_encoding,
                          accept=accept)
        if task_meta is None:
            updated_status = 'job-offline'
            if dry_run:
                logging.info("Would've update job status to %s for %s." % (updated_status, task_id))
            else:
                new_doc = {
                    "doc": { "status": updated_status },
                    "doc_as_upsert": True
                }
                r = requests.post('%s/job_status-current/job/%s/_update' % (es_url, id),
                                  data=json.dumps(new_doc))
                result = r.json()
                if r.status_code != 200:
                    logging.error("Failed to update tags for %s. Got status code %d:\n%s" %
                                  (id, r.status_code, json.dumps(result, indent=2)))
                r.raise_for_status()
                logging.info("Set job %s to %s." % (id, updated_status))
            continue


if __name__ == "__main__":
    host = app.conf.get('JOBS_ES_URL', 'http://localhost:9200')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-d', '--dry_run', help="dry run", action='store_true')
    args = parser.parse_args()

    # prompt user system is quiet
    print("\033[1;31;40mThis script should run when no workers are consuming jobs from any queues.")
    print("(i.e. RabbitMQ admin shows all 0's in the unacked column).\033[0m")
    try: input_str = raw_input("Type YES to continue: ").strip()
    except: sys.exit(1)
    if input_str != "YES": sys.exit(0)

    offline_orphaned_jobs(host, args.dry_run)
