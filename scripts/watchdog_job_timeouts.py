#!/usr/bin/env python
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
from builtins import int
from future import standard_library
standard_library.install_aliases()
import os
import sys
import json
import time
import traceback
import logging
import argparse
import random
from datetime import datetime

from hysds.utils import parse_iso8601
from hysds.celery import app
import job_utils

log_format = "[%(asctime)s: %(levelname)s/watchdog_job_timeouts] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def tag_timedout_jobs(url, timeout):
    """Tag jobs stuck in job-started or job-offline that have timed out."""

    status = ["job-started", "job-offline"]
    source_data = [
            "status", "tags", "uuid", "celery_hostname",
            "job.job_info.time_start", "job.job_info.time_limit"
        ]
    query = job_utils.get_timedout_query(timeout, status, source_data)
    results = job_utils.run_query_with_scroll(query, index="job_status-current")
    logging.info("Found %d stuck jobs in job-started or job-offline" % len(results) +
                 " older than %d seconds." % timeout)

    # tag each with timedout
    for res in results:
        id = res['_id']
        src = res.get('_source', {})
        status = src['status']
        tags = src.get('tags', [])
        task_id = src['uuid']
        celery_hostname = src['celery_hostname']
        logging.info("job_info: {}".format(json.dumps(src)))

        # get job duration
        time_limit = src['job']['job_info']['time_limit']
        logging.info("time_limit: {}".format(time_limit))
        time_start = parse_iso8601(src['job']['job_info']['time_start'])
        logging.info("time_start: {}".format(time_start))
        time_now = datetime.utcnow()
        logging.info("time_now: {}".format(time_now))
        duration = (time_now - time_start).seconds
        logging.info("duration: {}".format(duration))

        if status == "job-started":
            # get task info
            task_query = {
                "query": {
                    "term": {
                        "_id": task_id
                    }
                },
                "_source": ["status"]
            }
            task_res = job_utils.es_query(task_query)
          
            if len(task_res['hits']['hits'])==0:
                logging.error("No result found with : query\n%s" %
                              (json.dumps(task_query, indent=2)))

            logging.info("task_res: {}".format(json.dumps(task_res)))

            # get worker info
            worker_query = {
                "query": {
                    "term": {
                        "_id": celery_hostname
                    }
                },
                "_source": ["status", "tags"]
            }

            worker_res = job_utils.es_query(worker_query)
            
            if len(worker_res['hits']['hits'])==0:
                logging.error("No result found with : query\n%s" %
                              (json.dumps(worker_query, indent=2)))

            worker_res = r.json()
            logging.info("worker_res: {}".format(json.dumps(worker_res)))

            # determine new status
            new_status = status
            if worker_res['hits']['total'] == 0 and duration > time_limit:
                new_status = 'job-offline'
            if worker_res['hits']['total'] > 0 and (
                "timedout" in worker_res['hits']['hits'][0]['_source'].get('tags', []) or \
                worker_res['hits']['hits'][0]['_source']['status'] == 'worker-offline'):
                new_status = 'job-offline'
            if task_res['hits']['total'] > 0:
                task_info = task_res['hits']['hits'][0]
                if task_info['_source']['status'] == 'task-failed':
                    new_status = 'job-failed'

            # update status
            if status != new_status:
                logger.info("updating status from {} to {}".format(status, new_status))
                if duration > time_limit and 'timedout' not in tags:
                    tags.append('timedout')
                new_doc = {
                    "doc": {"status": new_status,
                            "tags": tags },
                    "doc_as_upsert": True
                }
                print(json.dumps(new_doc, indent=2))
                response = job_utils.update_es(id, new_doc, url=url, index = "job_status-current")
                if response['result'].strip() != "updated":
                     err_str = "Failed to update status for {} : {}".format(id, json.dumps(response, indent=2))
                     logging.error(err_str)
                     raise Exception(err_str)
                logging.info("Set job {} to {} and tagged as timedout.".format(id, new_status))
                continue

        if 'timedout' in tags:
            logging.info("%s already tagged as timedout." % id)
        else:
            if duration > time_limit:
                tags.append('timedout')
                new_doc = {
                    "doc": {"tags": tags},
                    "doc_as_upsert": True
                }
                print(json.dumps(new_doc, indent=2))
                response = job_utils.update_es(id, new_doc, url=url, index = "job_status-current")
                if response['result'].strip() != "updated":
                     err_str = "Failed to update status for {} : {}".format(id, json.dumps(response, indent=2))
                     logging.error(err_str)
                     raise Exception(err_str)

def daemon(interval, url, timeout):
    """Watch for jobs that have timed out in job-started or job-offline state."""

    interval_min = interval - int(interval/4)
    interval_max = int(interval/4) + interval

    logging.info("interval min: %d" % interval_min)
    logging.info("interval max: %d" % interval_max)
    logging.info("url: %s" % url)
    logging.info("timeout threshold: %d" % timeout)

    while True:
        try:
            tag_timedout_jobs(url, timeout)
        except Exception as e:
            logging.error("Got error: %s" % e)
            logging.error(traceback.format_exc())
            traceback.format_exc()
        time.sleep(random.randint(interval_min, interval_max))


if __name__ == "__main__":
    desc = "Watchdog jobs stuck in job-offline or job-started."
    host = app.conf.get('JOBS_ES_URL', 'http://localhost:9200')
    print("host : {}".format(host))
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-i', '--interval', type=int, default=3600,
                        help="wake-up time interval in seconds")
    parser.add_argument('-u', '--url', default=host, help="ElasticSearch URL")
    parser.add_argument('-t', '--timeout', type=int, default=86400,
                        help="timeout threshold")
    args = parser.parse_args()
    daemon(args.interval, args.url, args.timeout)
