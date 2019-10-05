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
import boto3
import requests

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/watchdog_task_timeouts] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def tag_timedout_tasks(url, timeout):
    """Tag tasks stuck in task-started that have timed out."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "status": ["task-started"]
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "lt": "now-%ds" % timeout
                            }
                        }
                    }
                ]
            }
        },
        "_source": ["status", "tags", "uuid"]
    }

    # query
    url_tmpl = "{}/task_status-current/_search?search_type=scan&scroll=10m&size=100"
    r = requests.post(url_tmpl.format(url), data=json.dumps(query))
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
        r = requests.post('%s/_search/scroll?scroll=10m' % url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0:
            break
        for hit in res['hits']['hits']:
            results.append(hit)

    logging.info("Found %d stuck tasks in task-started" % len(results) +
                 " older than %d seconds." % timeout)

    # tag each with timedout
    for res in results:
        id = res['_id']
        src = res.get('_source', {})
        status = src['status']
        tags = src.get('tags', [])
        task_id = src['uuid']

        if 'timedout' not in tags:
            tags.append('timedout')
            new_doc = {
                "doc": {"tags": tags},
                "doc_as_upsert": True
            }
            r = requests.post('%s/task_status-current/task/%s/_update' % (url, id),
                              data=json.dumps(new_doc))
            result = r.json()
            if r.status_code != 200:
                logging.error("Failed to update tags for %s. Got status code %d:\n%s" %
                              (id, r.status_code, json.dumps(result, indent=2)))
            r.raise_for_status()
            logging.info("Tagged %s as timedout." % id)
        else:
            logging.info("%s already tagged as timedout." % id)


def daemon(interval, url, timeout):
    """Watch for tasks that have timed out in task-started state."""

    interval_min = interval - int(interval/4)
    interval_max = int(interval/4) + interval

    logging.info("interval min: %d" % interval_min)
    logging.info("interval max: %d" % interval_max)
    logging.info("url: %s" % url)
    logging.info("timeout threshold: %d" % timeout)

    while True:
        try:
            tag_timedout_tasks(url, timeout)
        except Exception as e:
            logging.error("Got error: %s" % e)
            logging.error(traceback.format_exc())
        time.sleep(random.randint(interval_min, interval_max))


if __name__ == "__main__":
    desc = "Watchdog tasks stuck in task-started."
    host = app.conf.get('JOBS_ES_URL', 'http://localhost:9200')
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-i', '--interval', type=int, default=3600,
                        help="wake-up time interval in seconds")
    parser.add_argument('-u', '--url', default=host, help="ElasticSearch URL")
    parser.add_argument('-t', '--timeout', type=int, default=86400,
                        help="timeout threshold")
    args = parser.parse_args()
    daemon(args.interval, args.url, args.timeout)
