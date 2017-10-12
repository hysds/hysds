#!/usr/bin/env python
import os, sys, requests, json
from subprocess import Popen, PIPE
from pprint import pprint

from hysds.celery import app


def ping(host):
    """Return True if host is up. False if not."""

    p = Popen(['ping', '-c', '1', '-w', '5', host], stdout=PIPE, stderr=PIPE)
    status = p.wait()
    if status == 0: return True
    else: return False


def clean(es_url, start_time):
    """Remove any started jobs from ES job_status index if
       the start_time for the task is earlier than the passed
       in start_time."""
    
    idx = 'job_status-current'
    doctype = 'job'
    query = { "query": {
                "term": { "status": "job-started" }
              },
              "filter": {
                "and": [
                  {
                    "bool": {
                      "must": [
                        {
                          "range": {
                            "job.job_info.time_start": {
                              "lte": start_time
                            }
                          }
                        }
                      ]
                    }
                  }
                ]
              }
            }
    r = requests.post('%s/%s/_search?search_type=scan&scroll=10m&size=100' %
                      (es_url, idx), data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    started_jobs = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']:
            src = hit['_source']
            started_jobs.append({'execute_node': src['job']['job_info']['execute_node'],
                                 'id': src['job_id'], 'task_id': src['job']['task_id']})

    # loop and check task info
    for job in started_jobs:
        #print job
        r = requests.delete("%s/%s/%s/%s" % (es_url, idx, doctype, job['task_id']))
        r.raise_for_status()
        res = r.json()
        print "Cleaned out job %s for host %s." % (job['id'], job['execute_node'])


if __name__ == "__main__":
    job_status_es_url = sys.argv[1]
    start_time = sys.argv[2]
    clean(job_status_es_url, start_time)
