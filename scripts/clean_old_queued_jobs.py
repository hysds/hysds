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


def clean(es_url, time_queued):
    """Remove any queued jobs from ES job_status index if
       the time_queued for the task is earlier than the passed
       in time_queued."""
    
    idx = 'job_status'
    doctype = 'job'
    query = { "query": {
                "term": { "status": "job-queued" }
              },
              "filter": {
                "and": [
                  {
                    "bool": {
                      "must": [
                        {
                          "range": {
                            "job.job_info.time_queued": {
                              "lte": time_queued
                            }
                          }
                        }
                      ]
                    }
                  }
                ]
              }
            }
    #print json.dumps(query, indent=2)
    r = requests.post('%s/%s/_search?search_type=scan&scroll=10m&size=100' %
                      (es_url, idx), data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    queued_jobs = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']:
            src = hit['_source']
            queued_jobs.append(src['job_id'])
    #print '\n'.join(queued_jobs)

    # loop and check task info
    for job_id in queued_jobs:
        #print job
        r = requests.delete("%s/%s/%s/_query?q=_id:%s" % (es_url, idx, doctype, job_id))
        r.raise_for_status()
        res = r.json()
        print "Cleaned out queued job %s." % job_id


if __name__ == "__main__":
    job_status_es_url = sys.argv[1]
    time_queued = sys.argv[2]
    clean(job_status_es_url, time_queued)
