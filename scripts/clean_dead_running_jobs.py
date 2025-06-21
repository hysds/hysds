#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import json
import os
import sys
from pprint import pprint
from subprocess import PIPE, Popen

import requests

from hysds.celery import app


def ping(host):
    """Return True if host is up. False if not."""

    p = Popen(["ping", "-c", "1", "-w", "5", host], stdout=PIPE, stderr=PIPE)
    status = p.wait()
    if status == 0:
        return True
    else:
        return False


def clean(es_url):
    """Remove any started jobs from ES job_status index if
    task dead."""

    idx = "job_status"
    doctype = "job"
    query = {"query": {"term": {"status": "job-started"}}}
    r = requests.post(
        f"{es_url}/{idx}/_search?search_type=scan&scroll=10m&size=100",
        data=json.dumps(query),
    )
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result["hits"]["total"]
    scroll_id = scan_result["_scroll_id"]
    started_jobs = []
    while True:
        r = requests.post(f"{es_url}/_search/scroll?scroll=10m", data=scroll_id)
        res = r.json()
        scroll_id = res["_scroll_id"]
        if len(res["hits"]["hits"]) == 0:
            break
        for hit in res["hits"]["hits"]:
            src = hit["_source"]
            started_jobs.append(
                {
                    "host": src["host"],
                    "execute_node": src["job"]["job_info"]["execute_node"],
                    "id": src["job_id"],
                    "task_id": src["job"]["task_id"],
                }
            )

    # loop and check task info
    for job in started_jobs:
        up = ping(job["execute_node"])
        # print job['execute_node'], up
        if not up:
            r = requests.delete(f"{es_url}/{idx}/{doctype}/_query?q=_id:{job['id']}")
            r.raise_for_status()
            res = r.json()
            print(f"Cleaned out job {job['id']} for host {job['execute_node']}.")


if __name__ == "__main__":
    job_status_es_url = sys.argv[1]
    clean(job_status_es_url)
