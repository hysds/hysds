#!/usr/bin/env python
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library

standard_library.install_aliases()
import os
import sys
import json
import time
import traceback
import logging
import argparse
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.ssl_ import create_urllib3_context
from google.cloud import monitoring
from datetime import datetime

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/custom_gcp_metrics-jobs] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# class for custom cipher for rabbitmq
class CustomCipherAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ssl_context = create_urllib3_context(ciphers="DHE-RSA-AES128-GCM-SHA256")
        kwargs['ssl_context'] = ssl_context
        return super(CustomCipherAdapter, self).init_poolmanager(*args, **kwargs)

def get_waiting_job_count(job, user="guest", password="guest"):
    """Return number of jobs waiting."""

    # get rabbitmq admin api host
    host = (
        app.conf.get("PYMONITOREDRUNNER_CFG", {})
        .get("rabbitmq", {})
        .get("hostname", "localhost")
    )

    session = requests.Session()
    session.mount("https://", CustomCipherAdapter())
    # get number of jobs waiting (ready)
    url = "https://%s:15672/api/queues/%%2f/%s" % (host, job)
    r = session.get(url, auth=(user, password), verify=None)
    r.raise_for_status()
    res = r.json()
    return res["messages_ready"]


def submit_metric(resource_id, project, job, job_count):
    """Submit GCP custom metric data."""

    metric_ns = "HySDS"
    metric_name = "JobsWaiting-%s" % job
    client = monitoring.Client()
    metric = client.metric(
        "custom.googleapis.com/%s/%s" % (metric_ns, metric_name),
        labels={"resource_id": resource_id},
    )
    resource = client.resource("global", {})
    client.write_point(metric, resource, job_count, end_time=datetime.utcnow())
    logging.info(
        "updated job count for %s queue as metric %s:%s: %s"
        % (job, metric_ns, metric_name, job_count)
    )


def daemon(project, job, interval):
    """Submit GCP custom metric for jobs waiting to be run."""

    # get resource id
    r = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/id",
        headers={"Metadata-Flavor": "Google"},
    )
    resource_id = r.content

    logging.info("resource_id: %s" % resource_id)
    logging.info("project: %s" % project)
    logging.info("queue: %s" % job)
    logging.info("interval: %d" % interval)

    while True:
        try:
            job_count = get_waiting_job_count(job)
            logging.info("jobs_waiting for %s queue: %s" % (job, job_count))
            submit_metric(resource_id, project, job, job_count)
        except Exception as e:
            logging.error("Got error: %s" % e)
            logging.error(traceback.format_exc())
        time.sleep(interval)


if __name__ == "__main__":
    desc = "Sync GCP custom metric for number of jobs waiting to be run for a queue."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("project", help="GCP project ID")
    parser.add_argument("queue", help="HySDS job queue to monitor")
    parser.add_argument(
        "-i", "--interval", type=int, default=60, help="update time interval in seconds"
    )
    args = parser.parse_args()
    daemon(args.project, args.queue, args.interval)
