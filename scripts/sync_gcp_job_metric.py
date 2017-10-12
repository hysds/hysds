#!/usr/bin/env python
import os, sys, json, time, traceback, logging, argparse, requests
from google.cloud import monitoring
from datetime import datetime

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/custom_gcp_metrics-jobs] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def get_waiting_job_count(job, user='guest', password='guest'):
    """Return number of jobs waiting."""

    # get rabbitmq admin api host
    host = app.conf.get('PYMONITOREDRUNNER_CFG', {}).get('rabbitmq', {}).get('hostname', 'localhost')

    # get number of jobs waiting (ready)
    url = "http://%s:15672/api/queues/%%2f/%s" % (host, job)
    r = requests.get(url, auth=(user, password))
    r.raise_for_status()
    res = r.json()
    return res['messages_ready']


def submit_metric(resource_id, project, job, job_count):
    """Submit GCP custom metric data."""

    metric_ns = 'HySDS'
    metric_name = 'JobsWaiting-%s' % job
    client = monitoring.Client()
    metric = client.metric('custom.googleapis.com/%s/%s' % (metric_ns, metric_name),
                            labels={ 'resource_id': resource_id })
    resource = client.resource('global', { })
    client.write_point(metric, resource, job_count, end_time=datetime.utcnow())
    logging.info("updated job count for %s queue as metric %s:%s: %s" %
                 (job, metric_ns, metric_name, job_count))


def daemon(project, job, interval):
    """Submit GCP custom metric for jobs waiting to be run."""

    # get resource id
    r = requests.get('http://metadata.google.internal/computeMetadata/v1/instance/id',
                     headers={'Metadata-Flavor': 'Google'})
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
        except Exception, e:
            logging.error("Got error: %s" % e)
            logging.error(traceback.format_exc())
        time.sleep(interval)    


if __name__ == "__main__":
    desc = "Sync GCP custom metric for number of jobs waiting to be run for a queue."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('project', help="GCP project ID")
    parser.add_argument('queue', help="HySDS job queue to monitor")
    parser.add_argument('-i', '--interval', type=int, default=60,
                        help="update time interval in seconds")
    args = parser.parse_args()
    daemon(args.project, args.queue, args.interval)
