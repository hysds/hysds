#!/usr/bin/env python
import os, sys, json, time, traceback, logging, argparse, requests
# from google.cloud import monitoring
from google.cloud import monitoring_v3
from datetime import datetime

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/custom_gcp_metrics-queues] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

def create_metric_descriptor(project, queue):
    client = monitoring_v3.MetricServiceClient()
    project_name = client.project_path(project)
    descriptor = monitoring_v3.types.MetricDescriptor()
    descriptor.type = 'custom.googleapis.com/HySDS/' + queue
    descriptor.metric_kind = (
        monitoring_v3.enums.MetricDescriptor.MetricKind.GAUGE)
    descriptor.value_type = (
        monitoring_v3.enums.MetricDescriptor.ValueType.DOUBLE)
    descriptor = client.create_metric_descriptor(project_name, descriptor)


def get_waiting_queue_count(queue, user='guest', password='guest'):
    """Return number of queues waiting."""

    # get rabbitmq admin api host
    host = app.conf.get('PYMONITOREDRUNNER_CFG', {}).get('rabbitmq', {}).get('hostname', 'localhost')

    # get number of queues waiting (ready)
    url = "http://%s:15672/api/queues/%%2f/%s" % (host, queue)
    r = requests.get(url, auth=(user, password))
    r.raise_for_status()
    res = r.json()
    return res['messages_ready']


def submit_metric(resource_id, project, queue, queue_count):
    """Submit GCP custom metric data."""

    metric_ns = 'HySDS'
    metric_name = queue
    #client = monitoring.Client()
    client = monitoring_v3.MetricServiceClient()
    metric = client.metric('custom.googleapis.com/%s/%s' % (metric_ns, metric_name),
                            labels={ 'resource_id': resource_id })
    resource = client.resource('global', { })
    client.write_point(metric, resource, queue_count, end_time=datetime.utcnow())
    logging.info("updated queue count for %s queue as metric %s:%s: %s" %
                 (queue, metric_ns, metric_name, queue_count))


def daemon(project, queue, interval):
    """Submit GCP custom metric for queues waiting to be run."""

    # get resource id
    r = requests.get('http://metadata.google.internal/computeMetadata/v1/instance/id',
                     headers={'Metadata-Flavor': 'Google'})
    resource_id = r.content

    logging.info("resource_id: %s" % resource_id)
    logging.info("project: %s" % project)
    logging.info("queue: %s" % queue)
    logging.info("interval: %d" % interval)

    while True:
        try:
            queue_count = get_waiting_queue_count(queue)
            logging.info("queues_waiting for %s queue: %s" % (queue, queue_count))
            submit_metric(resource_id, project, queue, queue_count)
        except Exception, e:
            logging.error("Got error: %s" % e)
            logging.error(traceback.format_exc())
        time.sleep(interval)    


if __name__ == "__main__":
    desc = "Sync GCP custom metric for number of queues waiting to be run for a queue."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('project', help="GCP project ID")
    parser.add_argument('queue', help="HySDS queue queue to monitor")
    parser.add_argument('-i', '--interval', type=int, default=60,
                        help="update time interval in seconds")
    args = parser.parse_args()
    daemon(args.project, args.queue, args.interval)
