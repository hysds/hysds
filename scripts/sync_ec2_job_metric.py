#!/usr/bin/env python
import os, sys, json, time, traceback, logging, argparse
import boto3, requests

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/custom_ec2_metrics-jobs] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def get_waiting_job_count(job, user='guest', password='guest'):
    """Return number of jobs waiting."""

    # get rabbitmq admin api host
    host = app.conf.get('PYMONITOREDRUNNER_CFG', {}).get('rabbitmq', {}).get('hostname', 'localhost')

    # get number of jobs waiting (ready)
    url = "http://%s:15672/api/queues/%%2f/%s" % (host, job)
    r = requests.get(url, auth=(user, password))
    #r.raise_for_status()
    if r.status_code == 200:
        return r.json()['messages_ready']
    else: return 0


def submit_metric(job, job_count, metric_ns):
    """Submit EC2 custom metric data."""

    metric_name = 'JobsWaiting-%s' % job
    client = boto3.client('cloudwatch')
    client.put_metric_data(Namespace=metric_ns,
                           MetricData=[{
                               'MetricName': metric_name,
                               'Value': job_count,
                               'Unit':  'Count'
                           }])
    logging.info("updated job count for %s queue as metric %s:%s: %s" %
                 (job, metric_ns, metric_name, job_count))


def daemon(job, interval, namespace, user="guest", password="guest"):
    """Submit EC2 custom metric for jobs waiting to be run."""

    logging.info("queue: %s" % job)
    logging.info("interval: %d" % interval)
    logging.info("namespace: %s" % namespace)
    while True:
        try:
            job_count = get_waiting_job_count(job, user, password)
            logging.info("jobs_waiting for %s queue: %s" % (job, job_count))
            submit_metric(job, job_count, namespace)
        except Exception, e:
            logging.error("Got error: %s" % e)
            logging.error(traceback.format_exc())
        time.sleep(interval)    


if __name__ == "__main__":
    desc = "Sync EC2 custom metric for number of jobs waiting to be run for a queue."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('queue', help="HySDS job queue to monitor")
    parser.add_argument('-i', '--interval', type=int, default=60,
                        help="update time interval in seconds")
    parser.add_argument('-n', '--namespace', default='HySDS',
                        help="CloudWatch metrics namespace")
    parser.add_argument('-u', '--user', default="guest", help="rabbitmq user")
    parser.add_argument('-p', '--password', default="guest", help="rabbitmq password")
    args = parser.parse_args()
    daemon(args.queue, args.interval, args.namespace, args.user, args.password)
