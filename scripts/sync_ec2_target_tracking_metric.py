#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import argparse
import json
import logging
import os
import sys
import time
import traceback

import backoff
import boto3
import botocore
import requests
import urllib3
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.ssl_ import create_urllib3_context
import backoff

from hysds.celery import app

log_format = "[%(asctime)s: %(levelname)s/custom_ec2_metrics-jobs] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# class for custom cipher for rabbitmq
class CustomCipherAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ssl_context = create_urllib3_context(ciphers=app.conf.get("broker_use_ssl", {}).get("ciphers"))
        kwargs['ssl_context'] = ssl_context
        return super(CustomCipherAdapter, self).init_poolmanager(*args, **kwargs)

def get_job_count(queue, user="guest", password="guest", total_jobs=False):
    """Return number of waiting jobs for a queue. If total_jobs is set
    to True, then the total number of jobs for a queue is returned."""

    # get rabbitmq admin api host
    host = (
        app.conf.get("PYMONITOREDRUNNER_CFG", {})
        .get("rabbitmq", {})
        .get("hostname", "localhost")
    )

    # get number of jobs
    session = requests.Session()
    session.mount("https://", CustomCipherAdapter())

    url = "https://%s:15673/api/queues/%%2f/%s" % (host, queue)
    r = session.get(url, auth=(user, password), verify=False)
    # r.raise_for_status()
    if r.status_code == 200:
        return r.json()["messages" if total_jobs else "messages_ready"]
    else:
        return 0


@backoff.on_exception(
    backoff.expo, botocore.exceptions.ClientError, max_tries=8, max_value=64
)
def describe_asg(client, asg):
    """Backoff wrapper for describe_auto_scaling_groups()."""

    return client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg])


def get_desired_capacity_max(asg):
    """Get current value of ASG's desired capacity and max size."""

    c = boto3.client("autoscaling")
    r = describe_asg(c, asg)
    groups = r["AutoScalingGroups"]
    if len(groups) == 0:
        raise RuntimeError(f"Autoscaling group {asg} not found.")
    return groups[0]["DesiredCapacity"], groups[0]["MaxSize"]


@backoff.on_exception(
    backoff.expo, botocore.exceptions.ClientError, max_tries=8, max_value=64
)
def set_desired_capacity(client, asg, desired):
    """Backoff wrapper for set_desired_capacity()."""

    return client.set_desired_capacity(
        AutoScalingGroupName=asg, DesiredCapacity=int(desired)
    )


def bootstrap_asg(asg, desired):
    """Bootstrap ASG's desired capacity."""

    c = boto3.client("autoscaling")
    r = set_desired_capacity(c, asg, desired)
    return desired


@backoff.on_exception(
    backoff.expo, botocore.exceptions.ClientError, max_tries=8, max_value=64
)
def put_metric_data(client, metric_name, metric_ns, asg, queue, metric):
    """Backoff wrapper for put_metric_data()."""

    client.put_metric_data(
        Namespace=metric_ns,
        MetricData=[
            {
                "MetricName": metric_name,
                "Dimensions": [
                    {"Name": "AutoScalingGroupName", "Value": asg},
                    {"Name": "Queue", "Value": queue},
                ],
                "Value": metric,
            }
        ],
    )


def submit_metric(queue, asg, metric, metric_ns, total_jobs=False):
    """Submit EC2 custom metric data."""

    if total_jobs:
        metric_name = f"JobsPerInstance-{asg}"
    else:
        metric_name = f"JobsWaitingPerInstance-{asg}"
    client = boto3.client("cloudwatch")
    put_metric_data(client, metric_name, metric_ns, asg, queue, metric)
    logging.info(
        "updated target tracking metric for %s queue and ASG %s as metric %s:%s: %s"
        % (queue, asg, metric_ns, metric_name, metric)
    )


def daemon(
    queue, asg, interval, namespace, user="guest", password="guest", total_jobs=False
):
    """Submit EC2 custom metric for an ASG's target tracking policy."""

    logging.info(f"queue: {queue}")
    logging.info(f"interval: {interval}")
    logging.info(f"namespace: {namespace}")
    while True:
        try:
            job_count = float(get_job_count(queue, user, password, total_jobs))
            if total_jobs:
                logging.info(f"jobs_total for {queue} queue: {job_count}")
            else:
                logging.info(f"jobs_waiting for {queue} queue: {job_count}")
            desired_capacity, max_size = map(float, get_desired_capacity_max(asg))
            if desired_capacity == 0:
                if job_count > 0:
                    desired_capacity = float(
                        bootstrap_asg(
                            asg, max_size if job_count > max_size else job_count
                        )
                    )
                    logging.info(
                        f"bootstrapped ASG {asg} to desired={desired_capacity}"
                    )
                else:
                    desired_capacity = 1.0
            metric = job_count / desired_capacity
            submit_metric(queue, asg, metric, namespace, total_jobs)
        except Exception as e:
            logging.error(f"Got error: {e}")
            logging.error(traceback.format_exc())
        time.sleep(interval)


if __name__ == "__main__":
    desc = "Sync EC2 custom metric to drive target tracking autoscaling policy."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("queue", help="HySDS job queue to monitor")
    parser.add_argument("asg", help="Autoscaling group name")
    parser.add_argument(
        "-i", "--interval", type=int, default=60, help="update time interval in seconds"
    )
    parser.add_argument(
        "-n", "--namespace", default="HySDS", help="CloudWatch metrics namespace"
    )
    parser.add_argument("-u", "--user", default="guest", help="rabbitmq user")
    parser.add_argument("-p", "--password", default="guest", help="rabbitmq password")
    parser.add_argument(
        "-t",
        "--total_jobs",
        action="store_true",
        help="use total job count instead of waiting job count (default)",
    )
    args = parser.parse_args()
    daemon(
        args.queue,
        args.asg,
        args.interval,
        args.namespace,
        args.user,
        args.password,
        args.total_jobs,
    )
