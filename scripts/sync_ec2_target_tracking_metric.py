#!/usr/bin/env python
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
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
import boto3
import botocore
import requests
import backoff

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/custom_ec2_metrics-jobs] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def get_total_job_count(queue, user="guest", password="guest"):
    """Return total number of jobs for a queue."""

    # get rabbitmq admin api host
    host = (
        app.conf.get("PYMONITOREDRUNNER_CFG", {})
        .get("rabbitmq", {})
        .get("hostname", "localhost")
    )

    # get total number of jobs
    url = "http://%s:15672/api/queues/%%2f/%s" % (host, queue)
    r = requests.get(url, auth=(user, password))
    # r.raise_for_status()
    if r.status_code == 200:
        return r.json()["messages"]
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
        raise RuntimeError("Autoscaling group {} not found.".format(asg))
    return groups[0]["DesiredCapacity"], groups[0]["MaxSize"]


@backoff.on_exception(
    backoff.expo, botocore.exceptions.ClientError, max_tries=8, max_value=64
)
def set_desired_capacity(client, asg, desired):
    """Backoff wrapper for set_desired_capacity()."""

    return client.set_desired_capacity(AutoScalingGroupName=asg, DesiredCapacity=int(desired))


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


def submit_metric(queue, asg, metric, metric_ns):
    """Submit EC2 custom metric data."""

    metric_name = "JobsPerInstance-%s" % (asg)
    client = boto3.client("cloudwatch")
    put_metric_data(client, metric_name, metric_ns, asg, queue, metric)
    logging.info(
        "updated target tracking metric for %s queue and ASG %s as metric %s:%s: %s"
        % (queue, asg, metric_ns, metric_name, metric)
    )


def daemon(queue, asg, interval, namespace, user="guest", password="guest"):
    """Submit EC2 custom metric for the ratio of jobs to workers."""

    logging.info("queue: %s" % queue)
    logging.info("interval: %d" % interval)
    logging.info("namespace: %s" % namespace)
    while True:
        try:
            job_count = float(get_total_job_count(queue, user, password))
            logging.info("jobs_total for %s queue: %s" % (queue, job_count))
            desired_capacity, max_size = map(float, get_desired_capacity_max(asg))
            if desired_capacity == 0:
                if job_count > 0:
                    desired_capacity = float(bootstrap_asg(asg, max_size if job_count > max_size else job_count))
                    logging.info(
                        "bootstrapped ASG %s to desired=%s" % (asg, desired_capacity)
                    )
                else:
                    desired_capacity = 1.0
            metric = job_count / desired_capacity
            submit_metric(queue, asg, metric, namespace)
        except Exception as e:
            logging.error("Got error: %s" % e)
            logging.error(traceback.format_exc())
        time.sleep(interval)


if __name__ == "__main__":
    desc = "Sync EC2 custom metric for ratio of jobs to workers for a queue."
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
    args = parser.parse_args()
    daemon(
        args.queue, args.asg, args.interval, args.namespace, args.user, args.password
    )
