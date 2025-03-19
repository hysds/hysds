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


def get_queue_info(queue, user="guest", password="guest"):
    """Return RabbitMQ queue information."""

    # get rabbitmq admin api host
    host = (
        app.conf.get("PYMONITOREDRUNNER_CFG", {})
        .get("rabbitmq", {})
        .get("hostname", "localhost")
    )

    # request info for queue
    url = "http://%s:15672/api/queues/%%2f/%s" % (host, queue)
    r = requests.get(url, auth=(user, password))
    status_code = r.status_code
    if status_code == 200:
        return status_code, r.json()
    else:
        return status_code, None


def get_job_count(queue, user="guest", password="guest", total_jobs=False):
    """Return number of waiting jobs for a queue. If total_jobs is set
       to True, then the total number of jobs for a queue is returned."""

    # get number of jobs
    status_code, resp = get_queue_info(queue, user, password)
    if status_code == 200:
        return resp["messages" if total_jobs else "messages_ready"]
    else:
        return 0


def get_consumer_count(queue, user="guest", password="guest"):
    """Return number of consumers for a queue."""

    # get number of jobs
    status_code, resp = get_queue_info(queue, user, password)
    if status_code == 200:
        return resp["consumers"]
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

    return client.set_desired_capacity(
        AutoScalingGroupName=asg, DesiredCapacity=int(desired)
    )


def bootstrap_asg(asg, desired):
    """Bootstrap ASG's desired capacity."""

    c = boto3.client("autoscaling")
    set_desired_capacity(c, asg, desired)
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
        metric_name = "JobsPerInstance-%s" % (asg)
    else:
        metric_name = "JobsWaitingPerInstance-%s" % (asg)
    client = boto3.client("cloudwatch")
    put_metric_data(client, metric_name, metric_ns, asg, queue, metric)
    logging.info(
        "updated target tracking metric for %s queue and ASG %s as metric %s:%s: %s"
        % (queue, asg, metric_ns, metric_name, metric)
    )


def daemon(
    queue, asg, interval, namespace, user="guest", password="guest", total_jobs=False, use_consumer_count=False
):
    """Submit EC2 custom metric for an ASG's target tracking policy."""

    logging.info("queue: %s" % queue)
    logging.info("interval: %d" % interval)
    logging.info("namespace: %s" % namespace)
    while True:
        try:
            job_count = float(get_job_count(queue, user, password, total_jobs))
            if total_jobs:
                logging.info("jobs_total for %s queue: %s" % (queue, job_count))
            else:
                logging.info("jobs_waiting for %s queue: %s" % (queue, job_count))

            # calculate target tracking metric
            if use_consumer_count:
                # use consumer count
                consumer_count = float(get_consumer_count(queue, user, password))
                metric = job_count / (1.0 if consumer_count == 0. else consumer_count)
            else:
                # use ASG instance count
                desired_capacity, max_size = map(float, get_desired_capacity_max(asg))
                if desired_capacity == 0:
                    if job_count > 0:
                        desired_capacity = float(
                            bootstrap_asg(
                                asg, max_size if job_count > max_size else job_count
                            )
                        )
                        logging.info(
                            "bootstrapped ASG %s to desired=%s" % (asg, desired_capacity)
                        )
                    else:
                        desired_capacity = 1.0
                metric = job_count / desired_capacity
            submit_metric(queue, asg, metric, namespace, total_jobs)
        except Exception as e:
            logging.error("Got error: %s" % e)
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
    parser.add_argument(
        "-c",
        "--consumer_count",
        action="store_true",
        help="use RabbitMQ queue consumer count instead of ASG desired value (default)",
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
        args.consumer_count,
    )
