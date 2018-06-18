#!/usr/bin/env python
import os, sys, json, time, traceback, logging, argparse
import boto3, requests

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/custom_ec2_metrics-jobs] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def get_waiting_job_count(queue, user='guest', password='guest'):
    """Return number of jobs waiting."""

    # get rabbitmq admin api host
    host = app.conf.get('PYMONITOREDRUNNER_CFG', {}).get('rabbitmq', {}).get('hostname', 'localhost')

    # get number of jobs waiting (ready)
    url = "http://%s:15672/api/queues/%%2f/%s" % (host, queue)
    r = requests.get(url, auth=(user, password))
    #r.raise_for_status()
    if r.status_code == 200:
        return r.json()['messages_ready']
    else: return 0


def get_desired_capacity(asg):
    """Get current value of ASG's desired capacity."""

    c = boto3.client('autoscaling')
    r = c.describe_auto_scaling_groups(AutoScalingGroupNames=[asg])
    groups = r['AutoScalingGroups']
    if len(groups) == 0:
        raise RuntimeError("Autoscaling group %s not found." % asg)
    return groups[0]['DesiredCapacity']


def bootstrap_asg(asg):
    """Bootstrap ASG's desired to 1."""

    c = boto3.client('autoscaling')
    r = c.set_desired_capacity(AutoScalingGroupName=asg, DesiredCapacity=1)
    return 1


def submit_metric(queue, asg, metric, metric_ns):
    """Submit EC2 custom metric data."""

    metric_name = 'JobsWaitingPerInstance-%s' % (asg)
    client = boto3.client('cloudwatch')
    client.put_metric_data(Namespace=metric_ns,
                           MetricData=[{
                               'MetricName': metric_name,
                               'Dimensions': [
                                   {
                                       'Name': 'AutoScalingGroupName',
                                       'Value': asg,
                                   },
                                   {
                                       'Name': 'Queue',
                                       'Value': queue,
                                   },
                               ],
                               'Value': metric
                           }])
    logging.info("updated target tracking metric for %s queue and ASG %s as metric %s:%s: %s" %
                 (queue, asg, metric_ns, metric_name, metric))


def daemon(queue, asg, interval, namespace, user="guest", password="guest"):
    """Submit EC2 custom metric for jobs waiting to be run."""

    logging.info("queue: %s" % queue)
    logging.info("interval: %d" % interval)
    logging.info("namespace: %s" % namespace)
    while True:
        try:
            job_count = float(get_waiting_job_count(queue, user, password))
            logging.info("jobs_waiting for %s queue: %s" % (queue, job_count))
            desired_capacity = float(get_desired_capacity(asg))
            if desired_capacity == 0:
                if job_count > 0:
                    desired_capacity = float(bootstrap_asg(asg))
                    logging.info("bootstrapped ASG %s to desired=%s" % (asg, desired_capacity))
                else: desired_capacity = 1.0
            metric = job_count/desired_capacity
            submit_metric(queue, asg, metric, namespace)
        except Exception, e:
            logging.error("Got error: %s" % e)
            logging.error(traceback.format_exc())
        time.sleep(interval)    


if __name__ == "__main__":
    desc = "Sync EC2 custom metric for number of jobs waiting to be run for a queue."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('queue', help="HySDS job queue to monitor")
    parser.add_argument('asg', help="Autoscaling group name")
    parser.add_argument('-i', '--interval', type=int, default=60,
                        help="update time interval in seconds")
    parser.add_argument('-n', '--namespace', default='HySDS',
                        help="CloudWatch metrics namespace")
    parser.add_argument('-u', '--user', default="guest", help="rabbitmq user")
    parser.add_argument('-p', '--password', default="guest", help="rabbitmq password")
    args = parser.parse_args()
    daemon(args.queue, args.asg, args.interval, args.namespace, args.user, args.password)
