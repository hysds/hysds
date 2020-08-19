#!/usr/bin/env python
"""
Watchdog completed job type execution with an expected periodicity.
"""
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import

from builtins import str
from future import standard_library
standard_library.install_aliases()
import os
import sys
import getpass
import json
import types
import base64
import socket
from hysds_commons.log_utils import logger
import traceback
import logging
import argparse
from datetime import datetime
from hysds.celery import app
import job_util

import elasticsearch
import socket

log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

HYSDS_QUEUES = (
    app.conf['JOBS_PROCESSED_QUEUE'],
    app.conf['USER_RULES_JOB_QUEUE'],
    app.conf['DATASET_PROCESSED_QUEUE'],
    app.conf['USER_RULES_DATASET_QUEUE'],
    app.conf['USER_RULES_TRIGGER_QUEUE'],
    app.conf['ON_DEMAND_DATASET_QUEUE'],
    app.conf['ON_DEMAND_JOB_QUEUE'],
)


def check_queue_execution(url, rabbitmq_url, periodicity=0,  slack_url=None, email=None, user=None, password=None):
    """Check that job type ran successfully within the expected periodicity."""

    logging.info("url: %s" % url)
    logging.info("rabbitmq url: %s" % rabbitmq_url)
    logging.info("periodicity: %s" % periodicity)
    hostname = socket.gethostname()    
    IPAddr = socket.gethostbyname(hostname)
   
    queue_list = job_util.get_all_queues(rabbitmq_url, user, password)
    #print("queue_list : {}".format(queue_list))
    if len(queue_list)==0:
        print("No non-empty queue found")
        return

    queue_name_list = []
    print("\nQueue_List : ")
    for obj in queue_list:
        queue_name=obj["name"]
        print(queue_name)
        queue_name_list.append(queue_name)

    is_alert=False
    error=""
    for obj in queue_list:
        queue_name=obj["name"]
        print("Processing queue_name : {}".format(queue_name))
        if queue_name=='Recommended Queues':
            print("in Recommended Queues")
            continue
        messages_ready=obj["messages_ready"]
        total_messages=obj["messages"]
        messages_unacked=obj["messages_unacknowledged"]
        running = total_messages - messages_ready
        queue_alert_jobs = {}

        try:	
            resuly = job_util.do_queue_query_with_job_status(url, queue_name, "job-started")
            print("result : {}".format(result))
            count = (len(result))
            if count == 0: 
                is_alert=False
            else:
                i = 0
                while i<count:
                    latest_job = result[i]['_source']
                    i = i +1 
                    #logging.info("Checking job: %s" % json.dumps(latest_job, indent=2, sort_keys=True))
                    print("\nChecking Job : {}".format(latest_job['job_id']))
                    print("job status : %s" %latest_job['status'])
                    start_dt = datetime.strptime(latest_job['job']['job_info']['time_start'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    now = datetime.utcnow()
                    delta = (now-start_dt).total_seconds()
                    if 'time_limit' in latest_job['job']['job_info']:
                        logging.info("Using job time limit as periodicity")
                        periodicity = latest_job['job']['job_info']['time_limit']
                        logging.info("periodicity: %s" % periodicity)
                        logging.info("Job delta: %s" % delta)
                        if delta > periodicity:
                            logging.info("IS_ALERT is TRUE for : {}".format(latest_job['job_id']))
                            is_alert=True

                            queue_alert_jobs[latest_job['job_id']] = {
                                "time_queued" : latest_job['job']['job_info']['time_queued'],
                                "time_started" : latest_job['job']['job_info']['time_start'],
                                "time_limit" : "%s.2f-hours" %(periodicity/3600.),
                                "delta" : "%s.2f-hours" %(delta/3600.)
                            }
                        else: continue

            if len(queue_alert_jobs)>0:
                error = "Machine Name : {}".format(IPAddr)
                error +='\nQueue Name : %s' %queue_name
                error += "\nError : POSSIBLE JOB HANGING in Queue :  %s!!\n." % queue_name
                error += "\nNUMBER of POSSIBLE JOB HANGING : %s!!\n." % len(queue_alert_jobs)
                error += "\n\nError : The following jobs are running for longer than time limit".format(periodicity)
 
                for job_id in queue_alert_jobs:
                    error += "\n\nJob id : {}\n".format(job_id)
                    error += "time_queued: %s\n" % queue_alert_jobs[job_id]["time_queued"]
                    error += "time_started: %s\n" % queue_alert_jobs[job_id]["time_started"]
                    error += "time_limit: %s\n" % queue_alert_jobs[job_id]["time_limit"]
                    error += "RunTime: %s\n" % queue_alert_jobs[job_id]["delta"]
                print(error)
                #Send the queue status now.
                subject = "\n\nQueue Status Alert: POSSIBLE JOB HANGING in Queue : {}\n\n".format(queue_name)

                # send notification via slack
                if slack_url:
                    job_util.send_slack_notification(slack_url, subject, error, "#f23e26", attachment_only=True)

                # send notification via email
                if email:
                    job_util.send_email_notification(email, "Queue Status", subject + error)
        except Exception as err:
            subject = "Error Retriving Job Status"
            error = "Machine Name : {}".format(IPAddr)
            error +='\nQueue Name : %s' %queue_name
            error += "\nError : {}".format(str(err))

            if slack_url:
                job_util.send_slack_notification(slack_url, subject, error, "#f23e26", attachment_only=True)

                # send notification via email
            if email:
                job_util.send_email_notification(email, "Queue Status", subject + error)


if __name__ == "__main__":
    host = app.conf.get('JOBS_ES_URL', 'http://localhost:9200')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('rabbitmq_admin_url', help="RabbitMQ Admin Url")
    parser.add_argument('periodicity', type=int,
                        help="successful job execution periodicity in seconds")
    parser.add_argument('-u', '--url', default=host, help="ElasticSearch URL")
    parser.add_argument('-n', '--user', default=None, help="User to access the rabbit_mq")
    parser.add_argument('-p', '--password', default=None, help="password to access the rabbit_mq")
    parser.add_argument('-s', '--slack_url', default=None, help="Slack URL for notification")
    parser.add_argument('-e', '--email', default=None, help="email addresses (comma-separated) for notification")
    args = parser.parse_args()
    check_queue_execution(args.url, args.rabbitmq_admin_url, args.periodicity, args.slack_url, args.email, args.user, args.password)
