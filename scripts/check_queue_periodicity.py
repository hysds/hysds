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
import elasticsearch
import job_util

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
    
    queue_list = job_util.get_all_queues(rabbitmq_url, user, password)
    #print(queue_list)
    if len(queue_list)==0:
        print("No non-empty queue found")
        return

    is_alert=False
    error=""
    for obj in queue_list:
        queue_name=obj["name"]
        if queue_name=='Recommended Queues':
            continue
        messages_ready=obj["messages_ready"]
        total_messages=obj["messages"]
        messages_unacked=obj["messages_unacknowledged"]
        running = total_messages - messages_ready
	
        if messages_ready>0 and messages_unacked==0:
            is_alert=True
            error +='\nQueue Name : %s' %queue_name
            error += "\nError : No job running though jobs are waiting in the queue!!"
            error +='\nTotal jobs : %s' %total_messages
            error += '\nJobs WAITING in the queue : %s' %messages_ready
            error +='\nJobs running : %s' %messages_unacked
        else:
            print("processing job status for queue : %s" %queue_name)
            result = job_util.do_queue_query(url, queue_name)
            count = result['hits']['total']
            if count == 0: 
                is_alert=True
                error +='\nQueue Name : %s' %queue_name
                error += "\nError : No job found for Queue :  %s!!\n." % queue_name
            else:
                latest_job = result['hits']['hits'][0]['_source']
                logging.info("latest_job: %s" % json.dumps(latest_job, indent=2, sort_keys=True))
                print("job status : %s" %latest_job['status'])
                start_dt = datetime.strptime(latest_job['job']['job_info']['time_start'], "%Y-%m-%dT%H:%M:%S.%fZ")
                now = datetime.utcnow()
                delta = (now-start_dt).total_seconds()
                if 'time_limit' in latest_job['job']['job_info']:
                    logging.info("Using job time limit as periodicity")
                    periodicity = latest_job['job']['job_info']['time_limit']
                logging.info("periodicity: %s" % periodicity)
                logging.info("Successful Job delta: %s" % delta)
                if delta > periodicity:
                    is_alert=True
                    error +='\nQueue Name : %s' %queue_name
                    error += '\nError: Possible Job hanging in the queue'
                    error +='\nTotal jobs : %s' %total_messages
                    error += '\nJobs WAITING in the queue : %s' %messages_ready
                    error +='\nJobs running : %s' %messages_unacked
                    error  += '\nThe last job running in Queue "%s" for %.2f-hours.\n' % (queue_name, delta/3600.) 
                    error += "job_id: %s\n" % latest_job['job_id']
                    error += "time_queued: %s\n" % latest_job['job']['job_info']['time_queued']
                    error += "time_started: %s\n" % latest_job['job']['job_info']['time_start']
                    color = "#f23e26"
                else: continue

    if not is_alert:
        return
    #Send the queue status now.
    subject = "\n\nQueue Status Alert\n\n" 

    # send notification via slack
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
