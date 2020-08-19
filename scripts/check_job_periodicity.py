#!/usr/bin/env python
"""
Watchdog completed job type execution with an expected periodicity.
"""
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import

from builtins import str
from future import standard_library
standard_library.install_aliases()
import os
import sys
import getpass
import requests
import json
import types
import base64
import socket
import traceback
import logging
import argparse
from datetime import datetime
import elasticsearch
from hysds.celery import app
import job_util


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


def check_failed_job(url, job_type, periodicity, error, slack_url=None, email=None):
    """Check that if any job of  job type Failed within the expected periodicity."""

    logging.info("url: %s" % url)
    logging.info("job_type: %s" % job_type)
    logging.info("periodicity: %s" % periodicity)

    # build query
    result = job_util.do_job_query(url, job_type, "job-failed")
    count = result['hits']['total']
    if count == 0:
        error += "\n\nNo Failed jobs found for job type %s." % job_type
    else:
        latest_job = result['hits']['hits'][0]['_source']
        logging.info("latest_job: %s" % json.dumps(
            latest_job, indent=2, sort_keys=True))
        end_dt = datetime.strptime(
            latest_job['job']['job_info']['time_end'], "%Y-%m-%dT%H:%M:%S.%fZ")
        now = datetime.utcnow()
        delta = (now-end_dt).total_seconds()
        logging.info("Failed Job delta: %s" % delta)
        error += "\nThe last failed job of type %s was %.2f-hours ago:\n" % (
            job_type, delta/3600.)
        error += "\njob_id: %s\n" % latest_job['job_id']
        #error += "payload_id: %s\n" % latest_job['payload_id']
        #error += "time_queued: %s\n" % latest_job['job']['job_info']['time_queued']
        error += "time_start: %s\n" % latest_job['job']['job_info']['time_start']
        error += "time_end: %s\n" % latest_job['job']['job_info']['time_end']
        error += "Error: %s\n" % latest_job['error']
        error += "Tracebak: %s\n" % latest_job['traceback']

    subject = "\nJob Status checking for job type %s:\n\n" % job_type

    # send notification via slack
    if slack_url:
        job_util.send_slack_notification(slack_url, subject, error,
                                "#f23e26", attachment_only=True)

    # send notification via email
    if email:
        job_util.send_email_notification(email, job_type, subject + error)


def check_job_execution(url, job_type, periodicity=0,  slack_url=None, email=None):
    """Check that job type ran successfully within the expected periodicity."""

    logging.info("url: %s" % url)
    logging.info("job_type: %s" % job_type)
    logging.info("Initial periodicity: %s" % periodicity)

    # build query
    result = job_util.do_job_query(url, job_type, "job-completed")
    count = result['hits']['total']
    if count == 0:
        error = "No Successfully Completed jobs found for job type %s!!\n." % job_type
    else:
        latest_job = result['hits']['hits'][0]['_source']
        logging.info("latest_job: %s" % json.dumps(
            latest_job, indent=2, sort_keys=True))
        end_dt = datetime.strptime(
            latest_job['job']['job_info']['time_end'], "%Y-%m-%dT%H:%M:%S.%fZ")
        now = datetime.utcnow()
        delta = (now-end_dt).total_seconds()
        if 'time_limit' in latest_job['job']['job_info']:
            logging.info("Using job time limit as periodicity")
            periodicity = latest_job['job']['job_info']['time_limit']
        logging.info("periodicity: %s" % periodicity)
        logging.info("Successful Job delta: %s" % delta)
        if delta > periodicity:
            error = '\nThere has not been a successfully completed job type "%s" for more than %.2f-hours.\n' % (
                job_type, delta/3600.)
            error += 'The last successfully completed job:\n'
            error += "job_id: %s\n" % latest_job['job_id']
            #error += "payload_id: %s\n" % latest_job['payload_id']
            #error += "time_queued: %s\n" % latest_job['job']['job_info']['time_queued']
            #error += "time_start: %s\n" % latest_job['job']['job_info']['time_start']
            error += "time_end: %s\n" % latest_job['job']['job_info']['time_end']
            color = "#f23e26"
        else:
            return

    # check for failed job now.
    check_failed_job(url, job_type, periodicity, error, slack_url, email)


if __name__ == "__main__":
    host = app.conf.get('JOBS_ES_URL', 'http://localhost:9200')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('job_type', help="HySDS job type to watchdog")
    parser.add_argument('periodicity', type=int,
                        help="successful job execution periodicity in seconds")
    parser.add_argument('-u', '--url', default=host, help="ElasticSearch URL")
    parser.add_argument('-s', '--slack_url', default=None,
                        help="Slack URL for notification")
    parser.add_argument('-e', '--email', default=None,
                        help="email addresses (comma-separated) for notification")
    args = parser.parse_args()
    check_job_execution(args.url, args.job_type,
                        args.periodicity, args.slack_url, args.email)
