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
import requests
import json
import types
import base64
import socket
from requests.auth import HTTPBasicAuth
from requests import HTTPError
from hysds_commons.request_utils import get_requests_json_response
from hysds_commons.log_utils import logger
import traceback
import logging
import argparse
from datetime import datetime
import smtplib
from smtplib import SMTP
# Import the email modules we'll need
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.header import Header
from email.utils import parseaddr, formataddr, COMMASPACE, formatdate
from hysds.celery import app
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


def send_slack_notification(channel_url, subject, text, color=None, subject_link=None,
                            attachment_only=False):
    """Send slack notification."""

    attachment = {
        "title": subject,
        "text": text,
    }
    if color is not None:
        attachment['color'] = color
    if subject_link is not None:
        attachment['subject_link'] = subject_link
    payload = {
        'attachments': [attachment]
    }
    if not attachment_only:
        payload['text'] = text
    r = requests.post(channel_url, data=json.dumps(payload),
                      headers={'Content-Type': 'application/json'})
    r.raise_for_status()


def get_hostname():
    """Get hostname."""

    # get hostname
    try:
        return socket.getfqdn()
    except:
        # get IP
        try:
            return socket.gethostbyname(socket.gethostname())
        except:
            raise RuntimeError(
                "Failed to resolve hostname for full email address. Check system.")


def send_email(sender, cc_recipients, bcc_recipients, subject, body, attachments=None):
    """Send an email.

    All arguments should be Unicode strings (plain ASCII works as well).

    Only the real name part of sender and recipient addresses may contain
    non-ASCII characters.

    The email will be properly MIME encoded and delivered though SMTP to
    172.17.0.1.  This is easy to change if you want something different.

    The charset of the email will be the first one out of US-ASCII, ISO-8859-1
    and UTF-8 that can represent all the characters occurring in the email.
    """

    # combined recipients
    recipients = cc_recipients + bcc_recipients

    # Header class is smart enough to try US-ASCII, then the charset we
    # provide, then fall back to UTF-8.
    header_charset = 'ISO-8859-1'

    # We must choose the body charset manually
    for body_charset in 'US-ASCII', 'ISO-8859-1', 'UTF-8':
        try:
            body.encode(body_charset)
        except UnicodeError:
            pass
        else:
            break

    # Split real name (which is optional) and email address parts
    sender_name, sender_addr = parseaddr(sender)
    parsed_cc_recipients = [parseaddr(rec) for rec in cc_recipients]
    parsed_bcc_recipients = [parseaddr(rec) for rec in bcc_recipients]
    #recipient_name, recipient_addr = parseaddr(recipient)

    # We must always pass Unicode strings to Header, otherwise it will
    # use RFC 2047 encoding even on plain ASCII strings.
    sender_name = str(Header(str(sender_name), header_charset))
    unicode_parsed_cc_recipients = []
    for recipient_name, recipient_addr in parsed_cc_recipients:
        recipient_name = str(Header(str(recipient_name), header_charset))
        # Make sure email addresses do not contain non-ASCII characters
        recipient_addr = recipient_addr.encode('ascii')
        unicode_parsed_cc_recipients.append((recipient_name, recipient_addr))
    unicode_parsed_bcc_recipients = []
    for recipient_name, recipient_addr in parsed_bcc_recipients:
        recipient_name = str(Header(str(recipient_name), header_charset))
        # Make sure email addresses do not contain non-ASCII characters
        recipient_addr = recipient_addr.encode('ascii')
        unicode_parsed_bcc_recipients.append((recipient_name, recipient_addr))

    # Make sure email addresses do not contain non-ASCII characters
    sender_addr = sender_addr.encode('ascii')
    recipients = cc_recipients + bcc_recipients
    # Create the message ('plain' stands for Content-Type: text/plain)
    msg = MIMEMultipart()
    msg['To'] = ', '.join(recipients)
    '''
    msg['CC'] = COMMASPACE.join([formataddr((recipient_name, recipient_addr))
                                 for recipient_name, recipient_addr in unicode_parsed_cc_recipients])
    msg['BCC'] = COMMASPACE.join([formataddr((recipient_name, recipient_addr))
                                  for recipient_name, recipient_addr in unicode_parsed_bcc_recipients])
    '''
    msg['Subject'] = Header(str(subject), header_charset)
    msg['FROM'] = "no-reply@jpl.nasa.gov"
    msg.attach(MIMEText(body.encode(body_charset), 'plain', body_charset))

    # Add attachments
    if isinstance(attachments, dict):
        for fname in attachments:
            part = MIMEBase('application', "octet-stream")
            part.set_payload(attachments[fname])
            email.encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment; filename="%s"' % fname)
            msg.attach(part)

    # Send the message via SMTP to docker host
    smtp_url = "smtp://127.0.0.1:25"
    smtp = SMTP("127.0.0.1")
    smtp.sendmail(sender, recipients, msg.as_string())
    smtp.quit()


def do_queue_query(url, queue_name):

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "resource": ["job"]
                        }
                    },
                    {
                        "term": {
                            "job_queue": queue_name
                        }
                    }
                ]
            }
        },
        "sort": [{"job.job_info.time_start": {"order": "desc"}}],
        "_source": ["job_id", "status", "job_queue", "payload_id", "payload_hash", "uuid",
                    "job.job_info.time_queued", "job.job_info.time_start", "job.job_info.time_limit",
                    "job.job_info.time_end", "error", "traceback"],
        "size": 1
    }
    logging.info("query: %s" % json.dumps(query, indent=2, sort_keys=True))

    # query
    ES = elasticsearch.Elasticsearch(url)
    result = ES.search(index="job_status-current", body=json.dumps(query))
    return result


def do_queue_query_with_job_status(url, queue_name, job_status="job-started"):

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "resource": ["job"]
                        }
                    },
                    {
                        "term": {
                            "job_queue": queue_name
                        }
                    },
                    {
                      "term": {
                            "status": job_status
                      }
                    }
                ]
            }
        },
        "sort": [{"job.job_info.time_start": {"order": "asc"}}],
        "_source": ["job_id", "status", "job_queue", "payload_id", "payload_hash", "uuid",
                    "job.job_info.time_queued", "job.job_info.time_start", "job.job_info.time_limit",
                    "job.job_info.time_end", "tags", "celery_hostname", "error", "traceback"]
    }
    logging.info("query: %s" % json.dumps(query, indent=2, sort_keys=True))

    # query


    return query_es(url, query, "job_status-current")

    '''
    ES = elasticsearch.Elasticsearch(url)
    scan_result = ES.search(index="job_status-current", body=json.dumps(query))

    #scan_result = r.json()
    #logger.info("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        #r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = ES.search(index="job_status-current", body=scroll_id)
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits


    return result
    '''

def do_job_query(url, job_type, job_status):

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "status": [job_status]
                        }
                    },
                    {
                        "terms": {
                            "resource": ["job"]
                        }
                    },
                    {
                        "terms": {
                            "type": [job_type]
                        }
                    }
                ]
            }
        },
        "sort": [{"job.job_info.time_end": {"order": "desc"}}],
        "_source": ["job_id", "payload_id", "payload_hash", "uuid",
                    "job.job_info.time_queued", "job.job_info.time_start",
                    "job.job_info.time_end", "job.job_info.time_limit"
                    "error", "traceback"],
        "size": 1
    }
    logging.info("query: %s" % json.dumps(query, indent=2, sort_keys=True))

    # query
    ES = elasticsearch.Elasticsearch(url)
    result = ES.search(index="job_status-current", body=json.dumps(query)) 

    return result

def query_es(es_url, query, es_index):
    """Query ES."""

    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    #logger.info("url: {}".format(url))
    r = requests.post(url, data=json.dumps(query))
    #r.raise_for_status()
    scan_result = r.json()

    #print("scan_result : {}".format(scan_result))

    #logger.info("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    #count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits


def send_email_notification(emails, job_type, text, attachments=[]):
    """Send email notification."""

    cc_recipients = [i.strip() for i in emails.split(',')]
    bcc_recipients = []
    subject = "[job_periodicity_watchdog] %s" % job_type
    body = text
    attachments = None
    send_email("%s@%s" % (getpass.getuser(), get_hostname()), cc_recipients,
               bcc_recipients, subject, body, attachments=attachments)

def getTask_info(task_id):
    task_res = None
    try:
        task_query = {
            "query": {
                "term": {
                    "_id": task_id
                }
            },
            "_source": ["status"]
        }
        r = requests.post('%s/task_status-current/task/_search' % url,
                              data=json.dumps(task_query))
        if r.status_code != 200:
            logging.error("Failed to query ES. Got status code %d:\n%s" %
                              (r.status_code, json.dumps(task_query, indent=2)))
        else:
            task_res = r.json()
            logging.info("task_res: {}".format(json.dumps(task_res)))
    except Exception as err:
        logger.info("ERROR : getTask_info for job_id : {} : {}".format(task_id, str(err)))
    return task_res

def getworker_info(celery_hostname):
    worker_res = None
    try:
        worker_query = {
            "query": {
                "term": {
                    "_id": celery_hostname
                }
            },
            "_source": ["status", "tags"]
        }
        r = requests.post('%s/worker_status-current/task/_search' % url,
                              data=json.dumps(worker_query))
        if r.status_code != 200:
            logging.error("Failed to query ES. Got status code %d:\n%s" %
                              (r.status_code, json.dumps(worker_query, indent=2)))
        else:
            worker_res = r.json()
            logging.info("worker_res: {}".format(json.dumps(worker_res)))
    except Exception as err:
        logger.info("ERROR : getworker_info for job_id : {} : {}".format(worker_id, str(err)))
    return worker_res

def update_job_status(id, new_status, tags):
    update_status = 0
    try:
        new_doc = {
            "doc": {"status": new_status,
            "tags": tags },
            "doc_as_upsert": True
        }
        r = requests.post('%s/job_status-current/job/%s/_update' % (url, id),
                                  data=json.dumps(new_doc))
        result = r.json()
        if r.status_code != 200:
            logging.error("Failed to update status for %s. Got status code %d:\n%s" %
                          (id, r.status_code, json.dumps(result, indent=2)))
        else:
            update_status = 1
        logging.info("Set job {} to {} and tagged as timedout.".format(id, new_status))
    except Exception as err:
        logging.error("Failed to update status for %s. Error : %s" %(id, str(err)))
        update_status = 0

    return update_status


def get_all_queues(rabbitmq_admin_url, user=None, password=None):
    '''
    List the queues available for job-running
    Note: does not return celery internal queues
    @param rabbitmq_admin_url: RabbitMQ admin URL
    @return: list of queues
    '''
    print("get_all_queues : {}/ {}".format(user,password))

    try:
        if user and password:
            data = get_requests_json_response(os.path.join(rabbitmq_admin_url, "api/queues"), auth=HTTPBasicAuth(user, password), verify=False)
        else:
            data = get_requests_json_response(os.path.join(rabbitmq_admin_url, "api/queues"), verify=False)
            #print(data)
    except HTTPError as e:
        if e.response.status_code == 401:
            logger.error("Failed to authenticate to {}. Ensure credentials are set in .netrc.".format(rabbitmq_admin_url))
        raise
    #'''
    for obj in data:
        if not obj["name"].startswith("celery") and obj["name"] not in HYSDS_QUEUES:
            if obj["name"] =='Recommended Queues':
                continue
	    
            if obj["name"]=="factotum-job_worker-scihub_throttled":
                print(obj["name"])
                print(obj)
                print(json.dumps(obj, indent=2, sort_keys=True))
                break
    #'''	    
    return [ obj for obj in data if not obj["name"].startswith("celery") and obj["name"] not in HYSDS_QUEUES and obj["name"] !='Recommended Queues' and obj["messages_ready"]>0]
