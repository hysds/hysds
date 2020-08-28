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
import getpass
import requests
import json
import socket
from requests.auth import HTTPBasicAuth
from requests import HTTPError

import logging
import argparse
from datetime import datetime
from smtplib import SMTP

# Import the email modules we'll need
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.header import Header
from email.utils import parseaddr, formataddr, COMMASPACE, formatdate

from hysds_commons.request_utils import get_requests_json_response
from hysds_commons.log_utils import logger
from hysds.celery import app
import hysds.es_util


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

HYSDS_QUEUES = (
    app.conf["JOBS_PROCESSED_QUEUE"],
    app.conf["USER_RULES_JOB_QUEUE"],
    app.conf["DATASET_PROCESSED_QUEUE"],
    app.conf["USER_RULES_DATASET_QUEUE"],
    app.conf["USER_RULES_TRIGGER_QUEUE"],
    app.conf["ON_DEMAND_DATASET_QUEUE"],
    app.conf["ON_DEMAND_JOB_QUEUE"],
)


def send_slack_notification(
    channel_url, subject, text, color=None, subject_link=None, attachment_only=False
):
    """Send slack notification."""

    attachment = {
        "title": subject,
        "text": text,
    }
    if color is not None:
        attachment["color"] = color
    if subject_link is not None:
        attachment["subject_link"] = subject_link
    payload = {"attachments": [attachment]}
    if not attachment_only:
        payload["text"] = text
    r = requests.post(
        channel_url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )
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
                "Failed to resolve hostname for full email address. Check system."
            )


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
    header_charset = "ISO-8859-1"

    # We must choose the body charset manually
    for body_charset in "US-ASCII", "ISO-8859-1", "UTF-8":
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
    # recipient_name, recipient_addr = parseaddr(recipient)

    # We must always pass Unicode strings to Header, otherwise it will
    # use RFC 2047 encoding even on plain ASCII strings.
    sender_name = str(Header(str(sender_name), header_charset))
    unicode_parsed_cc_recipients = []
    for recipient_name, recipient_addr in parsed_cc_recipients:
        recipient_name = str(Header(str(recipient_name), header_charset))

        # Make sure email addresses do not contain non-ASCII characters
        recipient_addr = recipient_addr.encode("ascii")
        unicode_parsed_cc_recipients.append((recipient_name, recipient_addr))

    unicode_parsed_bcc_recipients = []
    for recipient_name, recipient_addr in parsed_bcc_recipients:
        recipient_name = str(Header(str(recipient_name), header_charset))

        # Make sure email addresses do not contain non-ASCII characters
        recipient_addr = recipient_addr.encode("ascii")
        unicode_parsed_bcc_recipients.append((recipient_name, recipient_addr))

    # Make sure email addresses do not contain non-ASCII characters
    sender_addr = sender_addr.encode("ascii")
    recipients = cc_recipients + bcc_recipients
    # Create the message ('plain' stands for Content-Type: text/plain)
    msg = MIMEMultipart()
    msg["To"] = ", ".join(recipients)
    """
    msg['CC'] = COMMASPACE.join([formataddr((recipient_name, recipient_addr))
                                 for recipient_name, recipient_addr in unicode_parsed_cc_recipients])
    msg['BCC'] = COMMASPACE.join([formataddr((recipient_name, recipient_addr))
                                  for recipient_name, recipient_addr in unicode_parsed_bcc_recipients])
    """
    msg["Subject"] = Header(str(subject), header_charset)
    msg["FROM"] = "no-reply@jpl.nasa.gov"
    msg.attach(MIMEText(body.encode(body_charset), "plain", body_charset))

    # Add attachments
    if isinstance(attachments, dict):
        for fname in attachments:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachments[fname])
            email.encoders.encode_base64(part)
            part.add_header("Content-Disposition", 'attachment; filename="%s"' % fname)
            msg.attach(part)

    # Send the message via SMTP to docker host
    smtp_url = "smtp://127.0.0.1:25"
    utils.get_logger(__file__).debug("smtp_url : %s" % smtp_url)
    smtp = SMTP("127.0.0.1")
    smtp.sendmail(sender, recipients, msg.as_string())
    smtp.quit()


def do_queue_query(queue_name):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"resource": ["job"]}},
                    {"term": {"job_queue": queue_name}},
                ]
            }
        },
        "sort": [{"job.job_info.time_start": {"order": "desc"}}],
        "_source": [
            "job_id",
            "status",
            "job_queue",
            "payload_id",
            "payload_hash",
            "uuid",
            "job.job_info.time_queued",
            "job.job_info.time_start",
            "job.job_info.time_limit",
            "job.job_info.time_end",
            "error",
            "traceback",
        ],
        "size": 1,
    }
    logging.info("query: %s" % json.dumps(query, indent=2, sort_keys=True))

    # query
    ES = es_util.get_mozart_es()
    result = ES.search(index="job_status-current", body=json.dumps(query))
    return result


def send_email_notification(emails, job_type, text, attachments=[]):
    """Send email notification."""

    cc_recipients = [i.strip() for i in emails.split(",")]
    bcc_recipients = []
    subject = "[job_periodicity_watchdog] %s" % job_type
    body = text
    attachments = None
    send_email(
        "%s@%s" % (getpass.getuser(), get_hostname()),
        cc_recipients,
        bcc_recipients,
        subject,
        body,
        attachments=attachments,
    )


def get_all_queues(rabbitmq_admin_url, user=None, password=None):
    """
    List the queues available for job-running (Note: does not return celery internal queues)
    :param rabbitmq_admin_url: RabbitMQ admin URL
    :param user:
    :param password:
    :return: list of queues
    """
    print("get_all_queues : {}/ {}".format(user, password))

    try:
        if user and password:
            data = get_requests_json_response(
                os.path.join(rabbitmq_admin_url, "api/queues"),
                auth=HTTPBasicAuth(user, password),
                verify=False,
            )
        else:
            data = get_requests_json_response(
                os.path.join(rabbitmq_admin_url, "api/queues"), verify=False
            )
    except HTTPError as e:
        if e.response.status_code == 401:
            logger.error(
                "Failed to authenticate to {}. Ensure credentials are set in .netrc.".format(
                    rabbitmq_admin_url
                )
            )
        raise

    for obj in data:
        if not obj["name"].startswith("celery") and obj["name"] not in HYSDS_QUEUES:
            if obj["name"] == "Recommended Queues":
                continue

            if obj["name"] == "factotum-job_worker-scihub_throttled":
                print(obj["name"])
                print(obj)
                print(json.dumps(obj, indent=2, sort_keys=True))
                break

    return [
        obj
        for obj in data
        if not obj["name"].startswith("celery")
        and obj["name"] not in HYSDS_QUEUES
        and obj["name"] != "Recommended Queues"
        and obj["messages_ready"] > 0
    ]


def check_queue_execution(
    rabbitmq_url, periodicity=0, slack_url=None, email=None, user=None, password=None
):
    """Check that job type ran successfully within the expected periodicity."""

    logging.info("rabbitmq url: %s" % rabbitmq_url)
    logging.info("periodicity: %s" % periodicity)

    queue_list = get_all_queues(rabbitmq_url, user, password)
    if len(queue_list) == 0:
        print("No non-empty queue found")
        return

    is_alert = False
    error = ""
    for obj in queue_list:
        queue_name = obj["name"]
        if queue_name == "Recommended Queues":
            continue
        messages_ready = obj["messages_ready"]
        total_messages = obj["messages"]
        messages_unacked = obj["messages_unacknowledged"]
        running = total_messages - messages_ready

        if messages_ready > 0 and messages_unacked == 0:
            is_alert = True
            error += "\nQueue Name : %s" % queue_name
            error += "\nError : No job running though jobs are waiting in the queue!!"
            error += "\nTotal jobs : %s" % total_messages
            error += "\nJobs WAITING in the queue : %s" % messages_ready
            error += "\nJobs running : %s" % messages_unacked
        else:
            print("processing job status for queue : %s" % queue_name)
            result = do_queue_query(queue_name)
            count = result["hits"]["total"]
            if count == 0:
                is_alert = True
                error += "\nQueue Name : %s" % queue_name
                error += "\nError : No job found for Queue :  %s!!\n." % queue_name
            else:
                latest_job = result["hits"]["hits"][0]["_source"]
                logging.info(
                    "latest_job: %s" % json.dumps(latest_job, indent=2, sort_keys=True)
                )
                print("job status : %s" % latest_job["status"])
                start_dt = datetime.strptime(
                    latest_job["job"]["job_info"]["time_start"], "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                now = datetime.utcnow()
                delta = (now - start_dt).total_seconds()
                if "time_limit" in latest_job["job"]["job_info"]:
                    logging.info("Using job time limit as periodicity")
                    periodicity = latest_job["job"]["job_info"]["time_limit"]
                logging.info("periodicity: %s" % periodicity)
                logging.info("Successful Job delta: %s" % delta)
                if delta > periodicity:
                    is_alert = True
                    error += "\nQueue Name : %s" % queue_name
                    error += "\nError: Possible Job hanging in the queue"
                    error += "\nTotal jobs : %s" % total_messages
                    error += "\nJobs WAITING in the queue : %s" % messages_ready
                    error += "\nJobs running : %s" % messages_unacked
                    error += (
                        '\nThe last job running in Queue "%s" for %.2f-hours.\n'
                        % (queue_name, delta / 3600.0)
                    )
                    error += "job_id: %s\n" % latest_job["job_id"]
                    error += (
                        "time_queued: %s\n"
                        % latest_job["job"]["job_info"]["time_queued"]
                    )
                    error += (
                        "time_started: %s\n"
                        % latest_job["job"]["job_info"]["time_start"]
                    )
                    color = "#f23e26"
                else:
                    continue

    if not is_alert:
        return

    # Send the queue status now.
    subject = "\n\nQueue Status Alert\n\n"

    # send notification via slack
    if slack_url:
        send_slack_notification(
            slack_url, subject, error, "#f23e26", attachment_only=True
        )

    # send notification via email
    if email:
        send_email_notification(email, "Queue Status", subject + error)


if __name__ == "__main__":
    host = app.conf.get("JOBS_ES_URL", "http://localhost:9200")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rabbitmq_admin_url", help="RabbitMQ Admin Url")
    parser.add_argument(
        "periodicity", type=int, help="successful job execution periodicity in seconds"
    )
    parser.add_argument("-u", "--url", default=host, help="ElasticSearch URL")
    parser.add_argument(
        "-n", "--user", default=None, help="User to access the rabbit_mq"
    )
    parser.add_argument(
        "-p", "--password", default=None, help="password to access the rabbit_mq"
    )
    parser.add_argument(
        "-s", "--slack_url", default=None, help="Slack URL for notification"
    )
    parser.add_argument(
        "-e",
        "--email",
        default=None,
        help="email addresses (comma-separated) for notification",
    )

    args = parser.parse_args()
    check_queue_execution(
        args.rabbitmq_admin_url,
        args.periodicity,
        args.slack_url,
        args.email,
        args.user,
        args.password,
    )
