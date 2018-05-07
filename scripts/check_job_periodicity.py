#!/usr/bin/env python
"""
Watchdog completed job type execution with an expected periodicity.
"""

import os, sys, getpass, requests, json, types, base64, socket
import traceback, logging, argparse
from datetime import datetime
from smtplib import SMTP
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email.Header import Header
from email.Utils import parseaddr, formataddr, COMMASPACE, formatdate
from email import Encoders

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


def send_slack_notification(channel_url, subject, text, color=None, subject_link=None, 
                            attachment_only=False):
    """Send slack notification."""

    attachment = {
        "title": subject,
        "text": text, 
    }
    if color is not None: attachment['color'] = color
    if subject_link is not None: attachment['subject_link'] = subject_link
    payload = {
        'attachments': [attachment]
    }
    if not attachment_only: payload['text'] = text
    r = requests.post(channel_url, data=json.dumps(payload),
                      headers={ 'Content-Type': 'application/json' })
    r.raise_for_status()


def get_hostname():
    """Get hostname."""

    # get hostname
    try: return socket.getfqdn()
    except:
        # get IP
        try: return socket.gethostbyname(socket.gethostname())
        except:
            raise RuntimeError("Failed to resolve hostname for full email address. Check system.")


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
    sender_name = str(Header(unicode(sender_name), header_charset))
    unicode_parsed_cc_recipients = []
    for recipient_name, recipient_addr in parsed_cc_recipients:
        recipient_name = str(Header(unicode(recipient_name), header_charset))
        # Make sure email addresses do not contain non-ASCII characters
        recipient_addr = recipient_addr.encode('ascii')
        unicode_parsed_cc_recipients.append((recipient_name, recipient_addr))
    unicode_parsed_bcc_recipients = []
    for recipient_name, recipient_addr in parsed_bcc_recipients:
        recipient_name = str(Header(unicode(recipient_name), header_charset))
        # Make sure email addresses do not contain non-ASCII characters
        recipient_addr = recipient_addr.encode('ascii')
        unicode_parsed_bcc_recipients.append((recipient_name, recipient_addr))

    # Make sure email addresses do not contain non-ASCII characters
    sender_addr = sender_addr.encode('ascii')

    # Create the message ('plain' stands for Content-Type: text/plain)
    msg = MIMEMultipart()
    msg['CC'] = COMMASPACE.join([formataddr((recipient_name, recipient_addr))
                                 for recipient_name, recipient_addr in unicode_parsed_cc_recipients])
    msg['BCC'] = COMMASPACE.join([formataddr((recipient_name, recipient_addr))
                                  for recipient_name, recipient_addr in unicode_parsed_bcc_recipients])
    msg['Subject'] = Header(unicode(subject), header_charset)
    msg['FROM'] = "no-reply@jpl.nasa.gov"
    msg.attach(MIMEText(body.encode(body_charset), 'plain', body_charset))
    
    # Add attachments
    if isinstance(attachments, types.DictType):
        for fname in attachments:
            part = MIMEBase('application', "octet-stream")
            part.set_payload(attachments[fname])
            Encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % fname)
            msg.attach(part)

    # Send the message via SMTP to docker host
    smtp_url = "smtp://127.0.0.1:25"
    logger.info("smtp_url : %s" % smtp_url)
    smtp = SMTP("127.0.0.1")
    smtp.sendmail(sender, recipients, msg.as_string())
    smtp.quit()

def do_job_query(url, job_type, job_status):

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "status": [ job_status ]
                        }
                    },
                    {
                        "terms": {
                            "resource": [ "job" ]
                        }
                    },
                    {
                        "terms": {
                            "type": [ job_type ]
                        }
                    }
                ]
            }
        },
        "sort": [ {"job.job_info.time_end": { "order":"desc" } } ],
        "_source": [ "job_id", "payload_id", "payload_hash", "uuid",
                     "job.job_info.time_queued", "job.job_info.time_start",
                     "job.job_info.time_end", "error", "traceback" ],
        "size": 1
    }
    logging.info("query: %s" % json.dumps(query, indent=2, sort_keys=True))

    # query
    url_tmpl = "{}/job_status-current/_search"
    r = requests.post(url_tmpl.format(url), data=json.dumps(query))
    if r.status_code != 200:
        logging.error("Failed to query ES. Got status code %d:\n%s" %
                      (r.status_code, json.dumps(query, indent=2)))
    r.raise_for_status()
    result = r.json()

    return result

def send_email_notification(emails, job_type, text, attachments=[]):
    """Send email notification."""

    cc_recipients = [i.strip() for i in emails.split(',')]
    bcc_recipients = []
    subject = "[job_periodicity_watchdog] %s" % job_type
    body = text
    attachments = None
    send_email("%s@%s" % (getpass.getuser(), get_hostname()), cc_recipients,
               bcc_recipients, subject, body, attachments=attachments)


def check_failed_job(url, job_type, periodicity, error, slack_url=None, email=None):
    """Check that if any job of  job type Failed within the expected periodicity."""

    logging.info("url: %s" % url)
    logging.info("job_type: %s" % job_type)
    logging.info("periodicity: %s" % periodicity)

    # build query
    result = do_job_query(url, job_type, "job-failed")
    count = result['hits']['total']
    if count == 0: 
        error+="\n\nNo Failed jobs found for job type %s." % job_type
    else:
        latest_job = result['hits']['hits'][0]['_source']
        logging.info("latest_job: %s" % json.dumps(latest_job, indent=2, sort_keys=True))
        end_dt = datetime.strptime(latest_job['job']['job_info']['time_end'], "%Y-%m-%dT%H:%M:%S.%fZ")
        now = datetime.utcnow()
        delta = (now-end_dt).total_seconds()
        logging.info("Failed Job delta: %s" % delta)
        error +="\nThe last failed job of type %s was %.2f-hours ago:\n" %(job_type, delta/3600.)
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
        send_slack_notification(slack_url, subject, error, "#f23e26", attachment_only=True)

    # send notification via email
    if email:
        send_email_notification(email, job_type, subject + error)
    

def check_job_execution(url, job_type, periodicity,  slack_url=None, email=None):
    """Check that job type ran successfully within the expected periodicity."""

    logging.info("url: %s" % url)
    logging.info("job_type: %s" % job_type)
    logging.info("periodicity: %s" % periodicity)

    # build query
    result = do_job_query(url, job_type, "job-completed")
    count = result['hits']['total']
    if count == 0: 
        error = "No Successfully Completed jobs found for job type %s!!\n." % job_type
    else:
        latest_job = result['hits']['hits'][0]['_source']
        logging.info("latest_job: %s" % json.dumps(latest_job, indent=2, sort_keys=True))
        end_dt = datetime.strptime(latest_job['job']['job_info']['time_end'], "%Y-%m-%dT%H:%M:%S.%fZ")
        now = datetime.utcnow()
        delta = (now-end_dt).total_seconds()
        logging.info("Successful Job delta: %s" % delta)
        if delta > periodicity:
            error  = '\nThere has not been a successfully completed job type "%s" for more than %.2f-hours.\n' % (job_type, delta/3600.) 
            error += 'The last successfully completed job:\n'
            error += "job_id: %s\n" % latest_job['job_id']
            #error += "payload_id: %s\n" % latest_job['payload_id']
            #error += "time_queued: %s\n" % latest_job['job']['job_info']['time_queued']
            #error += "time_start: %s\n" % latest_job['job']['job_info']['time_start']
            error += "time_end: %s\n" % latest_job['job']['job_info']['time_end']
            color = "#f23e26"
        else: return

    #check for failed job now.
    check_failed_job(url, job_type, periodicity, error, slack_url, email)

if __name__ == "__main__":
    host = app.conf.get('JOBS_ES_URL', 'http://localhost:9200')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('job_type', help="HySDS job type to watchdog")
    parser.add_argument('periodicity', type=int,
                        help="successful job execution periodicity in seconds")
    parser.add_argument('-u', '--url', default=host, help="ElasticSearch URL")
    parser.add_argument('-s', '--slack_url', default=None, help="Slack URL for notification")
    parser.add_argument('-e', '--email', default=None, help="email addresses (comma-separated) for notification")
    args = parser.parse_args()
    check_job_execution(args.url, args.job_type, args.periodicity, args.slack_url, args.email)

