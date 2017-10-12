#!/usr/bin/env python
"""
Spot termination detector daemon that checks if the instance it's running on is
marked for termination. If so, it sends a custom HySDS event log.
"""
import os, sys, time, re, json, socket, requests, logging, argparse, traceback
from subprocess import call


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def log_event(url, event_type, event_status, event, tags):
    """Log custom event."""

    params = {
        'type': event_type,
        'status': event_status,
        'event': event,
        'tags': tags,
        'hostname': socket.getfqdn(),
    }
    headers = { 'Content-type': 'application/json' }
    r = requests.post("%s/event/add" % url, data=json.dumps(params), 
                      verify=False, headers=headers)
    r.raise_for_status()
    resp = r.json()
    return resp

    
def check_spot_termination():
    """Check if instance is marked for spot termination."""

    r = requests.get('http://169.254.169.254/latest/meta-data/spot/termination-time')
    #logging.info("got status code: %d" % r.status_code)
    if r.status_code == 200: return str(r.content)
    else: return None


def graceful_shutdown(url, term_time):
    """Gracefully shutdown supervisord, detach from AutoScale group or spot fleet,
       and shutdown."""

    # stop docker containers
    try:
        logging.info("Stopping all docker containers.")
        os.system("/usr/bin/docker stop --time=30 $(/usr/bin/docker ps -aq)")
    except: pass

    # shutdown supervisord
    try:
        logging.info("Stopping supervisord.")
        call(["/usr/bin/sudo", "/usr/bin/systemctl", "stop", "supervisord"])
    except: pass

    # log marked_for_termination
    try:
        print(log_event(url, 'aws_spot', 'marked_for_termination',
                        { 'terminate_time': term_time }, []))
    except: pass

    # die
    sys.exit(0)


def daemon(url, check_interval):
    """Check for spot termination notice."""

    logging.info("configuration:")
    logging.info("mozart_rest_url=%s" % url)
    logging.info("check=%d" % check_interval)

    while True:
        terminate_time = check_spot_termination()
        if terminate_time is not None:
            graceful_shutdown(url, terminate_time)
        time.sleep(check_interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mozart_rest_url', help="Mozart REST API," + \
                        " e.g. https://192.168.0.1/mozart/api/v0.1")
    parser.add_argument('-c', '--check', type=int, default=60,
                        help="check for spot termination notice every N seconds")
    args = parser.parse_args()
    daemon(args.mozart_rest_url, args.check)
