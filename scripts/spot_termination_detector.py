#!/usr/bin/env python
"""
Spot termination detector daemon that checks if the instance it's running on is
marked for termination. If so, it sends a custom HySDS event log.
"""
from future import standard_library

import os
import sys
import time
import re
import json
import socket
import requests
import logging
import argparse
import yaml
from subprocess import call

standard_library.install_aliases()

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# have yaml parse regular expressions
yaml.SafeLoader.add_constructor('tag:yaml.org,2002:python/regexp',
                                lambda l, n: re.compile(l.construct_scalar(n)))


def log_event(url, event_type, event_status, event, tags):
    """Log custom event."""

    params = {
        "type": event_type,
        "status": event_status,
        "event": event,
        "tags": tags,
        "hostname": socket.getfqdn(),
    }
    headers = {"Content-type": "application/json"}
    r = requests.post(
        "%s/event/add" % url, data=json.dumps(params), verify=False, headers=headers
    )
    r.raise_for_status()
    resp = r.json()
    return resp


def check_spot_termination():
    """Check if instance is marked for spot termination."""

    r = requests.get("http://169.254.169.254/latest/meta-data/spot/termination-time")
    logging.info("check_spot_termination response status code: %d" % r.status_code)
    if r.status_code == 200:
        return r.content.decode()
    else:
        return None


def graceful_shutdown(url, term_time):
    """Gracefully shutdown supervisord, detach from AutoScale group or spot fleet,
    and shutdown."""

    # log marked_for_termination
    try:
        logging.info("Begin logging a 'marked_for_termination' event.")
        print(
                log_event(
                    url,
                    "aws_spot",
                    "marked_for_termination",
                    {"terminate_time": term_time},
                    [],
                )
        )
        logging.info(f"Finished logging a 'marked_for_termination' event. Termination time: {term_time}")
    except Exception as e:
        logging.warning(f"Exception occurred while logging the 'marked_for_termination' event: {str(e)}")
        pass

    # stop docker containers
    try:
        logging.info("Stopping all docker containers.")
        os.system("/usr/bin/docker stop --time=30 $(/usr/bin/docker ps -aq)")
    except Exception as e:
        logging.warning(f"Exception occurred while stopping docker containers: {str(e)}")
        pass

    # shutdown supervisord
    try:
        logging.info("Stopping supervisord.")
        call(["/usr/bin/sudo", "/usr/bin/systemctl", "stop", "supervisord"])
    except Exception as e:
        logging.warning(f"Exception occurred while stopping supervisord: {str(e)}")
        pass

    # die
    sys.exit(0)


def spot_termination_detector(url, check_interval):
    """Check for spot termination notice."""

    logging.info("spot_termination_detector configuration:")
    logging.info("mozart_rest_url=%s" % url)
    logging.info("check=%d" % check_interval)

    while True:
        terminate_time = check_spot_termination()
        if terminate_time is not None:
            graceful_shutdown(url, terminate_time)
        time.sleep(check_interval)


if __name__ == "__main__":
    mozart_rest_url = None
    check = None

    # Parse the configuration file if there was one
    conf_parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    conf_parser.add_argument(
        "-f",
        "--file",
        type=str,
        default=None,
        help="Configuration file. Anything specified on the command-line takes precedence."
    )
    args, remaining_argv = conf_parser.parse_known_args()
    config_args = dict()
    if args.file:
        with open(args.file) as f:
            config_params = yaml.safe_load(f)
            mozart_rest_url = config_params.get("mozart_rest_url", None)
            check = config_params.get("check", None)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mozart_rest_url",
        nargs="?",
        default=None,
        help="Mozart REST API," + " e.g. https://192.168.0.1/mozart/api/v0.1"
    )
    parser.add_argument(
        "-c",
        "--check",
        type=int,
        default=None,
        help="check for spot termination notice every N seconds",
    )
    args = parser.parse_args(remaining_argv)
    if args.check:
        check = args.check

    # Set default value for check if not defined in the config file or command line
    if check is None:
        check = 60

    spot_termination_detector(mozart_rest_url, check)
