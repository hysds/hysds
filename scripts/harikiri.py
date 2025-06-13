#!/usr/bin/env python
"""
HySDS inactivity daemon to perform scale down of auto scaling group/spot fleet
request and perform self-termination (harikiri) of the instance. If a keep-alive
signal file exists at <root_work_dir>/.harikiri, then self-termination is bypassed
until it is removed.
"""
import argparse
import json
import logging
import os
import re
import socket
import time
import traceback
from datetime import datetime
from pprint import pformat
from random import randint
from subprocess import call

import backoff
import boto3
import requests
import yaml
from botocore.exceptions import ClientError
from future import standard_library

standard_library.install_aliases()

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


DAY_DIR_RE = re.compile(r"jobs/\d{4}/\d{2}/\d{2}/\d{2}/\d{2}$")

NO_JOBS_TIMER = None

KEEP_ALIVE = False

# have yaml parse regular expressions
yaml.SafeLoader.add_constructor(
    "tag:yaml.org,2002:python/regexp", lambda l, n: re.compile(l.construct_scalar(n))
)


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
        f"{url}/event/add", data=json.dumps(params), verify=False, headers=headers
    )
    r.raise_for_status()
    resp = r.json()
    return resp


def keep_alive(root_work):
    """Check if the keep alive signal exists."""

    return True if os.path.exists(os.path.join(root_work, ".harikiri")) else False


def is_jobless(root_work, inactivity_secs, logger=None):
    """Check if no jobs are running and hasn't run in the past
    amount of time passed in.
    """

    global NO_JOBS_TIMER
    global KEEP_ALIVE

    # check if keep-alive
    logging.info(f"KEEP_ALIVE: {KEEP_ALIVE}")
    if keep_alive(root_work):
        if KEEP_ALIVE is not True:
            KEEP_ALIVE = True
            if logger is not None:
                try:
                    print(log_event(logger, "harikiri", "keep_alive_set", {}, []))
                except Exception as e:
                    logging.warning(
                        f"Exception occurred while logging harikiri keep_alive_set: {str(e)}"
                    )
                    pass
        logging.info("Keep-alive exists.")
        return
    else:
        if KEEP_ALIVE is not False:
            KEEP_ALIVE = False
            if logger is not None:
                try:
                    print(log_event(logger, "harikiri", "keep_alive_unset", {}, []))
                except Exception as e:
                    logging.warning(
                        f"Exception occurred while logging harikiri keep_alive_unset: {str(e)}"
                    )
                    pass
            logging.info("Keep-alive removed.")

    most_recent = None
    for root, dirs, files in os.walk(root_work, followlinks=True):
        match = DAY_DIR_RE.search(root)
        if not match:
            continue
        dirs.sort()
        for d in dirs:
            job_dir = os.path.join(root, d)
            done_file = os.path.join(job_dir, ".done")
            if not os.path.exists(done_file):
                logging.info(f"{job_dir}: no .done file found. Not jobless yet.")
                NO_JOBS_TIMER = None
                return False
            t = os.path.getmtime(done_file)
            done_dt = datetime.utcfromtimestamp(t)
            age = (datetime.utcnow() - done_dt).total_seconds()
            if most_recent is None or age < most_recent:
                most_recent = age
            logging.info(f"{job_dir}: age={age}")
    if most_recent is None:
        if NO_JOBS_TIMER is None:
            NO_JOBS_TIMER = time.time()
        else:
            if (time.time() - NO_JOBS_TIMER) > inactivity_secs:
                return True
        return False
    if most_recent > inactivity_secs:
        return True
    return False


@backoff.on_exception(backoff.expo, ClientError, max_tries=10, max_value=512)
def get_all_groups(c):
    """Get all AutoScaling groups."""

    groups = []
    next_token = None
    while True:
        if next_token is None:
            resp = c.describe_auto_scaling_groups()
        else:
            resp = c.describe_auto_scaling_groups(NextToken=next_token)
        groups.extend(resp["AutoScalingGroups"])
        next_token = resp.get("NextToken", None)
        if next_token is None:
            break
    return groups


@backoff.on_exception(backoff.expo, ClientError, max_tries=10, max_value=512)
def get_all_fleets(c):
    """Get all Spot Fleet requests."""

    fleets = []
    next_token = None
    while True:
        if next_token is None:
            resp = c.describe_spot_fleet_requests()
        else:
            resp = c.describe_spot_fleet_requests(NextToken=next_token)
        fleets.extend(resp["SpotFleetRequestConfigs"])
        next_token = resp.get("NextToken", None)
        if next_token is None:
            break
    return fleets


@backoff.on_exception(backoff.expo, ClientError, max_tries=10, max_value=512)
def get_fleet_instances(c, fleet_name):
    """Get all Spot Fleet instances for a Spot Fleet request."""

    instances = []
    next_token = None
    while True:
        if next_token is None:
            resp = c.describe_spot_fleet_instances(SpotFleetRequestId=fleet_name)
        else:
            resp = c.describe_spot_fleet_instances(
                SpotFleetRequestId=fleet_name, NextToken=next_token
            )
        instances.extend(resp["ActiveInstances"])
        next_token = resp.get("NextToken", None)
        if next_token is None:
            break
    return instances


@backoff.on_exception(backoff.expo, ClientError, max_value=512)
def detach_instance(c, as_group, id):
    """Detach instance from AutoScaling group."""
    c.detach_instances(
        InstanceIds=[id],
        AutoScalingGroupName=as_group,
        ShouldDecrementDesiredCapacity=True,
    )


@backoff.on_exception(backoff.expo, ClientError, max_value=512)
def decrement_fleet(c, spot_fleet):
    """Decrement target capacity of spot fleet."""

    resp = c.describe_spot_fleet_requests(SpotFleetRequestIds=[spot_fleet])
    tg = resp["SpotFleetRequestConfigs"][0]["SpotFleetRequestConfig"]["TargetCapacity"]
    logging.info(f"TargetCapacity: {tg}")
    tg -= 1
    if tg > 0:
        c.modify_spot_fleet_request(
            ExcessCapacityTerminationPolicy="NoTermination",
            SpotFleetRequestId=spot_fleet,
            TargetCapacity=tg,
        )
    else:
        c.cancel_spot_fleet_requests(
            SpotFleetRequestIds=[spot_fleet], TerminateInstances=False
        )
    logging.info(f"response: {pformat(resp)}")


def seppuku(logger=None):
    """Shutdown supervisord and the instance if it detects that it is
    currently part of an autoscale group."""

    logging.info("Initiating seppuku.")

    # instances may be part of autoscaling group or spot fleet
    as_group = None
    spot_fleet = None

    # check if instance part of an autoscale group
    id = requests.get(
        "http://169.254.169.254/latest/meta-data/instance-id"
    ).content.decode()
    logging.info(f"Our instance id: {id}")
    c = boto3.client("autoscaling")
    for group in get_all_groups(c):
        group_name = str(group["AutoScalingGroupName"])
        logging.info(f"Checking group: {group_name}")
        for i in group["Instances"]:
            asg_inst_id = str(i["InstanceId"])
            logging.info(f"Checking group instance: {asg_inst_id}")
            if id == asg_inst_id:
                as_group = group_name
                logging.info("Matched!")
                break
    if as_group is None:
        logging.info(f"This instance {id} is not part of any autoscale group.")

        # check if instance is part of a spot fleet
        c = boto3.client("ec2")
        for fleet in get_all_fleets(c):
            fleet_name = str(fleet["SpotFleetRequestId"])
            logging.info(f"Checking fleet: {fleet_name}")
            for i in get_fleet_instances(c, fleet_name):
                sf_inst_id = str(i["InstanceId"])
                logging.info(f"Checking fleet instance: {sf_inst_id}")
                if id == sf_inst_id:
                    spot_fleet = fleet_name
                    logging.info("Matched!")
                    break
        if spot_fleet is None:
            logging.info(f"This instance {id} is not part of any spot fleet.")

    # gracefully shutdown
    while True:
        try:
            graceful_shutdown(as_group, spot_fleet, id, logger)
        except Exception as e:
            logging.error(
                f"Got exception in graceful_shutdown(): {str(e)}\n{traceback.format_exc()}"
            )
        time.sleep(randint(0, 600))


def graceful_shutdown(as_group, spot_fleet, id, logger=None):
    """Gracefully shutdown supervisord, detach from AutoScale group or spot fleet,
    and shutdown."""

    # stop docker containers
    try:
        logging.info("Stopping all docker containers.")
        os.system("/usr/bin/docker stop --time=30 $(/usr/bin/docker ps -aq)")
    except Exception as e:
        logging.warning(
            f"Exception occurred while stopping docker containers: {str(e)}"
        )
        pass

    # shutdown supervisord
    try:
        logging.info("Stopping supervisord.")
        call(["/usr/bin/sudo", "/usr/bin/systemctl", "stop", "supervisord"])
    except Exception as e:
        logging.warning(f"Exception occurred while stopping supervisord: {str(e)}")
        pass

    # let supervisord shutdown its processes
    time.sleep(60)

    # detach and die
    logging.info("Committing seppuku.")

    # detach if part of a spot fleet or autoscaling group
    try:
        if as_group is not None:
            c = boto3.client("autoscaling")
            detach_instance(c, as_group, id)
        if spot_fleet is not None:
            c = boto3.client("ec2")
            decrement_fleet(c, spot_fleet)
    except Exception as e:
        logging.error(
            f"Got exception in graceful_shutdown(): {str(e)}\n{traceback.format_exc()}"
        )

    # log seppuku
    if logger is not None:
        try:
            print(log_event(logger, "harikiri", "shutdown", {}, []))
        except Exception as e:
            logging.warning(
                f"Exception occurred while logging harikiri shutdown: {str(e)}"
            )
            pass
    time.sleep(60)

    call(["/usr/bin/sudo", "/sbin/shutdown", "-h", "now"])


def harikiri(root_work, inactivity_secs, check_interval, logger=None):
    """If no jobs are running and the last job finished more than the
    threshold, shutdown supervisord gracefully then shutdown the
    instance.
    """

    logging.info("harikiri configuration:")
    logging.info(f"root_work_dir={root_work}")
    logging.info(f"inactivity={inactivity_secs}")
    logging.info(f"check={check_interval}")
    logging.info(f"logger={logger}")

    while True:
        if is_jobless(root_work, inactivity_secs, logger):
            try:
                seppuku(logger)
            except Exception as e:
                logging.error(
                    f"Got exception in seppuku(): {str(e)}\n{traceback.format_exc()}"
                )
        time.sleep(check_interval)


if __name__ == "__main__":
    # Initialize arguments
    root_work_dir = None
    logger = None
    inactivity = None
    check = None

    # Parse the configuration file if there was one
    conf_parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    conf_parser.add_argument(
        "-f",
        "--file",
        type=str,
        default=None,
        help="Configuration file. Anything specified on the command-line takes precedence.",
    )
    args, remaining_argv = conf_parser.parse_known_args()
    config_args = dict()
    if args.file:
        with open(args.file) as f:
            config_params = yaml.safe_load(f)
            root_work_dir = config_params.get("root_work_dir", None)
            logger = config_params.get("logger", None)
            inactivity = config_params.get("inactivity", None)
            check = config_params.get("check", None)

    # Parse the rest of the arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "root_work_dir",
        nargs="?",
        default=None,
        help="root HySDS work directory, e.g. /data/work",
    )
    parser.add_argument(
        "-i",
        "--inactivity",
        type=int,
        default=None,
        help="inactivity threshold in seconds. Default is 600.",
    )
    parser.add_argument(
        "-c",
        "--check",
        type=int,
        default=None,
        help="check for inactivity every N seconds. Default is 60.",
    )
    parser.add_argument(
        "-l",
        "--logger",
        type=str,
        default=None,
        help="enable event logging; specify Mozart REST API,"
        + " e.g. https://192.168.0.1/mozart/api/v0.1",
    )
    args = parser.parse_args(remaining_argv)
    if args.root_work_dir:
        root_work_dir = args.root_work_dir
    if args.logger:
        logger = args.logger
    if args.inactivity:
        inactivity = args.inactivity
    if args.check:
        check = args.check

    # Set the default values for inactivity and check here
    if inactivity is None:
        inactivity = 600
    if check is None:
        check = 60

    harikiri(root_work_dir, inactivity, check, logger)
