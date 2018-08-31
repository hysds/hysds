#!/usr/bin/env python
"""
HySDS inactivity daemon to perform scale down of auto scaling group/spot fleet
request and perform self-termination (harikiri) of the instance. If a keep-alive 
signal file exists at <root_work_dir>/.harikiri, then self-termination is bypassed
until it is removed.
"""
import os, sys, time, re, json, socket, requests, logging, argparse, traceback, backoff
from random import randint
from subprocess import call
from datetime import datetime
from pprint import pformat
import boto3
from botocore.exceptions import ClientError


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


DAY_DIR_RE = re.compile(r'jobs/\d{4}/\d{2}/\d{2}/\d{2}/\d{2}$')

NO_JOBS_TIMER = None

KEEP_ALIVE = False


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
    logging.info("KEEP_ALIVE: %s" % KEEP_ALIVE)
    if keep_alive(root_work):
        if KEEP_ALIVE != True:
            KEEP_ALIVE = True
            if logger is not None:
                try: print(log_event(logger, 'harikiri', 'keep_alive_set', {}, []))
                except: pass
        logging.info("Keep-alive exists.")
        return
    else:
        if KEEP_ALIVE != False:
            KEEP_ALIVE = False
            if logger is not None:
                try: print(log_event(logger, 'harikiri', 'keep_alive_unset', {}, []))
                except: pass
            logging.info("Keep-alive removed.")

    most_recent = None
    for root, dirs, files in os.walk(root_work, followlinks=True):
        match = DAY_DIR_RE.search(root)
        if not match: continue
        dirs.sort()
        for d in dirs:
            job_dir = os.path.join(root, d)
            done_file = os.path.join(job_dir, '.done')
            if not os.path.exists(done_file):
                logging.info("%s: no .done file found. Not jobless yet." % job_dir)
                return False
            t = os.path.getmtime(done_file)
            done_dt = datetime.fromtimestamp(t)
            age = (datetime.utcnow() - done_dt).total_seconds()
            if most_recent is None or age < most_recent: most_recent = age
            logging.info("%s: age=%s" % (job_dir, age))
    if most_recent is None:
        if NO_JOBS_TIMER is None: NO_JOBS_TIMER = time.time()
        else:
            if (time.time() - NO_JOBS_TIMER) > inactivity_secs: return True
        return False
    if most_recent > inactivity_secs: return True
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
        groups.extend(resp['AutoScalingGroups'])
        next_token = resp.get('NextToken', None)
        if next_token is None: break
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
        fleets.extend(resp['SpotFleetRequestConfigs'])
        next_token = resp.get('NextToken', None)
        if next_token is None: break
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
            resp = c.describe_spot_fleet_instances(SpotFleetRequestId=fleet_name,
                                                   NextToken=next_token)
        instances.extend(resp['ActiveInstances'])
        next_token = resp.get('NextToken', None)
        if next_token is None: break
    return instances


@backoff.on_exception(backoff.expo, ClientError, max_value=512)
def detach_instance(c, as_group, id):
    """Detach instance from AutoScaling group."""
    c.detach_instances(InstanceIds=[id], AutoScalingGroupName=as_group,
                       ShouldDecrementDesiredCapacity=True)


@backoff.on_exception(backoff.expo, ClientError, max_value=512)
def decrement_fleet(c, spot_fleet):
    """Decrement target capacity of spot fleet."""

    resp = c.describe_spot_fleet_requests(SpotFleetRequestIds=[spot_fleet])
    tg = resp['SpotFleetRequestConfigs'][0]['SpotFleetRequestConfig']['TargetCapacity']
    logging.info("TargetCapacity: %s" % tg)
    tg -= 1
    if tg > 0:
        c.modify_spot_fleet_request(ExcessCapacityTerminationPolicy='NoTermination',
                                    SpotFleetRequestId=spot_fleet, TargetCapacity=tg)
    else:
        c.cancel_spot_fleet_requests(SpotFleetRequestIds=[spot_fleet],
                                     TerminateInstances=False)
    logging.info("response: %s" % pformat(resp))


def seppuku(logger=None):
    """Shutdown supervisord and the instance if it detects that it is 
       currently part of an autoscale group."""

    logging.info("Initiating seppuku.")

    # introduce random sleep
    meditation_time = randint(0, 600)
    logging.info("Meditating for %s seconds to avoid thundering herd." % meditation_time)
    time.sleep(meditation_time)

    # instances may be part of autoscaling group or spot fleet
    as_group = None
    spot_fleet = None

    # check if instance part of an autoscale group
    id = str(requests.get('http://169.254.169.254/latest/meta-data/instance-id').content)
    logging.info("Our instance id: %s" % id)
    c = boto3.client('autoscaling')
    for group in get_all_groups(c):
        group_name = str(group['AutoScalingGroupName'])
        logging.info("Checking group: %s" % group_name)
        for i in group['Instances']:
            asg_inst_id = str(i['InstanceId'])
            logging.info("Checking group instance: %s" % asg_inst_id)
            if id == asg_inst_id:
                as_group = group_name
                logging.info("Matched!")
                break
    if as_group is None:
        logging.info("This instance %s is not part of any autoscale group." % id)

        # check if instance is part of a spot fleet
        c = boto3.client('ec2')
        for fleet in get_all_fleets(c):
            fleet_name = str(fleet['SpotFleetRequestId'])
            logging.info("Checking fleet: %s" % fleet_name)
            for i in get_fleet_instances(c, fleet_name):
                sf_inst_id = str(i['InstanceId'])
                logging.info("Checking fleet instance: %s" % sf_inst_id)
                if id == sf_inst_id:
                    spot_fleet = fleet_name
                    logging.info("Matched!")
                    break
        if spot_fleet is None:
            logging.info("This instance %s is not part of any spot fleet." % id)

    # gracefully shutdown
    while True:
        try: graceful_shutdown(as_group, spot_fleet, id, logger)
        except Exception, e:
            logging.error("Got exception in graceful_shutdown(): %s\n%s" %
                          (str(e), traceback.format_exc()))
        time.sleep(randint(0, 600))


def graceful_shutdown(as_group, spot_fleet, id, logger=None):
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

    # let supervisord shutdown its processes
    time.sleep(60)

    # detach and die
    logging.info("Committing seppuku.")

    # detach if part of a spot fleet or autoscaling group
    try:
        if as_group is not None:
            c = boto3.client('autoscaling')
            detach_instance(c, as_group, id)
        if spot_fleet is not None:
            c = boto3.client('ec2')
            decrement_fleet(c, spot_fleet)
    except Exception, e:
        logging.error("Got exception in graceful_shutdown(): %s\n%s" %
                      (str(e), traceback.format_exc()))

    time.sleep(60)

    # log seppuku
    if logger is not None:
        try: print(log_event(logger, 'harikiri', 'shutdown', {}, []))
        except: pass

    call(["/usr/bin/sudo", "/sbin/shutdown", "-h", "now"])


def harikiri(root_work, inactivity_secs, check_interval, logger=None):
    """If no jobs are running and the last job finished more than the 
       threshold, shutdown supervisord gracefully then shutdown the 
       instance.
    """

    logging.info("harikiri configuration:")
    logging.info("root_work_dir=%s" % root_work)
    logging.info("inactivity=%d" % inactivity_secs)
    logging.info("check=%d" % check_interval)
    logging.info("logger=%s" % logger)

    while True:
        if is_jobless(root_work, inactivity_secs, logger):
            try: seppuku(logger)
            except Exception, e:
                logging.error("Got exception in seppuku(): %s\n%s" %
                              (str(e), traceback.format_exc()))
        time.sleep(check_interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root_work_dir',
                        help="root HySDS work directory, e.g. /data/work")
    parser.add_argument('-i', '--inactivity', type=int, default=600,
                        help="inactivity threshold in seconds")
    parser.add_argument('-c', '--check', type=int, default=60,
                         help="check for inactivity every N seconds")
    parser.add_argument('-l', '--logger', type=str, default=None,
                         help="enable event logging; specify Mozart REST API," + \
                              " e.g. https://192.168.0.1/mozart/api/v0.1")
    args = parser.parse_args()
    harikiri(args.root_work_dir, args.inactivity, args.check, args.logger)
