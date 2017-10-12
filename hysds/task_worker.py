from __future__ import absolute_import

import os, sys, re, json, time, shlex, tempfile
import requests, shutil, socket, backoff, logging
from datetime import datetime

from hysds.celery import app
from hysds.orchestrator import get_function
from hysds.job_worker import get_facts, AZ_INFO, INS_TYPE_INFO
from hysds.log_utils import backoff_max_tries, backoff_max_value
from hysds.utils import makedirs
#from hysds.pymonitoredrunner.MonitoredRunner import MonitoredRunner


# store facts
FACTS = None


@app.task(bind=True)
def run_task(self, payload):
    """Run task on a task worker. Payload is a JSON file describing the task.
       Currently supports python function task.

       Example of function payload:

           {
               "type": "my_job_type",
               "function": "mymodule.myfunction",
               "sys_path": "/home/ops/custom_libs", <- optional
               "args": [ 1, "a" ],
               "kwargs": {
                   "kw1": "this is a test",
                   "kw2": "this is another test",
               }
           }
    """

    # get worker instance facts
    facts = get_facts()

    # get availability zone and instance type
    try:
        r = requests.get(AZ_INFO, timeout=1)
        if r.status_code == 200:
            facts['ec2_placement_availability_zone'] = r.content
    except: pass
    try:
        r = requests.get(INS_TYPE_INFO, timeout=1)
        if r.status_code == 200:
            facts['ec2_instance_type'] = r.content
    except: pass

    # get task id and delivery info
    task_id = self.request.id
    delivery_info = self.request.delivery_info

    # create task work directory and enter it
    task_dir_abs = os.path.join(app.conf.ROOT_WORK_DIR, 'tasks')
    #yr, mo, dy, hr, mi, se, wd, y, z = time.gmtime()
    #task_dir = os.path.join(task_dir_abs, "%04d" % yr, "%02d" % mo, "%02d" % dy,
    #                       "%02d" % hr, "%02d" % mi, task_id)
    task_dir = task_dir_abs
    makedirs(task_dir)
    webdav_url = "http://%s:%s" % (facts['hysds_public_ip'], app.conf.WEBDAV_PORT)
    #task_url = os.path.join(webdav_url, 'tasks', "%04d" % yr, "%02d" % mo, "%02d" % dy,
    #                        "%02d" % hr, "%02d" % mi, task_id)
    task_url = os.path.join(webdav_url, 'tasks')
    os.chdir(task_dir)

    # set up task logger
    #log_file = os.path.join(task_dir, 'run_task.log')
    #task_logger = self.app.log.setup_task_loggers(loglevel=logging.DEBUG, logfile=log_file)
    task_logger = self.app.log.setup_task_loggers(loglevel=logging.DEBUG)
    #hdlr = logging.FileHandler(log_file)
    #task_logger.addHandler(hdlr)
    #old_outs = sys.stdout, sys.stderr

    # run task
    try:
        # redirect stdout/stderr
        #self.app.log.redirect_stdouts_to_logger(task_logger, loglevel=logging.DEBUG)

        # write task's running file to reserve space for task's done file later
        #task_running_file = os.path.join(task_dir, '.running')
        #with open(task_running_file, 'w') as f:
        #    f.write("%sZ\n" % datetime.utcnow().isoformat())

        # get task's .done file
        #task_done_file = os.path.join(task_dir, '.done')

        # parse payload
        sys_path = payload.get('sys_path', None) 
        func = get_function(payload['function'], sys_path)
        args = payload.get('args', [])
        kwargs = payload.get('kwargs', {})

        # log task info
        task_logger.info("task type: %s" % payload['type'])
        task_logger.info("function to run: %s" % payload['function'])
        task_logger.info("sys_path: %s" % str(sys_path))
        task_logger.info("args: %s" % str(args))
        task_logger.info("kwargs: %s" % json.dumps(kwargs, indent=2))
        task_logger.info("task started: {}".format(datetime.utcnow().isoformat()))
 
        # get task result
        result = func(*args, **kwargs)
        task_logger.info("result: {}".format(result))
    finally:
        # restore stdout/stderr
        #sys.stdout, sys.stderr = old_outs
        #task_logger.removeHandler(hdlr)

        # transition running file to done file
        #os.rename(task_running_file, task_done_file)
        #with open(task_done_file, 'w') as f:
        #    f.write("%sZ\n" % datetime.utcnow().isoformat())
        task_logger.info("task finished: {}".format(datetime.utcnow().isoformat()))

    # return task url
    return task_url
    

#@app.task
#def run_task(payload):
#    """Run task on a task worker. Payload is a JSON file describing the task.
#       Currently supports 2 types of tasks, python function and command line.
#
#       Example of function payload:
#
#           {
#               "type": "function",
#               "function": "mymodule.myfunction",
#               "sys_path": "/home/ops/custom_libs", <- optional
#               "args": [ 1, "a" ],
#               "kwargs": {
#                   "kw1": "this is a test",
#                   "kw2": "this is another test",
#               }
#           }
#
#       Example of command line:
#
#           {
#               "type": "command",
#               "command": "/bin/ls /data/public /data/cache",
#               "env": { <- optional
#                   "PATH": "/bin:/usr/bin:/usr/local/bin"
#               }
#           }
#    """
#
#    if payload['type'] == 'function':
#        sys_path = payload.get('sys_path', None) 
#        func = get_function(payload['function'], sys_path)
#        args = payload.get('args', [])
#        kwargs = payload.get('kwargs', {})
#
#        logger.info("function to run: %s" % payload['function'])
#        logger.info("sys_path: %s" % str(sys_path))
#        logger.info("args: %s" % str(args))
#        logger.info("kwargs: %s" % json.dumps(kwargs, indent=2))
# 
#        return func(*args, **kwargs)
#
#    elif payload['type'] == 'command':
#        cmd_list = shlex.split(payload['command'])
#        env = payload.get('env', {})
#        tmp_dir = tempfile.mkdtemp()
#
#        logger.info("command to run: %s" % payload['command'])
#        logger.info("env: %s" % json.dumps(env, indent=2))
#        logger.info("tmp_dir: %s" % tmp_dir)
#
#        mr = MonitoredRunner(cmd_list, tmp_dir, env, app.conf.PYMONITOREDRUNNER_CFG)
#        mr.start()
#        mr.join()
#        status = mr.getExitCode()
#        pid = mr.getPid()
#        shutil.rmtree(tmp_dir)
#
#        return status
#
#    else:
#        raise RuntimeError("Failed to run task: %s" % json.dumps(payload, indent=2))


@backoff.on_exception(backoff.expo,
                      socket.error,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def do_submit_task(payload, task_queue):
    """Submit task wrapper with exponential backoff and full jitter."""

    # submit job
    return run_task.apply_async((payload,), queue=task_queue)
