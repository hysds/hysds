from __future__ import absolute_import
from __future__ import print_function

import os, sys, json
from subprocess import check_output, Popen, PIPE

from hysds.log_utils import logger
from hysds.celery import app

import osaka.main


def verify_docker_mount(m):
    """Verify host mount."""

    if m == "/": raise(RuntimeError("Cannot mount host root directory"))
    for k in app.conf.WORKER_MOUNT_BLACKLIST:
        if m.startswith(k):
            raise(RuntimeError("Cannot mount %s: %s is blacklisted" % (m, k)))
    return True


def get_docker_params(image_name, image_url, image_mappings, root_work_dir, job_dir):
    """Build docker params."""

    # docker params dict
    params = {
        "image_name": image_name,
        "image_url": image_url,
        "uid": os.getuid(),
        "gid": os.getgid(),
        "working_dir": job_dir,
        "volumes": {
            "/sys/fs/cgroup": "/sys/fs/cgroup:ro",
            "/var/run/docker.sock": "/var/run/docker.sock",
        }
    }

    # add root work directory
    params['volumes'][root_work_dir] = root_work_dir

    # add default image mappings
    celery_cfg_file = os.environ.get('HYSDS_CELERY_CFG',
                                     os.path.join(os.path.dirname(app.conf.__file__),
                                                  "celeryconfig.py"))
    if celery_cfg_file not in image_mappings and "celeryconfig.py" not in image_mappings.values():
        image_mappings[celery_cfg_file] = "celeryconfig.py"
    dsets_cfg_file = os.environ.get('HYSDS_DATASETS_CFG',
                                    os.path.normpath(os.path.join(os.path.dirname(sys.executable),
                                                                  '..', 'etc', 'datasets.json')))
    if dsets_cfg_file not in image_mappings and "datasets.json" not in image_mappings.values():
        image_mappings[dsets_cfg_file] = "datasets.json"

    # add user-defined image mappings
    for k, v in image_mappings.iteritems():
        k = os.path.expandvars(k)
        verify_docker_mount(k)
        mode = "ro"
        if isinstance(v, list):
            if len(v) > 1: v, mode = v[0:2]
            elif len(v) == 1: v = v[0]
            else: raise(RuntimeError("Invalid image mapping: %s:%s" % (k, v)))
        if v.startswith('/'): mnt = v
        else: mnt = os.path.join(job_dir, v)
        params['volumes'][k] = "%s:%s" % (mnt, mode)

    return params


def ensure_image_loaded(image_name, image_url, cache_dir):
    """Pull docker image into local repo."""

    # check if image is in local docker repo
    try:
        image_info = check_output(['docker', 'inspect', image_name])
        logger.info("Docker image %s cached in repo" % image_name)
    except:
        logger.info("Failed to inspect docker image %s" % image_name)

        # pull image from url
        if image_url is not None:
            image_file = os.path.join(cache_dir, os.path.basename(image_url))
            if not os.path.exists(image_file):
                logger.info("Downloading image %s (%s) from %s" % 
                            (image_file, image_name, image_url))
                osaka.main.get(image_url, image_file)
                logger.info("Downloaded image %s (%s) from %s" %
                            (image_file, image_name, image_url))
            logger.info("Loading image %s (%s)" % (image_file, image_name))
            p = Popen(['docker', 'load', '-i', image_file], stderr=PIPE, stdout=PIPE)
            stdout, stderr = p.communicate()
            if p.returncode != 0:
                raise(RuntimeError("Failed to load image %s (%s): %s" % (image_file, image_name, stderr)))
            logger.info("Loaded image %s (%s)" % (image_file, image_name))
            try: os.unlink(image_file)
            except: pass
        else:
            # pull image from docker hub
            logger.info("Pulling image %s from docker hub" % image_name)
            check_output(['docker', 'pull', image_name])
            logger.info("Pulled image %s from docker hub" % image_name)
        image_info = check_output(['docker', 'inspect', image_name])


def get_base_docker_cmd(params):
    """Parse docker params and build base docker command line list."""

    # build command
    docker_cmd_base = [ "docker", "run", "--rm", "-u", 
                        "%s:%s" % (params['uid'], params['gid']) ]

    # add volumes
    for k, v in volumes.iteritems():
        docker_cmd_base.extend(["-v", "%s:%s" % (k, v)])

    # set work directory and image
    docker_cmd_base.extend(["-w", params['working_dir'], params['image_name']])

    return docker_cmd_base


def get_docker_cmd(params, cmd_line_list):
    """Pull docker image into local repo and add call to docker in the 
       command line list."""

    # build command
    docker_cmd = get_base_docker_cmd(params)

    # set command
    docker_cmd.extend([str(i) for i in cmd_line_list])

    return docker_cmd
