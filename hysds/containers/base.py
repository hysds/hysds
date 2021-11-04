from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import


from builtins import int
from builtins import str
from future import standard_library

standard_library.install_aliases()
import os
import sys
import json
# import backoff
import shutil
from datetime import datetime
from subprocess import Popen, PIPE
from atomicwrites import atomic_write
from tempfile import mkdtemp

from hysds.log_utils import logger
from hysds.celery import app

import osaka.main


class Base:
    def __init__(self, image_name, image_url, root_work_dir, job_dir, **kwargs):  # image_mappings, runtime_options=None
        self.image_name = image_name
        self.image_url = image_url
        self.job_dir = job_dir
        self.root_jobs_dir = os.path.join(root_work_dir, "jobs")
        self.root_tasks_dir = os.path.join(root_work_dir, "tasks")
        self.root_workers_dir = os.path.join(root_work_dir, "workers")
        self.root_cache_dir = os.path.join(root_work_dir, "cache")

        self.params = {
            "image_name": image_name,
            "image_url": image_url,
            "uid": os.getuid(),
            "gid": os.getgid(),
            "working_dir": job_dir,
            "runtime_options": {},
            "volumes": [
                (self.root_jobs_dir, self.root_jobs_dir),
                (self.root_tasks_dir, self.root_tasks_dir),
                (self.root_workers_dir, self.root_workers_dir),
                (self.root_cache_dir, "%s:ro" % self.root_cache_dir),
            ],
        }

    @classmethod
    def initialize(cls):
        pass

    @staticmethod
    def inspect_image(image):
        """
        inspect the container image; ex. docker inspect <image>
        :param image: str
        :return: str/byte
        """
        raise RuntimeError("method 'inspect_image' must be defined in the derived class")

    @staticmethod
    def pull_image(image):
        """
        run the 'docker pull <iamge>' command
        :param image:
        :return: str/byte
        """
        raise RuntimeError("method 'pull_image' must be defined in the derived class")

    @staticmethod
    def tag_image(image):
        """
        run the 'docker tag <iamge>' command
        :param image:
        :return: str/byte
        """
        raise RuntimeError("method 'tag_image' must be defined in the derived class")

    def get_base_cmd(self):
        """
        Parse docker params and build base docker command line list.
            ex. [ "docker", "run", "--init", "--rm", "-u", ... ]
        :return: List[str]
        """
        raise RuntimeError("method 'get_base_cmd' must be defined in the derived class")

    def get_container_cmd(self, cmd_line_list):
        """
        builds the final command which will run in the container
            ex. [ "docker", "run", "--init", "--rm", "-u", "0:0", "python", "foo.py", "args" ]
        :param cmd_line_list: List[str]
        :return:
        """
        docker_cmd = self.get_base_cmd()  # build command
        docker_cmd.extend([str(i) for i in cmd_line_list])  # set command
        return docker_cmd

    @staticmethod
    def verify_container_mount(mount, blacklist=app.conf.WORKER_MOUNT_BLACKLIST):
        """
        Verify host mount directory, ex. /data/work/...
        :param mount:
        :param blacklist:
        :return:
        """
        if mount == "/":
            raise RuntimeError("Cannot mount host root directory")
        for k in blacklist:
            if mount.startswith(k):
                raise RuntimeError("Cannot mount %s: %s is blacklisted" % (mount, k))
        return True

    @staticmethod
    def copy_mount(path, mnt_dir):
        """
        Copy path to a directory to be used for mounting into container. Return this path.
        :param path: str
        :param mnt_dir: str, ex; /mnt/...
        :return: str; total path of mount location
        """
        if not os.path.exists(mnt_dir):
            os.makedirs(mnt_dir, 0o777)
        mnt_path = os.path.join(mnt_dir, os.path.basename(path))
        if os.path.isdir(path):
            shutil.copytree(path, mnt_path)
        else:
            shutil.copy(path, mnt_path)
        logger.info("Copied container mount {} to {}.".format(path, mnt_path))
        return os.path.join(mnt_dir, os.path.basename(path))

    def validate_params(self, image_mappings, runtime_options=None):
        # add default image mappings
        celery_cfg_file = os.environ.get("HYSDS_CELERY_CFG", app.conf.__file__)
        if celery_cfg_file not in image_mappings and "celeryconfig.py" not in list(image_mappings.values()):
            image_mappings[celery_cfg_file] = "celeryconfig.py"

        datasets_cfg_file = os.environ.get(
            "HYSDS_DATASETS_CFG",
            os.path.normpath(os.path.join(os.path.dirname(sys.executable), "..", "etc", "datasets.json")),
        )
        if datasets_cfg_file not in image_mappings and "datasets.json" not in list(image_mappings.values()):
            image_mappings[datasets_cfg_file] = "datasets.json"

        # if running on k8s add hosts and resolv.conf; create mount directory
        blacklist = app.conf.WORKER_MOUNT_BLACKLIST
        mnt_dir = None
        on_k8s = int(app.conf.get("K8S", 0))
        if on_k8s:
            for f in ("/etc/hosts", "/etc/resolv.conf"):
                if f not in image_mappings and f not in list(image_mappings.values()):
                    image_mappings[f] = f
            blacklist = [i for i in blacklist if i != "/etc"]
            mnt_dir = mkdtemp(prefix=".container_mounts-", dir=self.job_dir)

        # add user-defined image mappings
        for k, v in list(image_mappings.items()):
            k = os.path.expandvars(k)
            self.verify_container_mount(k, blacklist)
            mode = "ro"
            if isinstance(v, list):
                if len(v) > 1:
                    v, mode = v[0:2]
                elif len(v) == 1:
                    v = v[0]
                else:
                    raise RuntimeError("Invalid image mapping: %s:%s" % (k, v))
            if v.startswith("/"):
                mnt = v
            else:
                mnt = os.path.join(self.job_dir, v)
            if mnt_dir is not None:
                k = self.copy_mount(k, mnt_dir)
            self.params["volumes"].append((k, "%s:%s" % (mnt, mode)))

        if runtime_options is None:
            runtime_options = {}
        for k, v in list(runtime_options.items()):  # validate we have GPUs
            if k == "gpus" and int(os.environ.get("HYSDS_GPU_AVAILABLE", 0)) == 0:
                logger.warning("Job specified runtime option 'gpus' but no GPUs were detected. Skipping this option")
                continue
            self.params["runtime_options"][k] = v
        return self.params

    def ensure_image_loaded(self, cache_dir):
        """Pull docker image into local repo."""

        # check if image is in local docker repo
        try:
            registry = app.conf.get("CONTAINER_REGISTRY", None)
            # Custom edit to load image from registry
            try:
                if registry is not None:
                    logger.info("Trying to load docker image {} from registry '{}'".format(self.image_name, registry))
                    registry_url = os.path.join(registry, self.image_name)
                    logger.info("docker pull {}".format(registry_url))
                    self.pull_image(registry_url)
                    logger.info("docker tag {} {}".format(registry_url, self.image_name))
                    self.tag_image(self.image_name)
            except Exception as e:
                logger.warn("Unable to load docker image from registry '{}': {}".format(registry, e))

            image_info = self.inspect_image(self.image_name)
            logger.info("Docker image %s cached in repo" % self.image_name)
        except:
            logger.info("Failed to inspect docker image %s" % self.image_name)

            # pull image from url
            if self.image_url is not None:
                image_file = os.path.join(cache_dir, os.path.basename(self.image_url))
                if not os.path.exists(image_file):
                    logger.info("Downloading image %s (%s) from %s" % (image_file, self.image_name, self.image_url))
                    try:
                        osaka.main.get(self.image_url, image_file)
                    except Exception as e:
                        raise RuntimeError("Failed to download image {}:\n{}".format(self.image_url, str(e)))
                    logger.info("Downloaded image %s (%s) from %s" % (image_file, self.image_name, self.image_url))
                load_lock = "{}.load.lock".format(image_file)
                try:
                    with atomic_write(load_lock) as f:
                        f.write("%sZ\n" % datetime.utcnow().isoformat())
                    logger.info("Loading image %s (%s)" % (image_file, self.image_name))
                    p = Popen(["docker", "load", "-i", image_file], stderr=PIPE, stdout=PIPE)
                    stdout, stderr = p.communicate()
                    if p.returncode != 0:
                        raise RuntimeError("Failed to load image {} ({}): {}".format(image_file, self.image_name,
                                                                                     stderr.decode()))
                    logger.info("Loaded image %s (%s)" % (image_file, self.image_name))
                    try:
                        os.unlink(image_file)
                    except:
                        pass
                    try:
                        os.unlink(load_lock)
                    except:
                        pass
                except OSError as e:
                    if e.errno == 17:
                        logger.info("Waiting for image %s (%s) to load" % (image_file, self.image_name))
                        self.inspect_image(self.image_name)
                    else:
                        raise
            else:
                logger.info("Pulling image %s from docker hub" % self.image_name)
                self.pull_image(self.image_name)
                logger.info("Pulled image %s from docker hub" % self.image_name)
            image_info = self.inspect_image(self.image_name)
        logger.info("image info for %s: %s" % (self.image_name, image_info.decode()))
        return json.loads(image_info)[0]
