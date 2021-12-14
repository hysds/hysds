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
import shutil
from datetime import datetime
# from subprocess import Popen, PIPE
from atomicwrites import atomic_write
from tempfile import mkdtemp

from hysds.log_utils import logger
from hysds.celery import app

import osaka.main


class Base:
    IMAGE_LOAD_TIME_MAX = 60

    @classmethod
    def inspect_image(cls, image):
        """
        inspect the container image; ex. docker inspect <image>
        :param image: str
        :return: str/byte
        """
        raise RuntimeError("method 'inspect_image' must be defined in the derived class")

    @classmethod
    def inspect_image_with_backoff(cls, image):
        """
        inspect the container image; ex. docker inspect <image>
        :param image: str
        :return: str/byte
        """
        raise RuntimeError("method 'inspect_image' must be defined in the derived class")

    @classmethod
    def pull_image(cls, image):
        """
        Pulls image, ex. run the 'docker pull <image>' command
        :param image:
        :return: str/byte
        """
        raise RuntimeError("method 'pull_image' must be defined in the derived class")

    @classmethod
    def tag_image(cls, registry_url, image):
        """
        Tags your image, ex. 'docker tag <image>' command
        :param registry_url: str
        :param image: str
        :return: str/byte
        """
        raise RuntimeError("method 'tag_image' must be defined in the derived class")

    @classmethod
    def load_image(cls, image_file):
        """
        Loads image into the container engine, ex. "docker load -i <image_file>"
        :param image_file: str, file location of docker image
        :return: Popen object: https://docs.python.org/3/library/subprocess.html#popen-objects
        """
        # Popen(["docker", "load", "-i", image_file], stderr=PIPE, stdout=PIPE)
        raise RuntimeError("method 'load_image' must be defined in the derived class")

    def create_base_cmd(self, params):
        """
        Parse docker params and build base docker command line list.
            ex. [ "docker", "run", "--init", "--rm", "-u", ... ]
        :return: List[str]
        """
        raise RuntimeError("method 'create_base_cmd' must be defined in the derived class")

    def create_container_cmd(self, params, cmd_line_list):
        """
        builds the final command which will run in the container
            ex. [ "docker", "run", "--init", "--rm", "-u", "0:0", "python", "foo.py", "args" ]
        :param params: Dict[str, any]
        :param cmd_line_list: List[str]
        :return:
        """
        docker_cmd = self.create_base_cmd(params)  # build command
        docker_cmd.extend([str(i) for i in cmd_line_list])  # set command
        return docker_cmd

    @classmethod
    def verify_container_mount(cls, mount, blacklist=app.conf.WORKER_MOUNT_BLACKLIST):
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

    @classmethod
    def copy_mount(cls, path, mnt_dir):
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

    @classmethod
    def create_container_params(cls, image_name, image_url, image_mappings, root_work_dir, job_dir,
                                runtime_options=None):
        """
        Build container params for runtime.
        :param image_name: str
        :param image_url: str
        :param image_mappings: dict
        :param root_work_dir: str
        :param job_dir: str
        :param runtime_options: None/dict
        :return:
        """
        root_jobs_dir = os.path.join(root_work_dir, "jobs")
        root_tasks_dir = os.path.join(root_work_dir, "tasks")
        root_workers_dir = os.path.join(root_work_dir, "workers")
        root_cache_dir = os.path.join(root_work_dir, "cache")

        params = {
            "image_name": image_name,
            "image_url": image_url,
            "uid": os.getuid(),
            "gid": os.getgid(),
            "working_dir": job_dir,
            "volumes": [
                (root_jobs_dir, root_jobs_dir),
                (root_tasks_dir, root_tasks_dir),
                (root_workers_dir, root_workers_dir),
                (root_cache_dir, "{}:ro".format(root_cache_dir)),
            ],
        }

        # add default image mappings
        celery_cfg_file = os.environ.get("HYSDS_CELERY_CFG", app.conf.__file__)
        if celery_cfg_file not in image_mappings and "celeryconfig.py" not in list(image_mappings.values()):
            image_mappings[celery_cfg_file] = "celeryconfig.py"
        dsets_cfg_file = os.environ.get(
            "HYSDS_DATASETS_CFG",
            os.path.normpath(os.path.join(os.path.dirname(sys.executable), "..", "etc", "datasets.json")),
        )
        if dsets_cfg_file not in image_mappings and "datasets.json" not in list(image_mappings.values()):
            image_mappings[dsets_cfg_file] = "datasets.json"

        # if running on k8s add hosts and resolv.conf; create mount directory
        blacklist = app.conf.WORKER_MOUNT_BLACKLIST
        mnt_dir = None
        on_k8s = int(app.conf.get("K8S", 0))  # TODO: may look into this for K8 integration
        if on_k8s:
            for f in ("/etc/hosts", "/etc/resolv.conf"):
                if f not in image_mappings and f not in list(image_mappings.values()):
                    image_mappings[f] = f
            blacklist = [i for i in blacklist if i != "/etc"]
            mnt_dir = mkdtemp(prefix=".container_mounts-", dir=job_dir)

        # add user-defined image mappings
        for k, v in list(image_mappings.items()):
            k = os.path.expandvars(k)
            cls.verify_container_mount(k, blacklist)

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
                mnt = os.path.join(job_dir, v)
            if mnt_dir is not None:
                k = cls.copy_mount(k, mnt_dir)
            params["volumes"].append((k, "%s:%s" % (mnt, mode)))

        # add runtime resources
        params["runtime_options"] = dict()
        if runtime_options is None:
            runtime_options = dict()
        for k, v in list(runtime_options.items()):
            if k == "gpus" and int(os.environ.get("HYSDS_GPU_AVAILABLE", 0)) == 0:  # validate we have GPUs
                logger.warning("Job specified runtime option 'gpus' but no GPUs were detected. Skipping this option")
                continue
            params["runtime_options"][k] = v
        return params

    @classmethod
    def ensure_image_loaded(cls, image_name, image_url, cache_dir):
        """Pull docker image into local repo."""

        # check if image is in local docker repo
        try:
            registry = app.conf.get("CONTAINER_REGISTRY", None)
            # Custom edit to load image from registry
            try:
                if registry is not None:
                    logger.info("Trying to load docker image {} from registry '{}'".format(image_name, registry))
                    registry_url = os.path.join(registry, image_name)
                    logger.info("docker pull {}".format(registry_url))
                    cls.pull_image(image_name)
                    logger.info("docker tag {} {}".format(registry_url, image_name))
                    cls.tag_image(registry_url, image_name)
            except Exception as e:
                logger.warn("Unable to load docker image from registry '{}': {}".format(registry, e))

            image_info = cls.inspect_image(image_name)
            logger.info("Docker image %s cached in repo" % image_name)
        except Exception as e:
            logger.info("Failed to inspect docker image %s: %s" % (image_name, str(e)))

            # pull image from url
            if image_url is not None:
                image_file = os.path.join(cache_dir, os.path.basename(image_url))
                if not os.path.exists(image_file):
                    logger.info("Downloading image %s (%s) from %s" % (image_file, image_name, image_url))
                    try:
                        osaka.main.get(image_url, image_file)
                    except Exception as e:
                        raise RuntimeError("Failed to download image {}:\n{}".format(image_url, str(e)))
                    logger.info("Downloaded image %s (%s) from %s" % (image_file, image_name, image_url))
                load_lock = "{}.load.lock".format(image_file)
                try:
                    with atomic_write(load_lock) as f:
                        f.write("%sZ\n" % datetime.utcnow().isoformat())
                    logger.info("Loading image %s (%s)" % (image_file, image_name))
                    p = cls.load_image(image_file)
                    stdout, stderr = p.communicate()
                    if p.returncode != 0:
                        raise RuntimeError(
                            "Failed to load image {} ({}): {}".format(
                                image_file, image_name, stderr.decode()
                            )
                        )
                    logger.info("Loaded image %s (%s)" % (image_file, image_name))
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
                        logger.info("Waiting for image %s (%s) to load" % (image_file, image_name))
                        cls.inspect_image_with_backoff(image_name)
                    else:
                        raise
            else:
                # pull image from docker hub
                logger.info("Pulling image %s from docker hub" % image_name)
                cls.pull_image(image_name)
                logger.info("Pulled image %s from docker hub" % image_name)
            image_info = cls.inspect_image(image_name)
        logger.info("image info for %s: %s" % (image_name, image_info.decode()))
        return json.loads(image_info)[0]
