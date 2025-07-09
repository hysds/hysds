from future import standard_library

standard_library.install_aliases()

import getpass
import json
import os
import shutil
import sys
from abc import ABC, abstractmethod
from tempfile import mkdtemp

import osaka.main

# from subprocess import Popen, PIPE
from atomicwrites import atomic_write

from hysds.celery import app
from hysds.log_utils import logger
from hysds.utils import datetime_iso_naive


class Base(ABC):
    IMAGE_LOAD_TIME_MAX = 600

    def __init__(self):
        self._uid = os.getuid()
        self._gid = os.getgid()
        self._user = getpass.getuser()

    @abstractmethod
    def inspect_image(self, image):
        """
        inspect the container image; ex. docker inspect <image>
        :param image: str
        :return: str/byte
        """
        raise RuntimeError(
            "method 'inspect_image' must be defined in the derived class"
        )

    @abstractmethod
    def inspect_image_with_backoff(self, image):
        """
        inspect the container image; ex. docker inspect <image>
        :param image: str
        :return: str/byte
        """
        raise RuntimeError(
            "method 'inspect_image' must be defined in the derived class"
        )

    @abstractmethod
    def pull_image(self, image):
        """
        Pulls image, ex. run the 'docker pull <image>' command
        :param image:
        :return: str/byte
        """
        raise RuntimeError("method 'pull_image' must be defined in the derived class")

    @abstractmethod
    def tag_image(self, registry_url, image):
        """
        Tags your image, ex. 'docker tag <image>' command
        :param registry_url: str
        :param image: str
        :return: str/byte
        """
        raise RuntimeError("method 'tag_image' must be defined in the derived class")

    @abstractmethod
    def load_image(self, image_file):
        """
        Loads image into the container engine, ex. "docker load -i <image_file>"
        :param image_file: str, file location of docker image
        :return: Popen object: https://docs.python.org/3/library/subprocess.html#popen-objects
        """
        raise RuntimeError("method 'load_image' must be defined in the derived class")

    @abstractmethod
    def create_base_cmd(self, params):
        """
        Parse docker params and build base docker command line list.
            ex. [ "docker", "run", "--init", "--rm", "-u", ... ]
        :return: List[str]
        """
        raise RuntimeError(
            "method 'create_base_cmd' must be defined in the derived class"
        )

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
                raise RuntimeError(f"Cannot mount {mount}: {k} is blacklisted")
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
        logger.info(f"Copied container mount {path} to {mnt_path}.")
        return os.path.join(mnt_dir, os.path.basename(path))

    def create_container_params(
        self,
        image_name,
        image_url,
        image_mappings,
        root_work_dir,
        job_dir,
        runtime_options=None,
        verdi_home=None,
        host_verdi_home=None,
    ):
        """
        Build container params for runtime.
        :param image_name: str
        :param image_url: str
        :param image_mappings: dict
        :param root_work_dir: str
        :param job_dir: str
        :param runtime_options: None/dict
        :param verdi_home: str
        :param host_verdi_home: str
        :return:
        """
        root_jobs_dir = os.path.join(root_work_dir, "jobs")
        root_tasks_dir = os.path.join(root_work_dir, "tasks")
        root_workers_dir = os.path.join(root_work_dir, "workers")
        root_cache_dir = os.path.join(root_work_dir, "cache")

        params = {
            "image_name": image_name,
            "image_url": image_url,
            "uid": self._uid,
            "gid": self._gid,
            "user_name": self._user,
            "working_dir": job_dir,
            "volumes": [
                (root_jobs_dir, root_jobs_dir),
                (root_tasks_dir, root_tasks_dir),
                (root_workers_dir, root_workers_dir),
            ],
        }

        if app.conf.get("CACHE_READ_ONLY", True) is True:
            params["volumes"].append((root_cache_dir, f"{root_cache_dir}:ro"))
        else:
            logger.info(
                f"CACHE_READ_ONLY set to false. Making it writable: {root_cache_dir}"
            )
            params["volumes"].append((root_cache_dir, f"{root_cache_dir}"))

        # add default image mappings
        celery_cfg_file = os.environ.get("HYSDS_CELERY_CFG", app.conf.__file__)
        if celery_cfg_file not in image_mappings and "celeryconfig.py" not in list(
            image_mappings.values()
        ):
            image_mappings[celery_cfg_file] = "celeryconfig.py"
        dsets_cfg_file = os.environ.get(
            "HYSDS_DATASETS_CFG",
            os.path.normpath(
                os.path.join(
                    os.path.dirname(sys.executable), "..", "etc", "datasets.json"
                )
            ),
        )
        if dsets_cfg_file not in image_mappings and "datasets.json" not in list(
            image_mappings.values()
        ):
            image_mappings[dsets_cfg_file] = "datasets.json"

        # if running on k8s add hosts and resolv.conf; create mount directory
        blacklist = app.conf.WORKER_MOUNT_BLACKLIST
        mnt_dir = None
        on_k8s = int(
            app.conf.get("K8S", 0)
        )  # TODO: may look into this for K8 integration
        if on_k8s:
            for f in ("/etc/hosts", "/etc/resolv.conf"):
                if f not in image_mappings and f not in list(image_mappings.values()):
                    image_mappings[f] = f
            blacklist = [i for i in blacklist if i != "/etc"]
            mnt_dir = mkdtemp(prefix=".container_mounts-", dir=job_dir)

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
                    raise RuntimeError(f"Invalid image mapping: {k}:{v}")
            if v.startswith("/"):
                mnt = v
            else:
                mnt = os.path.join(job_dir, v)
            if mnt_dir is not None:
                k = self.copy_mount(k, mnt_dir)
            # This will ensure that host paths are specified in the volume source mounts
            # rather than paths found only in the verdi container
            host_k = k
            if verdi_home and host_verdi_home:
                logger.info(f"verdi_home={verdi_home}, host_home={host_verdi_home}")
                if k.startswith(verdi_home):
                    host_k = k.replace(verdi_home, host_verdi_home)
                    logger.info(f"Replacing {k} with {host_k} in the volume mount")
                else:
                    logger.info(
                        f"Could not find {verdi_home} in {k}. Nothing to replace"
                    )
            else:
                logger.info(
                    f"verdi_home and/or host_home are not set. So will not convert source "
                    f"volume mount to point to a location on the host: {k}"
                )

            params["volumes"].append((host_k, f"{mnt}:{mode}"))

        # add runtime resources
        params["runtime_options"] = dict()
        if runtime_options is None:
            runtime_options = dict()
        for k, v in list(runtime_options.items()):
            if (
                k == "gpus" and int(os.environ.get("HYSDS_GPU_AVAILABLE", 0)) == 0
            ):  # validate we have GPUs
                logger.warning(
                    "Job specified runtime option 'gpus' but no GPUs were detected. Skipping this option"
                )
                continue
            params["runtime_options"][k] = v
        return params

    def ensure_image_loaded(self, image_name, image_url, cache_dir):
        """Pull docker image into local repo."""

        # check if image is in local docker repo
        try:
            registry = app.conf.get("CONTAINER_REGISTRY", None)
            # Custom edit to load image from registry
            try:
                if registry is not None:
                    logger.info(
                        f"Trying to load image {image_name} from registry '{registry}'"
                    )
                    registry_url = os.path.join(registry, image_name)
                    logger.info(
                        f"{self.__class__.__name__.lower()} pull {registry_url}"
                    )
                    self.pull_image(registry_url)
                    logger.info(
                        f"{self.__class__.__name__.lower()} tag {registry_url} {image_name}"
                    )
                    self.tag_image(registry_url, image_name)
            except Exception as e:
                logger.warning(f"Unable to load image from registry '{registry}': {e}")

            image_info = self.inspect_image(image_name)
            logger.info(f"Container image {image_name} cached in repo")
        except Exception as e:
            logger.info(f"Failed to inspect image {image_name}: {str(e)}")

            # pull image from url
            if image_url is not None:
                image_file = os.path.join(cache_dir, os.path.basename(image_url))
                if not os.path.exists(image_file):
                    logger.info(
                        f"Downloading image {image_file} ({image_name}) from {image_url}"
                    )
                    try:
                        osaka.main.get(image_url, image_file)
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to download image {image_url}:\n{str(e)}"
                        )
                    logger.info(
                        f"Downloaded image {image_file} ({image_name}) from {image_url}"
                    )
                load_lock = f"{image_file}.load.lock"
                try:
                    with atomic_write(load_lock) as f:
                        f.write(f"{datetime_iso_naive()}Z\n")
                    logger.info(f"Loading image {image_file} ({image_name})")
                    p = self.load_image(image_file)
                    stdout, stderr = p.communicate()
                    if p.returncode != 0:
                        raise RuntimeError(
                            f"Failed to load image {image_file} ({image_name}): {stderr.decode()}"
                        )
                    logger.info(f"Loaded image {image_file} ({image_name})")
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
                        logger.info(
                            f"Waiting for image {image_file} ({image_name}) to load"
                        )
                        self.inspect_image_with_backoff(image_name)
                    else:
                        raise
            else:
                # pull image from docker hub
                logger.info(f"Pulling image {image_name} from docker hub")
                self.pull_image(image_name)
                logger.info(f"Pulled image {image_name} from docker hub")
            image_info = self.inspect_image(image_name)
        logger.info(f"image info for {image_name}: {image_info.decode()}")
        return json.loads(image_info)[0]

    def get_container_cmd(self, params, cmd_line_list):
        """
        Parse given params and build base container command line list.
            ex. [ "docker", "run", "--init", "--rm", "-u", ... ]
        :return: List[str]
        """
        container_cmd = self.create_base_cmd(params)
        # set command
        container_cmd.extend([str(i) for i in cmd_line_list])

        return container_cmd
