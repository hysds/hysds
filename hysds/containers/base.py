from future import standard_library

standard_library.install_aliases()

import getpass
import json
import os
import platform
import shutil
import subprocess
import sys
from abc import ABC, abstractmethod
from tempfile import mkdtemp

import osaka.main

# from subprocess import Popen, PIPE
from atomicwrites import atomic_write

from hysds.celery import app
from hysds.log_utils import logger
from hysds.utils import datetime_iso_naive

# Labels stamped on every container HySDS starts, so a job's containers can be
# found and stopped after the process that started them is gone. Jobs that
# start their own containers (a PGE through the engine socket) get the same
# labels in _docker_params.json and may pass them on; those that do not are
# still found by the job dir they mount.
#
# The job dir label is what matching uses: it identifies one execution. The
# job and task ids do not -- a redelivered task keeps both, and on a host that
# runs several workers on one queue two executions of the same task can run
# side by side, each with its own job dir.
JOB_ID_LABEL = "hysds.job_id"
TASK_ID_LABEL = "hysds.task_id"
JOB_DIR_LABEL = "hysds.job_dir"

# container states that still have a process to stop ("stopping" is podman's
# word for a container mid-stop)
LIVE_STATES = ("running", "paused", "created", "restarting", "stopping")

# seconds to wait on an engine CLI call before giving up on it
CLI_TIMEOUT = 120


class Base(ABC):
    IMAGE_LOAD_TIME_MAX = 600

    def __init__(self):
        self._uid = os.getuid()
        self._gid = os.getgid()
        self._user = getpass.getuser()

    def container_cli(self):
        """
        The engine's CLI prefix for container commands,
            ex. ["docker"] or ["podman", "--remote", "--url", "unix:..."]
        Finding and stopping a job's containers through it assumes a daemon
        that owns them; an engine without one needs its own
        kill_job_containers().
        :return: List[str]
        """
        raise RuntimeError(
            "method 'container_cli' must be defined in the derived class"
        )

    def run_cli(self, args, timeout=CLI_TIMEOUT):
        """
        Run an engine CLI command and return its stdout. A failing or hanging
        call is logged and yields an empty string; the callers here run while
        a job is being torn down and must not raise over the engine.
        :param args: List[str]; the command after the CLI prefix
        :param timeout: int; seconds
        :return: str
        """
        cmd = self.container_cli() + [str(a) for a in args]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except (OSError, subprocess.SubprocessError) as e:
            logger.warning(f"{' '.join(cmd)}: {e}")
            return ""
        if proc.returncode != 0:
            logger.warning(
                f"{' '.join(cmd[:2])} exited {proc.returncode}: {proc.stderr.strip()}"
            )
        return proc.stdout

    def list_containers(self):
        """
        Inspect every container the engine knows about.
        :return: List[dict]; the engine's inspect records
        """
        ids = self.run_cli(["ps", "-aq", "--no-trunc"]).split()
        if not ids:
            return []
        records = self._inspect(ids)
        if len(records) < len(ids):
            # One container the engine cannot read fails the whole batch on
            # podman (a layer it has lost), and docker leaves out one removed
            # since the ps. Inspect the missing ones one at a time so the job's
            # healthy containers are still found.
            seen = {r.get("Id") for r in records}
            for i in ids:
                if i not in seen:
                    records.extend(self._inspect([i]))
        return records

    def _inspect(self, ids):
        """Inspect records for these container ids, or [] when the engine's
        output does not parse."""
        out = self.run_cli(["inspect", "--type", "container"] + ids)
        try:
            records = json.loads(out) if out.strip() else []
        except ValueError as e:
            logger.warning(f"Could not parse container inspect output: {e}")
            return []
        if not isinstance(records, list):
            return []
        return [r for r in records if isinstance(r, dict)]

    @staticmethod
    def _under(path, root):
        """True when path is root or lies below it."""
        if not path or not root:
            return False
        path = os.path.normpath(path)
        root = os.path.normpath(root)
        return path == root or path.startswith(root + os.sep)

    def find_job_containers(self, job_dir):
        """
        Containers that belong to one execution of a job and still have a
        process to stop: the one HySDS started for it (found by its job dir
        label, or by its working dir) and any the job started itself with part
        of the job dir mounted, which is how a PGE launched through the engine
        socket looks. The verdi container and anything else mounting the jobs
        root above the job dir do not match, and neither does a container
        labelled for another job dir, even one of the same job and task.
        :param job_dir: str; the job's work dir, as mounted on the host
        :return: List[dict]; inspect records
        """
        found = []
        for c in self.list_containers():
            state = (c.get("State") or {}).get("Status", "")
            if state not in LIVE_STATES:
                continue
            cfg = c.get("Config") or {}
            labels = cfg.get("Labels") or {}
            label_dir = labels.get(JOB_DIR_LABEL)
            if label_dir:
                if os.path.normpath(label_dir) == os.path.normpath(job_dir):
                    found.append(c)
                continue
            if self._under(cfg.get("WorkingDir"), job_dir):
                found.append(c)
                continue
            if any(
                self._under((m or {}).get("Source"), job_dir)
                for m in (c.get("Mounts") or [])
            ):
                found.append(c)
        return found

    def kill_job_containers(self, job_dir, grace=None):
        """
        Stop the containers of one execution of a job: TERM through `stop`,
        then KILL any that ignore it. Safe to call when nothing is running.
        :param job_dir: str
        :param grace: int; seconds between TERM and KILL
            (default app.conf CONTAINER_KILL_GRACE, else 30)
        :return: List[str]; ids of the containers that were stopped
        """
        if grace is None:
            grace = app.conf.get("CONTAINER_KILL_GRACE", 30)
        grace = int(grace)
        containers = self.find_job_containers(job_dir)
        if not containers:
            return []
        ids = [c["Id"] for c in containers]
        desc = ", ".join(
            f"{c.get('Name', '').lstrip('/') or c['Id'][:12]} "
            f"({(c.get('Config') or {}).get('Image', '?')})"
            for c in containers
        )
        logger.warning(
            f"Stopping {len(ids)} container(s) still running for {job_dir}: {desc}"
        )
        # -t: docker deprecated --time for --timeout; both engines take -t
        self.run_cli(["stop", "-t", grace] + ids, timeout=grace + CLI_TIMEOUT)
        survivors = [c["Id"] for c in self.find_job_containers(job_dir)]
        if survivors:
            logger.warning(
                f"Killing {len(survivors)} container(s) that survived stop: "
                f"{' '.join(i[:12] for i in survivors)}"
            )
            self.run_cli(["kill"] + survivors)
        return ids

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
        labels=None,
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
        :param labels: None/dict; labels to stamp on the container
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
            "labels": {str(k): str(v) for k, v in (labels or {}).items()},
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
            # Expand environment variables in runtime option values
            if isinstance(v, str):
                v = os.path.expandvars(v)
            params["runtime_options"][k] = v
        return params

    def get_architecture_url(self, image_url, metadata_urls=None):
        """
        Get architecture-specific URL for container image.
        
        :param image_url: Legacy single URL (backwards compatible)
        :param metadata_urls: Dict of architecture-specific URLs or JSON string
        :return: Appropriate URL for current architecture
        """
        arch_mappings = app.conf.get("CONTAINER_ARCHITECTURE_MAPPINGS", {
            "x86_64": "",
            "amd64": "",
            "arm64": "-arm64",
            "aarch64": "-arm64",
        })
        
        current_arch = platform.machine().lower()
        
        if metadata_urls:
            if isinstance(metadata_urls, str):
                try:
                    metadata_urls = json.loads(metadata_urls)
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Failed to parse metadata_urls JSON: {e}")
                    return image_url
            
            if isinstance(metadata_urls, dict):
                if current_arch in metadata_urls:
                    logger.info(f"Found architecture-specific URL for {current_arch}")
                    return metadata_urls[current_arch]
        
        return image_url

    def ensure_image_loaded(self, image_name, image_url, cache_dir, metadata_urls=None):
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

            # Get architecture-specific URL if available
            arch_specific_url = self.get_architecture_url(image_url, metadata_urls)
            
            # pull image from url
            if arch_specific_url is not None:
                image_file = os.path.join(cache_dir, os.path.basename(arch_specific_url))
                if not os.path.exists(image_file):
                    logger.info(
                        f"Downloading image {image_file} ({image_name}) from {arch_specific_url}"
                    )
                    try:
                        osaka.main.get(arch_specific_url, image_file)
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to download image {arch_specific_url}:\n{str(e)}"
                        )
                    logger.info(
                        f"Downloaded image {image_file} ({image_name}) from {arch_specific_url}"
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
