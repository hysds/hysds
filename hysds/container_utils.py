from future import standard_library

standard_library.install_aliases()
import json
import os
import shutil
import sys
from datetime import datetime
from subprocess import PIPE, Popen, check_output
from tempfile import mkdtemp

import backoff
import osaka.main
from atomicwrites import atomic_write

from hysds.celery import app
from hysds.log_utils import logger
from hysds.utils import datetime_iso_naive

# max time to wait for image to load
IMAGE_LOAD_TIME_MAX = 600


def has_fully_qualified_registry(image_ref):
    """
    Determine if image reference includes a fully qualified registry path.

    A Docker image reference has a fully qualified registry path if the first
    component (before the first slash) contains a dot (.) or colon (:).
    However, if there's no slash at all, a colon indicates a tag, not a registry.

    Examples:
        - docker.io/ubuntu:latest -> True (has dot in 'docker.io')
        - ghcr.io/user/image:v1 -> True (has dot in 'ghcr.io')
        - localhost:5000/myimage:v1 -> True (has colon in 'localhost:5000')
        - 123456.dkr.ecr.us-west-2.amazonaws.com/app -> True (has dots)
        - ubuntu:latest -> False (colon is for tag, not registry)
        - myimage -> False (no registry indicator)

    Args:
        image_ref: Docker image reference string

    Returns:
        bool: True if registry path is present, False otherwise
    """
    if not image_ref:
        return False

    # Check if there's a slash in the reference
    if '/' not in image_ref:
        # No slash means it's a simple image name (possibly with tag)
        # In this case, any colon is for the tag, not a registry
        # Only a dot would indicate a registry (but that's rare without a slash)
        # For practical purposes, no slash = no registry
        return False

    # Get the first component (before first slash)
    first_component = image_ref.split('/')[0]

    # Registry path indicated by dot (domain) or colon (port)
    return '.' in first_component or ':' in first_component


def verify_docker_mount(m, blacklist=app.conf.WORKER_MOUNT_BLACKLIST):
    """Verify host mount."""

    if m == "/":
        raise RuntimeError("Cannot mount host root directory")
    for k in blacklist:
        if m.startswith(k):
            raise RuntimeError(f"Cannot mount {m}: {k} is blacklisted")
    return True


def copy_mount(path, mnt_dir):
    """Copy path to a directory to be used for mounting into container. Return this path."""

    if not os.path.exists(mnt_dir):
        os.makedirs(mnt_dir, 0o777)
    mnt_path = os.path.join(mnt_dir, os.path.basename(path))
    if os.path.isdir(path):
        shutil.copytree(path, mnt_path)
    else:
        shutil.copy(path, mnt_path)
    logger.info(f"Copied container mount {path} to {mnt_path}.")
    return os.path.join(mnt_dir, os.path.basename(path))


def get_docker_params(
    image_name, image_url, image_mappings, root_work_dir, job_dir, runtime_options=None
):
    """
    Build docker params.
    :param image_name: str
    :param image_url: str
    :param image_mappings: dict
    :param root_work_dir: str
    :param job_dir: str
    :param runtime_options: None/dict
    :return:
    """
    # get dirs to mount
    root_jobs_dir = os.path.join(root_work_dir, "jobs")
    root_tasks_dir = os.path.join(root_work_dir, "tasks")
    root_workers_dir = os.path.join(root_work_dir, "workers")
    root_cache_dir = os.path.join(root_work_dir, "cache")

    # docker params dict
    params = {
        "image_name": image_name,
        "image_url": image_url,
        "uid": os.getuid(),
        "gid": os.getgid(),
        "working_dir": job_dir,
        "volumes": [
            ("/var/run/docker.sock", "/var/run/docker.sock"),
            (root_jobs_dir, root_jobs_dir),
            (root_tasks_dir, root_tasks_dir),
            (root_workers_dir, root_workers_dir),
            (root_cache_dir, f"{root_cache_dir}:ro"),
        ],
    }

    # add default image mappings
    celery_cfg_file = os.environ.get("HYSDS_CELERY_CFG", app.conf.__file__)
    if celery_cfg_file not in image_mappings and "celeryconfig.py" not in list(
        image_mappings.values()
    ):
        image_mappings[celery_cfg_file] = "celeryconfig.py"
    dsets_cfg_file = os.environ.get(
        "HYSDS_DATASETS_CFG",
        os.path.normpath(
            os.path.join(os.path.dirname(sys.executable), "..", "etc", "datasets.json")
        ),
    )
    if dsets_cfg_file not in image_mappings and "datasets.json" not in list(
        image_mappings.values()
    ):
        image_mappings[dsets_cfg_file] = "datasets.json"

    # if running on k8s add hosts and resolv.conf; create mount directory
    blacklist = app.conf.WORKER_MOUNT_BLACKLIST
    mnt_dir = None
    on_k8s = int(app.conf.get("K8S", 0))
    if on_k8s:
        for f in ("/etc/hosts", "/etc/resolv.conf"):
            if f not in image_mappings and f not in list(image_mappings.values()):
                image_mappings[f] = f
        blacklist = [i for i in blacklist if i != "/etc"]
        mnt_dir = mkdtemp(prefix=".container_mounts-", dir=job_dir)

    # add user-defined image mappings
    for k, v in list(image_mappings.items()):
        k = os.path.expandvars(k)
        verify_docker_mount(k, blacklist)
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
            k = copy_mount(k, mnt_dir)
        params["volumes"].append((k, f"{mnt}:{mode}"))

    # add runtime resources
    params["runtime_options"] = dict()
    if runtime_options is None:
        runtime_options = dict()
    for k, v in list(runtime_options.items()):
        # validate we have GPUs
        if k == "gpus" and int(os.environ.get("HYSDS_GPU_AVAILABLE", 0)) == 0:
            logger.warning(
                "Job specified runtime option 'gpus' but no GPUs were detected. Skipping this option"
            )
            continue
        # Expand environment variables in runtime option values
        if isinstance(v, str):
            v = os.path.expandvars(v)
        params["runtime_options"][k] = v

    return params


@backoff.on_exception(backoff.expo, Exception, max_time=IMAGE_LOAD_TIME_MAX)
def inspect_image(image):
    return check_output(["docker", "inspect", image])


def ensure_image_loaded(image_name, image_url, cache_dir):
    """Pull docker image into local repo."""

    # check if image is in local docker repo
    try:
        registry = app.conf.get("CONTAINER_REGISTRY", None)
        # Custom edit to load image from registry
        try:
            if registry is not None and not has_fully_qualified_registry(image_name):
                # Only prepend local registry if image doesn't already have a fully qualified registry
                logger.info(
                    f"Trying to load docker image {image_name} from registry '{registry}'"
                )
                registry_url = os.path.join(registry, image_name)
                logger.info(f"docker pull {registry_url}")
                check_output(["docker", "pull", registry_url])
                logger.info(f"docker tag {registry_url} {image_name}")
                check_output(["docker", "tag", registry_url, image_name])
            elif has_fully_qualified_registry(image_name):
                # Image already has a fully qualified registry path, use it directly
                logger.info(
                    f"Image {image_name} has fully qualified registry path, skipping local registry"
                )
        except Exception as e:
            logger.warning(
                f"Unable to load docker image from registry '{registry}': {e}"
            )

        image_info = check_output(["docker", "inspect", image_name])
        logger.info(f"Docker image {image_name} cached in repo")
    except:
        logger.info(f"Failed to inspect docker image {image_name}")

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
                p = Popen(
                    ["docker", "load", "-i", image_file], stderr=PIPE, stdout=PIPE
                )
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
                    inspect_image(image_name)
                else:
                    raise
        else:
            # pull image from docker hub
            logger.info(f"Pulling image {image_name} from docker hub")
            check_output(["docker", "pull", image_name])
            logger.info(f"Pulled image {image_name} from docker hub")
        image_info = check_output(["docker", "inspect", image_name])
    logger.info(f"image info for {image_name}: {image_info.decode()}")
    return json.loads(image_info)[0]


def get_base_docker_cmd(params):
    """Parse docker params and build base docker command line list."""

    # build command
    docker_cmd_base = [
        "docker",
        "run",
        "--init",
        "--rm",
        "-u",
        f"{params['uid']}:{params['gid']}",
    ]

    # add runtime options
    for k, v in params["runtime_options"].items():
        docker_cmd_base.extend([f"--{k}", v])

    # add volumes
    for k, v in params["volumes"]:
        docker_cmd_base.extend(["-v", f"{k}:{v}"])

    # set work directory and image
    docker_cmd_base.extend(["-w", params["working_dir"], params["image_name"]])

    return docker_cmd_base


def get_docker_cmd(params, cmd_line_list):
    """Pull docker image into local repo and add call to docker in the
    command line list."""

    # build command
    docker_cmd = get_base_docker_cmd(params)

    # set command
    docker_cmd.extend([str(i) for i in cmd_line_list])

    return docker_cmd
