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
import backoff
import shutil
from datetime import datetime
from subprocess import check_output, Popen, PIPE
from atomicwrites import atomic_write
from tempfile import mkdtemp

from hysds.log_utils import logger
from hysds.celery import app

import osaka.main


# max time to wait for image to load
IMAGE_LOAD_TIME_MAX = 600

IMAGE_INFO_ = """
[
    {
        "Id": "singularity",
        "RepoTags": [
            "singularity"
        ],
        "RepoDigests": [
            "singularity"
        ],
        "Parent": "",
        "Comment": "",
        "Created": "",
        "Container": "",
        "ContainerConfig": {
            "Hostname": "",
            "Domainname": "",
            "User": "",
            "AttachStdin": false,
            "AttachStdout": false,
            "AttachStderr": false,
            "Tty": false,
            "OpenStdin": false,
            "StdinOnce": false,
            "Env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            ],
            "ArgsEscaped": true,
            "Image": "",
            "Volumes": null,
            "WorkingDir": "",
            "Entrypoint": null,
            "OnBuild": null,
            "Labels": {}
        },
        "DockerVersion": "singularity",
        "Author": "",
        "Config": {
            "Hostname": "",
            "Domainname": "",
            "User": "",
            "AttachStdin": false,
            "AttachStdout": false,
            "AttachStderr": false,
            "Tty": false,
            "OpenStdin": false,
            "StdinOnce": false,
            "Env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            ],
            "Cmd": [
                "/hello"
            ],
            "ArgsEscaped": true,
            "Image": "",
            "Volumes": null,
            "WorkingDir": "",
            "Entrypoint": null,
            "OnBuild": null,
            "Labels": null
        },
        "Architecture": "amd64",
        "Os": "linux",
        "Size": 0,
        "VirtualSize": 0,
        "GraphDriver": {
            "Data": {
                "DeviceId": "",
                "DeviceName": "",
                "DeviceSize": ""
            },
            "Name": "devicemapper"
        },
        "RootFS": {
            "Type": "layers",
            "Layers": [
                ""
            ]
        },
        "Metadata": {
            "LastTagTime": ""
        }
    }
]
"""


def verify_docker_mount(m, blacklist=app.conf.WORKER_MOUNT_BLACKLIST):
    """Verify host mount."""

    if m == "/":
        raise RuntimeError("Cannot mount host root directory")
    for k in blacklist:
        if m.startswith(k):
            raise RuntimeError("Cannot mount %s: %s is blacklisted" % (m, k))
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
    logger.info("Copied container mount {} to {}.".format(path, mnt_path))
    return os.path.join(mnt_dir, os.path.basename(path))


def get_docker_params(image_name, image_url, image_mappings, root_work_dir, job_dir, runtime_options=None):
    """Build docker params."""

    # get dirs to mount
    root_jobs_dir = os.path.join(root_work_dir, 'jobs')
    root_tasks_dir = os.path.join(root_work_dir, 'tasks')
    root_workers_dir = os.path.join(root_work_dir, 'workers')
    root_cache_dir = os.path.join(root_work_dir, 'cache')

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
            (root_cache_dir, "{}:ro".format(root_cache_dir)),
        ]
    }

    # add default image mappings
    celery_cfg_file = os.environ.get('HYSDS_CELERY_CFG', app.conf.__file__)
    if celery_cfg_file not in image_mappings and "celeryconfig.py" not in list(image_mappings.values()):
        image_mappings[celery_cfg_file] = "celeryconfig.py"
    dsets_cfg_file = os.environ.get('HYSDS_DATASETS_CFG',
                                    os.path.normpath(os.path.join(os.path.dirname(sys.executable),
                                                                  '..', 'etc', 'datasets.json')))
    if dsets_cfg_file not in image_mappings and "datasets.json" not in list(image_mappings.values()):
        image_mappings[dsets_cfg_file] = "datasets.json"

    # if running on k8s add hosts and resolv.conf; create mount directory
    blacklist = app.conf.WORKER_MOUNT_BLACKLIST
    mnt_dir = None
    on_k8s = int(app.conf.get('K8S', 0))
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
                raise RuntimeError("Invalid image mapping: %s:%s" % (k, v))
        if v.startswith('/'):
            mnt = v
        else:
            mnt = os.path.join(job_dir, v)
        if mnt_dir is not None:
            k = copy_mount(k, mnt_dir)
        params['volumes'].append((k, "%s:%s" % (mnt, mode)))

    # add runtime resources
    params['runtime_options'] = dict()
    if runtime_options is None:
        runtime_options = dict()
    for k, v in list(runtime_options.items()):
        # validate we have GPUs
        if k == "gpus" and int(os.environ.get("HYSDS_GPU_AVAILABLE", 0)) == 0:
            logger.warning("Job specified runtime option 'gpus' but no GPUs were detected. Skipping this option.")
            continue
        params['runtime_options'][k] = v

    return params


def get_singularity_params(image_name, image_url, image_mappings, root_work_dir, root_cache_dir, job_dir):
    """Build singularity params."""

    logger.info("XXXXXX in get_singularity_params() XXXXX")

    # get dirs to mount
    root_jobs_dir = os.path.join(root_work_dir, 'jobs')
    logger.info("root_jobs_dir: %s"%root_jobs_dir)
    root_tasks_dir = os.path.join(root_work_dir, 'tasks')
    if not os.path.exists(root_tasks_dir):
      os.makedirs(root_tasks_dir, 0o777)
    logger.info("root_tasks_dir: %s"%root_tasks_dir)
    root_workers_dir = os.path.join(root_work_dir, 'workers')
    logger.info("root_workers_dir: %s"%root_workers_dir)
    root_cache_dir = os.path.join(root_cache_dir, 'cache')
    logger.info("root_cache_dir: %s"%root_cache_dir)

    image_file_basename = os.path.basename(image_url)
    logger.info("image_file_basename: %s"%image_file_basename)

    ### sandbox_basename = image_file_basename.replace('.tar.gz', '').encode('ascii', 'ignore')
    sandbox_basename = image_file_basename.replace('.tar.gz', '')
    logger.info("sandbox_basename: %s"%sandbox_basename)
    ### sandbox_dir = os.path.join(root_cache_dir, sandbox_basename)
    sandbox_dir = os.environ.get('SANDBOX_DIR', '/nobackupp12/esi_sar/PGE/container-aria-jpl_ariamh_aria-446_singularity-2020-09-30-b3b9f362af00.simg')
    logger.info("sandbox_dir: %s"%sandbox_dir)
    dem_dir = os.environ.get('DEM_ROOT')
    logger.info("dem_dir: %s" % dem_dir)
    verdi_dir = os.environ.get('VERDI_ROOT')
    logger.info("verdi_dir: %s" % verdi_dir)
    home_dir = os.environ.get('HOME')
    logger.info("home_dir: %s" % home_dir)

    # how to avoid this hardcoded sandbox info?
    ### sand_box = "/data/work/cache/data/data/singularity/sandbox/container-hello_world_master-2019-03-21-c1a943f9577c.simg"

    # singularity params dict
    params = {
        "image_name": image_name,
        "image_url": image_url,
        "uid": os.getuid(),
        "gid": os.getgid(),
        "working_dir": job_dir,
        "sandbox_dir": sandbox_dir,
        "mount_dir": '/'+sandbox_basename,
        "volumes": [
            ( root_jobs_dir, root_jobs_dir ),
            ( root_tasks_dir, root_tasks_dir ),
            ( root_workers_dir, root_workers_dir ),
            ( root_cache_dir, "{}:ro".format(root_cache_dir) ),
            ( dem_dir, "{}:ro".format(dem_dir) )
        ]
    }

    # add default image mappings
    celery_cfg_file = os.environ.get('HYSDS_CELERY_CFG',
                                     os.path.join(os.path.dirname(app.conf.__file__),
                                                  "celeryconfig.py"))
    if celery_cfg_file not in image_mappings and "celeryconfig.py" not in image_mappings.values():
        image_mappings[celery_cfg_file] = "celeryconfig.py"
    logger.info("XXXXXX in get_singularity_params(), celery_cfg_file: {0} XXXXX".format(celery_cfg_file))
    logger.info("XXXXXX in get_singularity_params(), image_mappings[celery_cfg_file]: {0} XXXXX".format(image_mappings[celery_cfg_file]))

    dsets_cfg_file = os.environ.get('HYSDS_DATASETS_CFG',
                                    os.path.normpath(os.path.join(os.path.dirname(sys.executable),
                                                                  '..', 'etc', 'datasets.json')))
    if dsets_cfg_file not in image_mappings and "datasets.json" not in image_mappings.values():
        image_mappings[dsets_cfg_file] = "datasets.json"

    # if running on k8s add hosts and resolv.conf; create mount directory
    blacklist = app.conf.WORKER_MOUNT_BLACKLIST
    mnt_dir = None
    on_k8s = int(app.conf.get('K8S', 0))
    if on_k8s:
        for f in ("/etc/hosts", "/etc/resolv.conf"):
            if f not in image_mappings and f not in image_mappings.values():
                image_mappings[f] = f
        blacklist = [i for i in blacklist if i != "/etc"]
        mnt_dir = mkdtemp(prefix=".container_mounts-", dir=job_dir)

    # add user-defined image mappings
    logger.info("XXXXXX image_mappings: %s XXXXX" % json.dumps(image_mappings))
    ### for k, v in image_mappings.iteritems():
    for k, v in image_mappings.items():
        k = os.path.expandvars(k)
        verify_docker_mount(k, blacklist)
        mode = "ro"
        if isinstance(v, list):
            if len(v) > 1: v, mode = v[0:2]
            elif len(v) == 1: v = v[0]
            else: raise(RuntimeError("Invalid image mapping: %s:%s" % (k, v)))

        if '/home/ops/verdi/etc/settings.conf' in k:
          k = os.path.join(verdi_dir, 'etc/settings.conf')
        if '/home/ops/.aws' in k:
          k = os.path.join(verdi_dir, '.aws')
        if '/home/ops/.netrc' in k:
          k = os.path.join(home_dir, '.netrc')

        if v.startswith('/'): mnt = v
        else: # copy to job_dir
          mnt = '/'+v
          shutil.copyfile(k, os.path.join(job_dir, v))
        ### else: mnt = os.path.join(job_dir, v)

        if mnt_dir is not None: k = copy_mount(k, mnt_dir)

        params['volumes'].append(( k, "%s:%s" % (mnt, mode) ))
        ### params['volumes'].append((k, mnt))

    return params



@backoff.on_exception(backoff.expo, Exception, max_time=IMAGE_LOAD_TIME_MAX)
def inspect_image(image):
    return check_output(['docker', 'inspect', image])


def ensure_image_loaded(image_name, image_url, cache_dir):
    """Pull docker image into local repo."""

    logger.info("XXXXXX in ensure_image_loaded() XXXXX")
    logger.info("image_name: %s" % image_name)
    logger.info("image_url: %s" % image_url)
    logger.info("cache_dir: %s" % cache_dir)

    # check if image is in local docker repo
    try:
        ### image_info = check_output(['docker', 'inspect', image_name])
        raise ValueError('XXXXXX test pulling image from url XXXX')
        logger.info("Docker image %s cached in repo" % image_name)

        """
        registry = app.conf.get("CONTAINER_REGISTRY", None)
        # Custom edit to load image from registry
        try:
            if registry is not None:
                logger.info(
                    "Trying to load docker image {} from registry '{}'".format(
                        image_name, registry))
                registry_url = os.path.join(registry, image_name)
                logger.info("docker pull {}".format(registry_url))
                check_output(['docker', 'pull', registry_url])
                logger.info("docker tag {} {}".format(registry_url, image_name))
                check_output(['docker', 'tag', registry_url, image_name])
        except Exception as e:
            logger.warn(
                "Unable to load docker image from registry '{}': {}".format(
                    registry, e))

        image_info = check_output(['docker', 'inspect', image_name])
        logger.info("Docker image %s cached in repo" % image_name)
        """
    except:
        ### logger.info("Failed to inspect docker image %s" % image_name)
        logger.info("Docker image %s is not used on NASA Pleiades" % image_name)

        # pull image from url
        if image_url is not None:
            image_file = os.path.join(cache_dir, os.path.basename(image_url))
            if not os.path.exists(image_file):
                logger.info("Downloading image %s (%s) from %s" %
                            (image_file, image_name, image_url))
                try:
                    osaka.main.get(image_url, image_file)
                except Exception as e:
                    raise RuntimeError("Failed to download image {}:\n{}".format(image_url, str(e)))
                logger.info("Downloaded image %s (%s) from %s" %
                            (image_file, image_name, image_url))
            # for singularity, do not run docker load
            """
            load_lock = "{}.load.lock".format(image_file)
            try:
                with atomic_write(load_lock) as f:
                    f.write("%sZ\n" % datetime.utcnow().isoformat())
                logger.info("Loading image %s (%s)" % (image_file, image_name))
                p = Popen(['docker', 'load', '-i', image_file],
                          stderr=PIPE, stdout=PIPE)
                stdout, stderr = p.communicate()
                if p.returncode != 0:
                    raise RuntimeError("Failed to load image {} ({}): {}".format(image_file, image_name, stderr.decode()))
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
                    logger.info("Waiting for image %s (%s) to load" %
                                (image_file, image_name))
                    inspect_image(image_name)
                else:
                    raise
            """

            # if singularity, unzip the tar ball into the cache dir
            ### is_singularity = True
            is_singularity = False # do not unzip cause it is too expensive on pleiades lustre
            if is_singularity:
                logger.info("is_singularity: True, image_file: %s" % image_file)
                # if sandbox_dir does not already exist, untar the sandbox tarball
                sandbox_dir = image_file.replace('.tar.gz', '')
                if not os.path.exists(sandbox_dir) or not os.path.isdir(sandbox_dir):
                  p = Popen(['tar', '--force-local', '-xvf', image_file], cwd=cache_dir, stderr=PIPE, stdout=PIPE)
                  stdout, stderr = p.communicate()
                  logger.info("stdout: %s" % stdout)
                  logger.info("stderr: %s" % stderr)
                  if p.returncode != 0:
                      raise(RuntimeError("Failed to unzip image tar %s (%s): %s" % (image_file, image_name, stderr)))
                  logger.info("unzipped image tar %s (%s)" % (image_file, image_name))
                else:
                  logger.info("sandbox tar ball already unzipped here %s " % sandbox_dir)
        else:
            # pull image from docker hub
            """
            logger.info("Pulling image %s from docker hub" % image_name)
            check_output(['docker', 'pull', image_name])
            logger.info("Pulled image %s from docker hub" % image_name)
            """
        ### image_info = check_output(['docker', 'inspect', image_name])

    image_info = IMAGE_INFO_

    ### logger.info("image info for %s: %s" % (image_name, image_info.decode()))
    return json.loads(image_info)[0]


def get_base_docker_cmd(params):
    """Parse docker params and build base docker command line list."""

    # build command
    docker_cmd_base = ["docker", "run", "--init", "--rm", "-u",
                       "%s:%s" % (params['uid'], params['gid'])]

    # add runtime options
    for k, v in params['runtime_options'].items():
        docker_cmd_base.extend(["--{}".format(k), v])

    # add volumes
    for k, v in params['volumes']:
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


def get_base_singularity_cmd(params):
    """Parse singularity params and build base singularity command line list."""

    # build command
    ### singularity_cmd_base = [ "/nasa/singularity/3.2.0/bin/singularity", "exec", "--no-home", "--home", "/home/ops" ]
    # try the latest version 3.5
    ### singularity_cmd_base = [ "/nasa/singularity/3.5.3/bin/singularity", "exec", "--userns", "--no-home", "--home", "/home/ops" ]
    # try the latest version 3.6.3
    ### singularity_cmd_base = [ "/nasa/singularity/3.6.3/bin/singularity", "exec", "--userns", "--no-home", "--home", "/home/ops" ]
    # try the latest version 3.6.4
    ### singularity_cmd_base = [ "/nasa/singularity/3.6.4/bin/singularity", "exec", "--userns", "--no-home", "--home", "/home/ops" ]
    # use module load singularity to get singularity without worrying about its installation details
    singularity_cmd_base = [ "singularity", "exec", "--userns", "--no-home", "--home", "/home/ops" ]

    # add volumes
    for k, v in params['volumes']:
      logger.info("XXXXXX k: %s" % k)
      ### if os.path.isfile(k):
      if os.path.exists(k):
        singularity_cmd_base.extend(["--bind", "%s:%s" % (k, v)])

    # bind work dir so --pwd work dir can work
    singularity_cmd_base.extend(["--bind", "%s:%s" % (params['working_dir'], params['working_dir'])])

    # set work directory and image
    ### docker_cmd_base.extend(["-w", params['working_dir'], params['image_name']])
    ### singularity_cmd_base.extend(["--pwd", params['mount_dir'], params['sandbox_dir']])
    singularity_cmd_base.extend(["--pwd", params['working_dir'], params['sandbox_dir']])

    return singularity_cmd_base



def get_singularity_cmd(params, cmd_line_list):
    """
    sample singularity command line:
    singularity_cmd = ["/nasa/singularity/3.2.0/bin/singularity", "exec", "--no-home", "--home", "/home/ops", "--bind", "/nobackupp14/l
2019-06-19-82a52bf2bb3b.simg:/container-hello_world_master-2019-06-19-82a52bf2bb3b.simg", "--pwd", "/container-hello_world_master-2019-
/work/cache/container-hello_world_master-2019-06-19-82a52bf2bb3b.simg", "/home/ops/verdi/ops/hello_world/run_hello_world.sh"]
    """

    """Pull docker image into local repo and add call to docker in the
       command line list."""

    # build command
    singularity_cmd = get_base_singularity_cmd(params)

    # set command
    singularity_cmd.extend([str(i) for i in cmd_line_list])
    logger.info("XXXXXX singularity_cmd: %s" % singularity_cmd)

    ### singularity_cmd = ["/nasa/singularity/3.2.0/bin/singularity", "exec", "--no-home", "--home", "/home/ops", "--bind", "/nobackupp ter-2019-07-24-b269614f8b4e.simg:/container-hello_world_master-2019-07-24-b269614f8b4e.simg", "--pwd", "/container-hello_world_master-2 lpan/work/cache/container-hello_world_master-2019-07-24-b269614f8b4e.simg", "/home/ops/verdi/ops/hello_world/run_hello_world.sh"]

    ### logger.info("XXXXXX hardcoded singularity_cmd: %s" % singularity_cmd)

    ### singularity exec --no-home --home /home/ops --bind /nobackupp14/lpan/work/cache/container-hello_world_master-2019-07-24-b269614 9-07-24-b269614f8b4e.simg --pwd /container-hello_world_master-2019-07-24-b269614f8b4e.simg /nobackupp14/lpan/work/cache/container-hello home/ops/verdi/ops/hello_world/run_hello_world.sh

    return singularity_cmd


