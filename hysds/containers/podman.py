import backoff
import os
from subprocess import check_output, Popen, PIPE

from hysds.containers.base import Base
from hysds.celery import app
from hysds.log_utils import logger


class Podman(Base):
    def __init__(self):
        super().__init__()
        # Get the podman sock from the host environment. Otherwise, fallback to this default.
        self._uid = os.environ.get("HOST_UID", self._uid)
        self.podman_sock = f"/run/user/{self._uid}/podman/podman.sock"

        cfg = app.conf.get('PODMAN_CFG', {})
        self._environment = cfg.get("environment", {})
        self._set_uid_gid = cfg.get("set_uid_gid", False)
        self._set_passwd_entry = cfg.get("set_passwd_entry", False)
        self._verdi_home = app.conf.get('VERDI_HOME', '/home/ops')
        self._verdi_shell = app.conf.get('VERDI_SHELL', '/bin/bash')
        self._cmd_base = cfg.get("cmd_base", {})

    def set_passwd_entry(self, bool_value):
        self._set_passwd_entry = bool_value

    def __create_podman_socket_cmd(self):
        podman_socket_cmd = [
            "podman",
            "--remote",
            "--url",
            f"unix:{self.podman_sock}"
        ]
        return podman_socket_cmd

    def inspect_image(self, image):
        """
        inspect the container image; ex. podman inspect <image>
        :param image: str
        :return: byte str
        """
        cmd = self.__create_podman_socket_cmd()
        cmd.extend(["inspect", image])
        return check_output(cmd)

    @backoff.on_exception(backoff.expo, Exception, max_time=Base.IMAGE_LOAD_TIME_MAX)
    def inspect_image_with_backoff(self, image):
        """
        inspect the container image; ex. podman inspect <image>
        :param image: str
        :return: byte str
        """
        return self.inspect_image(image)

    def pull_image(self, image):
        """
        run "podman pull <image>" command
        :param image: str; podman image name
        """
        cmd = self.__create_podman_socket_cmd()
        cmd.extend(["pull", image])
        return check_output(cmd)

    def tag_image(self, registry_url, image):
        """
        run "podman tag <image>" command
        :param registry_url;
        :param image: str; podman image name
        """
        cmd = self.__create_podman_socket_cmd()
        cmd.extend(["tag", registry_url, image])
        return check_output(cmd)

    def load_image(self, image_file):
        """
        Loads image into the container engine, ex. "podman load -i <image_file>"
        :param image_file: str, file location of podman image
        :return: Popen object: https://docs.python.org/3/library/subprocess.html#popen-objects
        """
        cmd = self.__create_podman_socket_cmd()
        cmd.extend([ "load", "-i", image_file])
        return Popen(cmd, stderr=PIPE, stdout=PIPE)

    def create_base_cmd(self, params):
        """
        Parse podman params and build base podman command line list
        params input must have "uid" and "gid" key
        :param params: Dict[str, any]
        :return: List[str]
        """
        podman_cmd_base = self.__create_podman_socket_cmd()
        podman_cmd_base.extend([
            "run",
            "--init",
            "--rm",
        ])

        # Persist any environment variables defined in the celery config
        for env_var in self._environment:
            if env_var in os.environ:
                podman_cmd_base.append(f"-e {env_var}=${env_var}")
            else:
                logger.warning(f"{env_var} does not exist. Won't include in podman command.")
        # set the -u if set
        if self._set_uid_gid is True:
            podman_cmd_base.extend(["-u", f"{params['uid']}:{params['gid']}"])

        # set the --passwd-entry if set
        if self._set_passwd_entry:
            podman_cmd_base.append(f"--passwd-entry={os.environ.get('HOST_USER', 'ops')}:*:{params['uid']}:{params['gid']}::{self._verdi_home}:{self._verdi_shell}")

        # add some base runtime options as defined in the celeryconfig
        for k, v in self._cmd_base.items():
            if k == "userns":
                podman_cmd_base.append(f"--{k}={v}")
            else:
                podman_cmd_base.extend(["--{}".format(k), v])

        # add runtime options
        for k, v in params["runtime_options"].items():
            podman_cmd_base.extend(["--{}".format(k), v])

        # add volumes
        for k, v in params["volumes"]:
            podman_cmd_base.extend(["-v", "%s:%s" % (k, v)])

        # set work directory and image
        podman_cmd_base.extend(["-w", params["working_dir"], params["image_name"]])

        return podman_cmd_base

    def create_container_params(self, image_name, image_url, image_mappings, root_work_dir, job_dir,
                                runtime_options=None, verdi_home=None, host_verdi_home=None):
        """
        Builds podman params
        :param image_name:
        :param image_url:
        :param image_mappings: The volume mounts
        :param root_work_dir:
        :param job_dir:
        :param runtime_options: THe specific flags to run with
        :param verdi_home: The verdi home
        :param host_verdi_home: The home dir on the host
        :return:
        """
        params = super().create_container_params(image_name, image_url, image_mappings, root_work_dir, job_dir,
                                                 runtime_options, verdi_home, host_verdi_home)
        params['podman_sock'] = self.podman_sock
        params['volumes'].insert(0, (self.podman_sock, self.podman_sock, ))
        return params
