import backoff
from subprocess import check_output, Popen, PIPE

from hysds.containers.base import Base
from hysds.celery import app


class Podman(Base):
    def __init__(self):
        super().__init__()
        self.podman_sock = f"/run/user/{self._uid}/podman/podman.sock"

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
            "-u",
            "%s:%s" % (params["uid"], params["gid"]),
            "--userns=keep-id",
            "--security-opt",
            "label=disable",
            f"--passwd-entry={params['user_name']}:*:{params['uid']}:{params['gid']}::{app.conf.get('VERDI_HOME')}:{app.conf.get('VERDI_SHELL')}"
        ])

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
                                runtime_options=None):
        """
        Builds podman params
        :param image_name:
        :param image_url:
        :param image_mappings: The volume mounts
        :param root_work_dir:
        :param job_dir:
        :param runtime_options: THe specific flags to run with
        :return:
        """
        params = super().create_container_params(image_name, image_url, image_mappings, root_work_dir, job_dir,
                                                 runtime_options)
        params['podman_sock'] = self.podman_sock
        params['volumes'].insert(0, (self.podman_sock, self.podman_sock, ))
        return params
