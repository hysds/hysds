import backoff
from subprocess import check_output, Popen, PIPE

from hysds.containers.base import Base
from hysds.celery import app


class Podman(Base):
    @staticmethod
    def inspect_image(image):
        """
        inspect the container image; ex. podman inspect <image>
        :param image: str
        :return: byte str
        """
        return check_output(["podman", "inspect", image])

    @classmethod
    @backoff.on_exception(backoff.expo, Exception, max_time=Base.IMAGE_LOAD_TIME_MAX)
    def inspect_image_with_backoff(cls, image):
        """
        inspect the container image; ex. podman inspect <image>
        :param image: str
        :return: byte str
        """
        return cls.inspect_image(image)

    @staticmethod
    def pull_image(image):
        """
        run "podman pull <image>" command
        :param image: str; podman image name
        """
        return check_output(["podman", "pull", image])

    @staticmethod
    def tag_image(registry_url, image):
        """
        run "podman tag <image>" command
        :param registry_url;
        :param image: str; podman image name
        """
        return check_output(["podman", "tag", registry_url, image])

    @staticmethod
    def load_image(image_file):
        """
        Loads image into the container engine, ex. "podman load -i <image_file>"
        :param image_file: str, file location of podman image
        :return: Popen object: https://docs.python.org/3/library/subprocess.html#popen-objects
        """
        return Popen(["podman", "load", "-i", image_file], stderr=PIPE, stdout=PIPE)

    @classmethod
    def create_base_cmd(cls, params):
        """
        Parse podman params and build base podman command line list
        params input must have "uid" and "gid" key
        :param params: Dict[str, any]
        :return: List[str]
        """

        podman_cmd_base = [
            "podman",
            "--remote",
            "--url",
            f"unix:{params['podman_sock']}",
            "run",
            "--init",
            "--rm",
            "-u",
            "%s:%s" % (params["uid"], params["gid"]),
            "--userns=keep-id",
            "--security-opt",
            "label=disable",
            f"--passwd-entry={params['user_name']:*:{params['uid']}:{params['gid']}::{app.get('VERDI_HOME')}:{app.get('VERDI_SHELL')}}"
        ]

        # add runtime options
        for k, v in params["runtime_options"].items():
            podman_cmd_base.extend(["--{}".format(k), v])

        # add volumes
        for k, v in params["volumes"]:
            podman_cmd_base.extend(["-v", "%s:%s" % (k, v)])

        # set work directory and image
        podman_cmd_base.extend(["-w", params["working_dir"], params["image_name"]])

        return podman_cmd_base

    @classmethod
    def create_container_params(cls, image_name, image_url, image_mappings, root_work_dir, job_dir,
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
        podman_sock = f"/run/user/{params['uid']}/podman/podman.sock"
        params['podman_sock'] = podman_sock
        params['volumes'].insert(0, (podman_sock, podman_sock, ))
        return params
