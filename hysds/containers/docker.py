import backoff
from subprocess import check_output, Popen, PIPE

from .base import Base


class Docker(Base):
    IMAGE_LOAD_TIME_MAX = 60

    @staticmethod
    @backoff.on_exception(backoff.expo, Exception, max_time=IMAGE_LOAD_TIME_MAX)
    def inspect_image(image):
        """
        inspect the container image; ex. docker inspect <image>
        :param image: str
        :return: byte str
        """
        return check_output(["docker", "inspect", image])

    @staticmethod
    def pull_image(image):
        """
        run "docker pull <image>" command
        :param image: str; docker image name
        """
        return check_output(["docker", "pull", image])

    @staticmethod
    def tag_image(registry_url, image):
        """
        run "docker tag <image>" command
        :param registry_url;
        :param image: str; docker image name
        """
        return check_output(["docker", "tag", registry_url, image])

    def create_base_cmd(self, params):
        """Parse docker params and build base docker command line list."""

        docker_cmd_base = [
            "docker",
            "run",
            "--init",
            "--rm",
            "-u",
            "%s:%s" % (params["uid"], params["gid"]),
        ]

        # add runtime options
        for k, v in params["runtime_options"].items():
            docker_cmd_base.extend(["--{}".format(k), v])

        # add volumes
        for k, v in params["volumes"]:
            docker_cmd_base.extend(["-v", "%s:%s" % (k, v)])

        # set work directory and image
        docker_cmd_base.extend(["-w", params["working_dir"], params["image_name"]])

        return docker_cmd_base

    @classmethod
    def create_container_params(cls, image_name, image_url, image_mappings, root_work_dir, job_dir,
                                runtime_options=None):
        """
        Builds docker params
        :param image_name:
        :param image_url:
        :param image_mappings:
        :param root_work_dir:
        :param job_dir:
        :param runtime_options:
        :return:
        """
        params = super().create_container_params(image_name, image_url, image_mappings, root_work_dir, job_dir,
                                                 runtime_options)
        docker_sock = "/var/run/docker.sock"
        params['volumes'].append((docker_sock, docker_sock))
        return params
