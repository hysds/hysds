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


from .base import Base


class Docker(Base):
    IMAGE_LOAD_TIME_MAX = 60

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        docker_sock = "/var/run/docker.sock"
        self.params['volumes'].insert(0, (docker_sock, docker_sock))

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
    def tag_image(image):
        """
        run "docker tag <image>" command
        :param image: str; docker image name
        """
        return check_output(["docker", "tag", image])

    def get_base_cmd(self):
        """Parse docker params and build base docker command line list."""

        # build command
        docker_cmd_base = [
            "docker",
            "run",
            "--init",
            "--rm",
            "-u",
            "%s:%s" % (self.params["uid"], self.params["gid"]),
        ]

        # add runtime options
        for k, v in self.params["runtime_options"].items():
            docker_cmd_base.extend(["--{}".format(k), v])

        # add volumes
        for k, v in self.params["volumes"]:
            docker_cmd_base.extend(["-v", "%s:%s" % (k, v)])

        # set work directory and image
        docker_cmd_base.extend(["-w", self.params["working_dir"], self.params["image_name"]])

        return docker_cmd_base
