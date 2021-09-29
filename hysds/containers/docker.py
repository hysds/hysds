import os
import docker
import docker.errors

from hysds.containers.base import ContainerBase


class Docker(ContainerBase):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.client = docker.client.from_env()
