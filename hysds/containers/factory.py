# from .docker import Docker
from hysds.containers.docker import Docker


def container_engine_factory(engine=None):
    """
    Factory function to return the Container engine class (derived from hysds.containers.base.Base)
    :param engine: str; docker engine, ex. docker, podman, singularity, etc.
    :return: Docker, Podman, Singularity class
    """
    return Docker
