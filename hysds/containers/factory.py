# from .docker import Docker
from hysds.containers.docker import Docker


def container_engine_factory(engine):
    """
    Factory function to return the Container engine class (derived from hysds.containers.base.Base)
    :param engine: str,
    :return: Docker, Podman, Singularity class
    """
    return Docker()
