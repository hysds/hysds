# from .docker import Docker
from hysds.containers.docker import Docker
from hysds.containers.podman import Podman


def container_engine_factory(engine=None):
    """
    Factory function to return the Container engine class (derived from hysds.containers.base.Base)
    :param engine: str; docker engine, ex. docker, podman, singularity, etc.
    :return: Docker, Podman, Singularity class
    """
    if engine == "docker":
        return Docker()
    elif engine == "podman":
        return Podman()
    else:
        raise ValueError(f"Unsupported container engine: {engine}")
