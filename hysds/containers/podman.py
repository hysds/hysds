import backoff
from subprocess import check_output,  Popen, PIPE

from hysds.containers.base import Base


class Podman(Base):
    """
    https://hysds-core.atlassian.net/wiki/spaces/HYS/pages/1972895745/Podman
    """

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
        # bug? change "podman load -i" to "podman load <" https://bugzilla.redhat.com/show_bug.cgi?id=1797599#c2
        # return Popen(["podman", "load", "-i", image_file], stderr=PIPE, stdout=PIPE)
        return Popen(["podman", "load", "<", image_file], stderr=PIPE, stdout=PIPE)

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
            "run",
            # "--privileged"
            "--init",
            "--userns=keep-id",
            "--rm",
            # "-u",
            # "%s:%s" % (params["uid"], params["gid"]),
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
        """
        podman run -it --privileged \
            -v /run/user/$UID:/run/user/$UID \
            -v $HOME/.local/share/containers:$HOME/.local/share/containers \
            -v $HOME/.local/share/containers:/var/lib/containers \
            podman:v3.3.1 bash
        """
        # docker_sock = "/var/run/docker.sock"
        # params['volumes'].insert(0, (docker_sock, docker_sock,))

        # TODO: look into mounting these volumes
        # user_volume = "/run/user/{}".format(params["uid"])
        # containers_volume = "$HOME/.local/share/containers"
        # params['volumes'].append((containers_volume, containers_volume, ))
        # params['volumes'].append((containers_volume, "/var/lib/containers", ))
        # params['volumes'].append((user_volume, user_volume, ))
        return params
