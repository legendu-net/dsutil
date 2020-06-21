from typing import Union, List, Set, Tuple, Dict, Iterable, Callable
import tempfile
from pathlib import Path
import itertools as it
import time
from timeit import default_timer as timer
import datetime
import subprocess as sp
from loguru import logger
import git
from collections import deque
import pandas as pd


def run_cmd(cmd, shell: bool = False, check: bool = False) -> None:
    """Run a (Docker) command.
    :param cmd: The command to run.
    :param shell: Whether to run the command as a shell subprocess.
    :param check: Whether to check for errors. 
        If so, exceptions are thrown on errors.
    """
    msg = " ".join(str(elem) for elem in cmd) if isinstance(cmd, list) else cmd
    logger.debug("Running command: {}", msg)
    sp.run(cmd, shell=shell, check=check)


def tag_date(tag: str) -> str:
    """Get the current date as a 6-digit string.
    :return: The current in 6-digit format.
    """
    mmddhh = datetime.datetime.now().strftime("%m%d%H")
    return mmddhh if tag in ("", "latest") else f"{tag}_{mmddhh}"


def _push_image_timing(image: str) -> Tuple[str, float]:
    """Push a Docker image to Docker Hub and time the pushing.
    :param image: The full name of the image to push to Docker Hub.
    :return: The time (in seconds) used to push the Docker image.
    """
    start = timer()
    run_cmd(["docker", "push", image])
    end = timer()
    return image, end - start


def push_image(image: str, retry: int = 3, seconds: float = 60) -> Tuple[str, float]:
    """Push a Docker image to Docker Hub. Automatically retry pushing once it fails.
    :param image: The full name of the image to push to Docker Hub.
    :param retry: The total number of times to retry.
    :param seconds: The number of seconds to wait before retrying.
    :return: The time (in seconds) used to push the Docker image.
    """
    if retry <= 1:
        return _push_image_timing(image)
    for _ in range(retry):
        try:
            return _push_image_timing(image)
        except sp.CalledProcessError:
            time.sleep(seconds)


def _pull_image_timing(image: str) -> Tuple[str, float]:
    start = timer()
    run_cmd(["docker", "pull", image])
    end = timer()
    return image, end - start


def pull_image(image: str, retry: int = 3, seconds: float = 60) -> Tuple[str, float]:
    """Pull a Docker image from Docker Hub. Automatically retry pulling once.
    :param image: The full name of the image to push from Docker Hub.
    :return: The time (in seconds) to wait before retrying.
    """
    logger.info("\n\nPulling the Docker image {}...", image)
    if retry <= 1:
        return _pull_image_timing(image)
    for _ in range(retry):
        try:
            return _pull_image_timing(image)
        except sp.CalledProcessError:
            time.sleep(seconds)


class DockerImage:
    DOCKERFILE = "Dockerfile"

    @classmethod
    def git_url(cls, docker_image) -> str:  # pylint: disable=E0202
        if docker_image.startswith("dclong/"):
            docker_image = docker_image.replace("dclong/", "docker-")
            return f"https://github.com/dclong/{docker_image}.git"
        return ""

    def __init__(
        self,
        name: str,
        path: Path = None,
        git_url_mapping: Union[Dict[str, str], Callable] = DockerImage.git_url,
        branch: str = "dev"
    ):
        """Initialize a DockerImage object.

        :param name: Name of the Docker image, e.g., "dclong/jupyterhub-ds".
        :param path: The path to a local directory containing a local copy of the Git repository.
        :param git_url: A dictionary or callable function to map the Docker image name to its Git repo URL.
        :param branch: The branch of the GitHub repository to use.
        """
        self.name = name
        self.path = path
        self.git_url_mapping = git_url_mapping
        self.git_url = self._get_git_url()
        self.branch = branch
        self.is_root = False
        self.tag_build = None

    def get_deps(self, images: Dict[str, DockerImage]) -> Dict[str, DockerImage]:
        deps = deque()
        obj = self
        while obj.git_url not in images:
            if obj.git_url:
                deps.appendleft(obj)
                name, _ = self.base_image()
                obj = DockerImage(
                    name=name, git_url_mapping=self.git_url_mapping, branch=self.branch
                )
            else:
                deps[0].is_root = True
                break
        for dep in deps:
            images[dep.git_url] = dep
        return images

    def clone_repo(self) -> Path:
        if not self.git_url or self.path:
            return None
        self.path = Path(tempfile.mkdtemp())
        repo = git.Repo.clone_from(self.git_url, self.path)
        for rb in repo.remote().fetch():
            if rb.name.split("/")[1] == self.branch:
                repo.git.checkout(self.branch)

    def _get_git_url(self):
        if isinstance(self.git_url_mapping, dict):
            return self.git_url_mapping.get(self.name, DockerImage.git_url(self.name))
        return self.git_url_mapping(self.name)

    def build(self,
              tag_build: str,
              tag_base: str = "",
              no_cache: bool = False) -> Tuple[str, float]:
        self.clone_repo()
        if self.is_root:
            pull_image(":".join(self.base_image()))
        logger.info("\n\nBuilding the Docker image {}...", self.name)
        self._update_base_tag(tag_build, tag_base)
        cmd = ["docker", "build", "-t", f"{self.name}:{tag_build}", str(self.path)]
        if no_cache:
            cmd.append("--no-cache")
        run_cmd(cmd, check=True)
        self.tag_build = tag_build

    def _update_base_tag(self, tag_build: str, tag_base: str) -> None:
        tag = tag_base if self.is_root else tag_build
        dockerfile = self.path / DockerImage.DOCKERFILE
        with dockerfile.open() as fin:
            lines = fin.readlines()
        for idx, line in enumerate(lines):
            if line.startswith("FROM "):
                lines[idx] = line[:line.rfind(":")] + f":{tag}\n"
                break
        with dockerfile.open("w") as fout:
            fout.writelines(lines)

    def base_image(self) -> List[str, str]:
        """Get the name of the base image (of this Docker image).
        """
        dockerfile = self.path / DockerImage.DOCKERFILE
        with dockerfile.open() as fin:
            for line in fin:
                if line.startswith("FROM "):
                    line = line[5:].strip()
                    if ":" in line:
                        return line.split(":")
                    return [line, "latest"]
            raise LookupError("The FROM line is not found in the Dockerfile!")

    def push(
        self,
        tag_tran_fun: Callable = tag_date,
        retry: int = 3,
        seconds: float = 60
    ) -> pd.DataFrame:
        image = f"{self.name}:{self.tag_build}"
        data = [push_image(image=image, retry=retry, seconds=seconds)]
        tag_new = tag_tran_fun(self.tag_build)
        if tag_new != self.tag_build:
            image_new = f"{self.name}:{tag_new}"
            run_cmd(["docker", "tag", image, image_new])
            data.append(push_image(image=image_new, retry=retry, seconds=seconds))
        return pd.DataFrame(data, columns=["image", "seconds"])


class DockerImageBuilder:
    def __init__(
        self,
        names: Iterable[str],
        paths: Iterable[Path] = it.repeat(None),
        git_url_mapping: Union[Dict[str, str], Callable] = DockerImage.git_url,
        branch: str = ""
    ):
        self.docker_images = {}
        for name, path in zip(names, paths):
            DockerImage(
                name=name, path=path, git_url_mapping=git_url_mapping, branch=branch
            ).get_deps(self.docker_images)

    def push(self, tag_tran_fun: Callable = tag_date) -> pd.DataFrame:
        """Push all Docker images in self.docker_images.
        :param tag_tran_fun: A function takeing a tag as the parameter 
            and generating a new tag to tag Docker images before pushing.
        """
        frames = [image.push(tag_tran_fun) for _, image in self.docker_images]
        return pd.concat(frames)

    def build(
        self,
        tag_build: str = "next",
        tag_base: str = "",
        no_cache: Set[str] = set()
    ) -> Tuple[str, float]:
        """Build all Docker images in self.docker_images in order.
        :param tag_build: The tag of built images.
        :param tag_base: The tag to the root base image to use.
        :param no_cache: A set of docker images to disable cache when building.
        """
        frames = [image.build(tag_base, tag_build) for _, image in self.docker_images]
        return pd.concat(frames)
