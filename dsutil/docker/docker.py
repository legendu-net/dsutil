"""Docker related utils.
"""
from __future__ import annotations
from typing import Union, List, Sequence, Set, Deque, Tuple, Dict, Iterable, Callable
import tempfile
from pathlib import Path
import time
from timeit import default_timer as timer
import datetime
import subprocess as sp
from collections import deque
import shutil
from loguru import logger
import git
import pandas as pd


def run_cmd(cmd: Union[str, List[str]], check: bool = False) -> None:
    """Run a (Docker) command.
    :param cmd: The command to run.
    :param shell: Whether to run the command as a shell subprocess.
    :param check: Whether to check for errors. 
        If so, exceptions are thrown on errors.
    """
    msg = " ".join(str(elem) for elem in cmd) if isinstance(cmd, list) else cmd
    logger.debug("Running command: {}", msg)
    sp.run(cmd, shell=isinstance(cmd, str), check=check)


def tag_date(tag: str) -> str:
    """Get the current date as a 6-digit string.
    :return: The current in 6-digit format.
    """
    mmddhh = datetime.datetime.now().strftime("%m%d%H")
    return mmddhh if tag in ("", "latest") else f"{tag}_{mmddhh}"


def _push_image_timing(image: str) -> Tuple[str, float, str]:
    """Push a Docker image to Docker Hub and time the pushing.
    :param image: The full name of the image to push to Docker Hub.
    :return: The time (in seconds) used to push the Docker image.
    """
    start = timer()
    run_cmd(["docker", "push", image])
    end = timer()
    return image, end - start, "push"


def push_image(image: str,
               retry: int = 3,
               seconds: float = 60) -> Tuple[str, float, str]:
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
    return _push_image_timing(image)


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
    logger.info("Pulling the Docker image {}...", image)
    if retry <= 1:
        return _pull_image_timing(image)
    for _ in range(retry):
        try:
            return _pull_image_timing(image)
        except sp.CalledProcessError:
            time.sleep(seconds)
    return _pull_image_timing(image)


class DockerImage:
    """Class representing a Docker Image.
    """
    DOCKERFILE = "Dockerfile"

    def __init__(
        self,
        git_url: str,
        branch: str = "dev",
    ):
        """Initialize a DockerImage object.

        :param git_url: URL of the remote Git repository.
        :param branch: The branch of the GitHub repository to use.
        """
        self.git_url = git_url.strip()
        self.branch = branch
        self.path = None
        self.name = ""
        self.base_image = ""
        self.git_url_base = ""
        self.is_root = False
        self.tag_build = None

    def clone_repo(self) -> None:
        """Clone the Git repository to a local directory.
        """
        if self.path:
            return
        self.path = Path(tempfile.mkdtemp())
        logger.info("Cloning {} into {}", self.git_url, self.path)
        repo = git.Repo.clone_from(self.git_url, self.path)
        for rb in repo.remote().fetch():
            if rb.name.split("/")[1] == self.branch:
                repo.git.checkout(self.branch)
        self._parse_dockerfile()

    def _parse_dockerfile(self):
        dockerfile = self.path / DockerImage.DOCKERFILE
        with dockerfile.open() as fin:
            for line in fin:
                if line.startswith("# NAME:"):
                    self.name = line[7:].strip()
                    logger.info("This image name: {}", self.name)
                elif line.startswith("FROM "):
                    self.base_image = line[5:].strip()
                    logger.info("Base image name: {}", self.base_image)
                elif line.startswith("# GIT:"):
                    self.git_url_base = line[6:].strip()
                    logger.info("Base image URL: {}", self.git_url_base)
        if not self.name:
            raise LookupError("The name tag '# NAME:' is not found in the Dockerfile!")
        if not self.base_image:
            raise LookupError("The FROM line is not found in the Dockerfile!")

    def get_deps(self, images: Dict[str, DockerImage]) -> Deque[DockerImage]:
        """Get all dependencies of this DockerImage in order.

        :param images: A dict containing dependency images.
        A key is an URL of a DockerImage and the value is the corresponding DockerImage.
        :return: A dict containing dependency images.
        A key is an URL of a DockerImage and the value is the corresponding DockerImage.
        """
        self.clone_repo()
        deps = deque([self])
        obj = self
        while obj.git_url_base not in images:
            if obj.git_url_base:
                obj = DockerImage(git_url=obj.git_url_base, branch=obj.branch)
                obj.clone_repo()
                deps.appendleft(obj)
            else:
                deps[0].is_root = True
                break
        return deps

    def _copy_ssh(self, copy_ssh_to: str):
        if copy_ssh_to:
            ssh_dst = self.path / copy_ssh_to
            try:
                shutil.rmtree(ssh_dst)
            except FileNotFoundError:
                pass
            shutil.copytree(Path.home() / ".ssh", ssh_dst)

    def build(
        self,
        tag_build: str = None,
        tag_base: str = "",
        no_cache: bool = False,
        copy_ssh_to: str = ""
    ) -> Tuple[str, float, str]:
        """Build the Docker image.

        :param tag_build: The tag of the Docker image to build.
        If None (default), then it is determined by the branch name.
        When the branch is master the "latest" tag is used,
        otherwise the next tag is used.
        If an empty string is specifed for tag_build,
        it is also treated as the latest tag.
        :param tag_base: The tag of the base image to use.
        If emtpy (default),
        then the tag of the base image is as specified in the Dockerfile.
        :param no_cache: If True, no cache is used when building the Docker image;
        otherwise, cache is used.
        :param copy_ssh_keys: If True, SSH keys are copied into a directory named ssh 
        under the current local Git repository. 
        :return: A tuple of the format (image_name_built, time_taken).
        """
        start = timer()
        self.clone_repo()
        self._copy_ssh(copy_ssh_to)
        if tag_build is None:
            tag_build = "latest" if self.branch == "master" else "next"
        elif tag_build == "":
            tag_build = "latest"
        if self.is_root:
            pull_image(self.base_image)
        logger.info("Building the Docker image {}...", self.name)
        self._update_base_tag(tag_build, tag_base)
        image = f"{self.name}:{tag_build}"
        cmd = ["docker", "build", "-t", image, str(self.path)]
        if no_cache:
            cmd.append("--no-cache")
        run_cmd(cmd, check=True)
        self.tag_build = tag_build
        self._remove_ssh(copy_ssh_to)
        end = timer()
        return image, end - start, "build"

    def _remove_ssh(self, copy_ssh_to: str):
        if copy_ssh_to:
            try:
                shutil.rmtree(self.path / copy_ssh_to)
            except FileNotFoundError:
                pass

    def _update_base_tag(self, tag_build: str, tag_base: str) -> None:
        tag = tag_base if self.is_root else tag_build
        if not tag:
            return
        dockerfile = self.path / DockerImage.DOCKERFILE
        with dockerfile.open() as fin:
            lines = fin.readlines()
        for idx, line in enumerate(lines):
            if line.startswith("FROM "):
                lines[idx] = line[:line.rfind(":")] + f":{tag}\n"
                break
        with dockerfile.open("w") as fout:
            fout.writelines(lines)

    def push(
        self,
        tag_tran_fun: Callable = tag_date,
        retry: int = 3,
        seconds: float = 60
    ) -> pd.DataFrame:
        """Push the built Docker image to the container repository.

        :param tag_tran_fun: A function takeing a tag as the parameter
        and generating a new tag to tag Docker images before pushing.
        :param retry: The number of times (default 3) to retry pushing the Docker image.
        :param seconds: The number of seconds (default 60) to wait before retrying.
        :return: A pandas DataFrame with 2 columns "image" (name of the built Docker image) 
        and "seconds" (time taken to build the Docker image).
        """
        image = f"{self.name}:{self.tag_build}"
        data = [push_image(image=image, retry=retry, seconds=seconds)]
        if tag_tran_fun:
            tag_new = tag_tran_fun(self.tag_build)
            if tag_new != self.tag_build:
                image_new = f"{self.name}:{tag_new}"
                run_cmd(["docker", "tag", image, image_new])
                data.append(push_image(image=image_new, retry=retry, seconds=seconds))
        return pd.DataFrame(data, columns=["image", "seconds", "type"])


class DockerImageBuilder:
    """A class for build many Docker images at once.
    """
    def __init__(
        self,
        git_urls: Union[Iterable[str], str, Path],
        branch: str = "dev",
    ):
        if isinstance(git_urls, str):
            git_urls = Path(git_urls)
        if isinstance(git_urls, Path):
            with git_urls.open("r") as fin:
                lines = (line.strip() for line in fin)
                git_urls = [
                    line for line in lines if not line.startswith("#") and line != ""
                ]
        self.git_urls = git_urls
        self.branch = branch
        self.docker_images: Dict[str, DockerImage] = {}

    def _get_deps(self) -> None:
        """Get dependencies (of all Docker images to build) in order.
        """
        if not self.docker_images:
            for git_url in self.git_urls:
                deps: Sequence[DockerImage] = DockerImage(
                    git_url=git_url, branch=self.branch
                ).get_deps(self.docker_images)
                for dep in deps:
                    self.docker_images[dep.git_url] = dep
        self._login_servers()

    def _login_servers(self) -> None:
        servers = set()
        for _, image in self.docker_images.items():
            if image.base_image.count("/") > 1:
                servers.add(image.base_image.split("/")[0])
            if image.name.count("/") > 1:
                servers.add(image.name.split("/")[0])
        for server in servers:
            run_cmd(f"docker login {server}", check=True)

    def push(self, tag_tran_fun: Callable = tag_date) -> pd.DataFrame:
        """Push all Docker images in self.docker_images.

        :param tag_tran_fun: A function takeing a tag as the parameter
        and generating a new tag to tag Docker images before pushing.
        """
        self._get_deps()
        frames = [image.push(tag_tran_fun) for _, image in self.docker_images.items()]
        return pd.concat(frames)

    def build(
        self,
        tag_build: str = None,
        tag_base: str = "",
        no_cache: Union[str, List[str], Set[str]] = None,
        copy_ssh_to: str = "",
        push: bool = True,
    ) -> Tuple[str, float]:
        """Build all Docker images in self.docker_images in order.
        :param tag_build: The tag of built images.
        :param tag_base: The tag to the root base image to use.
        :param no_cache: A set of docker images to disable cache when building.
        """
        self._get_deps()
        if isinstance(no_cache, str):
            no_cache = set([no_cache])
        elif isinstance(no_cache, list):
            no_cache = set(no_cache)
        elif no_cache is None:
            no_cache = set()
        data = [
            image.build(
                tag_build=tag_build,
                tag_base=tag_base,
                no_cache=image.name in no_cache,
                copy_ssh_to=copy_ssh_to
            ) for _, image in self.docker_images.items()
        ]
        frame = pd.DataFrame(data, columns=["image", "seconds", "type"])
        if push:
            frame = pd.concat([frame, self.push()])
        return frame
