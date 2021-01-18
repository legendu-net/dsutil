"""Docker related utils.
"""
from __future__ import annotations
from typing import Union, List, Sequence, Set, Deque, Tuple, Dict, Iterable, Callable
import tempfile
from pathlib import Path
import time
import timeit
import datetime
import subprocess as sp
from collections import deque
import shutil
import yaml
from loguru import logger
import pandas as pd
import git
import docker
import networkx as nx
from git import Repo


def tag_date(tag: str) -> str:
    """Get the current date as a 6-digit string.

    :param tag: A tag of Docker image.
    :return: The current in 6-digit format.
    """
    mmddhh = datetime.datetime.now().strftime("%m%d%H")
    return mmddhh if tag in ("", "latest") else f"{tag}_{mmddhh}"


def _push_image_timing(repo: str, tag: str) -> Tuple[str, str, float, str]:
    """Push a Docker image to Docker Hub and time the pushing.
    :param repo: The local repository of the Docker image.
    :param tag: The tag of the Docker image to push.
    :return: The time (in seconds) used to push the Docker image.
    """
    client = docker.from_env()
    seconds = timeit.timeit(
        lambda: client.images.push(repo, tag), timer=time.perf_counter_ns, number=1
    ) / 1E9
    return repo, tag, seconds, "push"


def _retry_docker(task: Callable,
                  retry: int = 3,
                  seconds: float = 60) -> Tuple[str, str, float, str]:
    """Retry a Docker API on failure (for a few times).
    :param task: The task to run.
    :param retry: The total number of times to retry.
    :param seconds: The number of seconds to wait before retrying.
    :return: The time (in seconds) used to run the task.
    """
    if retry <= 1:
        return task()
    for _ in range(retry):
        try:
            return task()
        except docker.errors.APIError:
            time.sleep(seconds)
    return task()


def _pull_image_timing(repo: str, tag: str) -> Tuple[str, str, float, str]:
    client = docker.from_env()
    seconds = timeit.timeit(
        lambda: client.images.pull(repo, tag), timer=time.perf_counter_ns, number=1
    ) / 1E9
    return repo, tag, seconds, "pull"


def _ignore_socket(dir_, files):
    dir_ = Path(dir_)
    return [file for file in files if (dir_ / file).is_socket()]


class DockerImage:
    """Class representing a Docker Image.
    """
    DOCKERFILE = "Dockerfile"

    def __init__(
        self,
        git_url: str,
        branch: str = "dev",
        branch_fallback: str = "dev",
    ):
        """Initialize a DockerImage object.

        :param git_url: URL of the remote Git repository.
        :param branch: The branch of the GitHub repository to use.
        """
        self.git_url = git_url.strip()
        self.branch = branch
        self.branch_fallback = branch_fallback
        self.path = None
        self.name = ""
        self.base_image = ""
        self.git_url_base = ""
        self.is_root = False
        self.tag_build = None

    def clone_repo(self, repo_path: Dict[str, str]) -> None:
        """Clone the Git repository to a local directory.

        :param repo_branch: A dick containing mapping of git_url to its local path.
        """
        if self.path:
            return
        if self.git_url in repo_path:
            self.path = repo_path[self.git_url]
        else:
            self.path = Path(tempfile.mkdtemp())
            logger.info("Cloning {} into {}", self.git_url, self.path)
            repo = git.Repo.clone_from(self.git_url, self.path)
            repo_path[self.git_url] = self.path
        for ref in repo.refs:
            if ref.name.endswith("/" + self.branch):
                repo.git.checkout(self.branch)
                break
        else:
            repo.git.checkout(self.branch_fallback)
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

    def get_deps(self, repo_branch, repo_path: Dict[str, str]) -> Deque[DockerImage]:
        """Get all dependencies of this DockerImage in order.

        :param repo_branch: A set-like collection containing tuples of (git_url, branch).
        :param repo_branch: A dick containing mapping of git_url to its local path.
        :return: A deque containing dependency images.
        """
        self.clone_repo(repo_path)
        deps = deque([self])
        obj = self
        while (obj.git_url_base, obj.branch) not in repo_branch:
            if obj.git_url_base:
                obj = DockerImage(git_url=obj.git_url_base, branch=obj.branch)
                obj.clone_repo(repo_path)
                deps.appendleft(obj)
            else:
                deps[0].is_root = True
                break
        return deps

    def _copy_ssh(self, copy_ssh_to: str):
        if copy_ssh_to:
            ssh_src = Path.home() / ".ssh"
            if not ssh_src.is_dir():
                logger.warning("~/.ssh does NOT exists!")
                return
            ssh_dst = self.path / copy_ssh_to
            try:
                shutil.rmtree(ssh_dst)
            except FileNotFoundError:
                pass
            shutil.copytree(ssh_src, ssh_dst, ignore=_ignore_socket)
            logger.info("~/.ssh has been copied to {}", ssh_dst)

    def build(
        self,
        tag_build: str = None,
        tag_base: str = "",
        no_cache: bool = False,
        copy_ssh_to: str = ""
    ) -> Tuple[str, str, float, str]:
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
        :param copy_ssh_to: If True, SSH keys are copied into a directory named ssh
            under the current local Git repository. 
        :return: A tuple of the format (image_name_built, time_taken).
        """
        start = time.perf_counter_ns()
        self.clone_repo({})
        self._copy_ssh(copy_ssh_to)
        if tag_build is None:
            if self.branch in ("master", "main"):
                tag_build = "latest"
            elif self.branch == "dev":
                tag_build = "next"
            else:
                tag_build = self.branch
        elif tag_build == "":
            tag_build = "latest"
        if self.is_root:
            _retry_docker(lambda: _pull_image_timing(*self.base_image.split(":")))
        logger.info("Building the Docker image {}...", self.name)
        self._update_base_tag(tag_build, tag_base)
        docker.from_env().images.build(
            path=str(self.path),
            tag=f"{self.name}:{tag_build}",
            nocache=no_cache,
            rm=True,
            pull=False,
            cache_from=None
        )
        self.tag_build = tag_build
        self._remove_ssh(copy_ssh_to)
        end = time.perf_counter_ns()
        return self.name, tag_build, (end - start) / 1E9, "build"

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
        data = [
            _retry_docker(
                lambda: _push_image_timing(self.name, self.tag_build), retry, seconds
            )
        ]
        if tag_tran_fun:
            tag_new = tag_tran_fun(self.tag_build)
            if tag_new != self.tag_build:
                docker.from_env().images.get(f"{self.name}:{self.tag_build}").tag(
                    self.name, tag_new, force=True
                )
                data.append(
                    _retry_docker(
                        lambda: _push_image_timing(self.name, tag_new), retry, seconds
                    )
                )
        return pd.DataFrame(data, columns=["repo", "tag", "seconds", "type"])


class DockerImageBuilder:
    """A class for build many Docker images at once.
    """
    def __init__(self, branch_urls: Union[Dict[str, List[str]], str, Path], branch_fallback: str = "dev"):
        if isinstance(branch_urls, (str, Path)):
            with open(branch_urls, "r") as fin:
                branch_urls = yaml.load(fin, Loader=yaml.FullLoader)
        self._branch_urls = branch_urls
        self._branch_fallback = branch_fallback
        self._graph = None
        self._repo_branch = {}
        self._repo_path = {}

    def _build_graph_branch(self, branch, urls):
        for url in urls:
            deps: Sequence[DockerImage] = DockerImage(
                git_url=url, branch=branch
            ).get_deps(self._graph.nodes, self._repo_path)
            if deps[0].git_url_base:
                self._add_nodes(
                    (deps[0].git_url_base, deps[0].branch, self.branch_fallback), deps[0]
                )
            self._repo_branch.setdefault(deps[0].git_url, [])
            self._repo_branch.get(deps[0].git_url).append(deps[0].branch)
            for idx in range(1, len(deps)):
                self._add_nodes(deps[idx - 1], deps[idx])
                self._repo_branch.setdefault(deps[idx].git_url, [])
                self._repo_branch.get(deps[idx].git_url).append(deps[idx].branch)

    def _find_identical_node(self, dep: Union[DockerImage, Tuple[str, str, str]]) -> Union[Tuple[str, str], None]:
        """Find node in the graph which has identical branch as the specified dependency.
        Notice that a node in the graph is represented as (git_url, branch).

        :param dep: A dependency of the type DockerImage. 
        """
        if isinstance(dep, tuple):
            git_url, branch, branch_fallback = dep
        else:
            git_url = dep.git_url
            branch = dep.branch
            branch_fallback = dep.branch_fallback
        branches = self._repo_branch.get(git_url, [])
        if not branches:
            return None
        path = self._repo_path.get(git_url)
        for branch in branches:
            if self._compare_git_branches(path, (branch, branch), ("", branch_fallback)):
                return (git_url, branch)
        return None

    def _compare_git_branches(
        self, path: str, branches: Tuple[str, str], fallback: Tuple[str, str]
    ) -> bool:
        """Compare whether 2 branches of a repo are identical.

        :param path: The path to a local Git repository.
        :param branches: A pair of branches to compare.
        :param fallbacks: A pair of fallback branches to use fo the 2 branches respecitively.
        :return: True if there are no differences between the 2 branches and false otherwise.
        """
        repo = Repo(path)
        b1, b2 = branches
        f1, f2 = fallback
        commit1 = self._get_branch_commit(repo, b1, f1)
        commit2 = self._get_branch_commit(repo, b2, f2)
        return not commit1.diff(commit2)

    @staticmethod
    def _get_branch_commit(repo, branch: str, fallback: str):
        fallback_commit = None
        for ref in repo.refs:
            if ref.name.endswith("/" + branch):
                return ref.commit
            if ref.name.endswith("/" + fallback):
                fallback_commit = ref.commit
        return fallback_commit

    def _add_nodes(self, dep1: Union[DockerImage, Tuple[str, str, str]], dep2: DockerImage) -> None:
        inode1 = self._find_identical_node(dep1)
        if inode1 is None:
            pass
            # TODO: need throw an exception here as this is impossible
        inode2 = self._find_identical_node(dep2)
        if inode2 is None:
            self._graph.add_edge(inode1, (dep2.git_url, dep2.branch))
            return
        pnode2 = next(self._graph.predecessors(inode2))
        if pnode2 != inode1:
            # node2 has an identical node inode2 in the graph
            # but inode2's parent is different from the parent of node2 (which is inode1),
            # so we cannot reuse inode2 here
            self._graph.add_edge(inode1, (dep2.git_url, dep2.branch_urls))

    def _build_graph(self):
        if self._graph is not None:
            return
        self._graph = nx.DiGraph()
        for branch, urls in self._branch_urls.items():
            self._build_graph_branch(branch, urls)
        with open("edges.txt", "w") as fout:
            for edge in self._graph.edges:
                fout.write(str(edge) + "\n")
        #self._login_servers()

    #def _login_servers(self) -> None:
    #    servers = set()
    #    for _, image in self.docker_images.items():
    #        if image.base_image.count("/") > 1:
    #            servers.add(image.base_image.split("/")[0])
    #        if image.name.count("/") > 1:
    #            servers.add(image.name.split("/")[0])
    #    for server in servers:
    #        sp.run(f"docker login {server}", shell=True, check=True)

    def push(self, tag_tran_fun: Callable = tag_date) -> pd.DataFrame:
        """Push all Docker images in self.docker_images.

        :param tag_tran_fun: A function takeing a tag as the parameter
            and generating a new tag to tag Docker images before pushing.
        :return: A pandas DataFrame summarizing pushing information.
        """
        self._build_graph()
        frames = [image.push(tag_tran_fun) for _, image in self.docker_images.items()]
        return pd.concat(frames)

    def build(
        self,
        tag_build: str = None,
        tag_base: str = "",
        no_cache: Union[bool, str, List[str], Set[str]] = None,
        copy_ssh_to: str = "",
        push: bool = True,
    ) -> pd.DataFrame:
        """Build all Docker images in self.docker_images in order.

        :param tag_build: The tag of built images.
        :param tag_base: The tag to the root base image to use.
        :param no_cache: A set of docker images to disable cache when building.
        :param copy_ssh_to: If True, SSH keys are copied into a directory named ssh
            under each of the local Git repositories. 
        :param push: If True, push the built Docker images to DockerHub.
        :return: A pandas DataFrame summarizing building information.
        """
        self._build_graph()
        if isinstance(no_cache, str):
            no_cache = set([no_cache])
        elif isinstance(no_cache, list):
            no_cache = set(no_cache)
        elif isinstance(no_cache, bool) and no_cache:
            no_cache = set(image.name for image in self.docker_images.values())
        else:
            no_cache = set()
        data = [
            image.build(
                tag_build=tag_build,
                tag_base=tag_base,
                no_cache=image.name in no_cache,
                copy_ssh_to=copy_ssh_to
            ) for image in self.docker_images.values()
        ]
        frame = pd.DataFrame(data, columns=["repo", "tag", "seconds", "type"])
        if push:
            frame = pd.concat([frame, self.push()])
        return frame
