"""Docker related utils.
"""
from __future__ import annotations
from typing import Union, List, Sequence, Deque, Tuple, Dict, Callable
from dataclasses import dataclass
import json
import tempfile
from pathlib import Path
import time
import timeit
import datetime
from collections import deque
import shutil
import yaml
from loguru import logger
import pandas as pd
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
    logger.info("Pushing Docker image {}:{} ...", repo, tag)

    def _push():
        for line in client.images.push(repo, tag, stream=True, decode=True):
            print(line)

    seconds = timeit.timeit(_push, timer=time.perf_counter_ns, number=1) / 1E9
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


def branch_to_tag(branch: str) -> str:
    """Convert a branch to its corresponding Docker image tag.

    :param branch: A branch name.
    :return: The Docker image tag corresponding to the branch.
    """
    if branch in ("master", "main"):
        return "latest"
    if branch == "dev":
        return "next"
    return branch


@dataclass(frozen=True)
class Node:
    """A class similar to DockerImage for simplifying code.
    """
    git_url: str
    branch: str
    branch_effective: str


class DockerImage:
    """Class representing a Docker Image.
    """
    DOCKERFILE = "Dockerfile"

    def __init__(
        self,
        git_url: str,
        branch: str = "dev",
        branch_fallback: str = "dev",
        repo_path: Dict[str, str] = None
    ):
        """Initialize a DockerImage object.

        :param git_url: URL of the remote Git repository.
        :param branch: The branch of the GitHub repository to use.
        """
        self._git_url = git_url.strip()
        self._branch = branch
        self._branch_fallback = branch_fallback
        self._branch_effective = ""
        self._repo_path = {} if repo_path is None else repo_path
        self._path = None
        self._name = ""
        self._base_image = ""
        self._git_url_base = ""
        self._tag_build = None

    def is_root(self) -> bool:
        """Check whether this DockerImage is a root DockerImage.
        """
        return not self._git_url_base

    def clone_repo(self) -> None:
        """Clone the Git repository to a local directory.

        :param repo_branch: A dick containing mapping of git_url to its local path.
        """
        if self._path:
            return
        if self._git_url in self._repo_path:
            self._path = self._repo_path[self._git_url]
            repo = Repo(self._path)
            logger.info(
                "{} has already been cloned into {} previously.", self._git_url,
                self._path
            )
        else:
            self._path = Path(tempfile.mkdtemp())
            logger.info("Cloning {} into {}", self._git_url, self._path)
            repo = Repo.clone_from(self._git_url, self._path)
            self._repo_path[self._git_url] = self._path
        for ref in repo.refs:
            if ref.name.endswith("/" + self._branch):
                self._branch_effective = self._branch
                break
        else:
            self._branch_effective = self._branch_fallback
        repo.git.checkout(self._branch_effective, force=True)
        self._parse_dockerfile()

    def _parse_dockerfile(self):
        dockerfile = self._path / DockerImage.DOCKERFILE
        with dockerfile.open() as fin:
            for line in fin:
                if line.startswith("# NAME:"):
                    self._name = line[7:].strip()
                    logger.info("This image name: {}", self._name)
                elif line.startswith("FROM "):
                    self._base_image = line[5:].strip()
                    logger.info("Base image name: {}", self._base_image)
                elif line.startswith("# GIT:"):
                    self._git_url_base = line[6:].strip()
                    logger.info("Base image URL: {}", self._git_url_base)
        if not self._name:
            raise LookupError("The name tag '# NAME:' is not found in the Dockerfile!")
        if not self._base_image:
            raise LookupError("The FROM line is not found in the Dockerfile!")

    def get_deps(self, repo_branch) -> Deque[DockerImage]:
        """Get all dependencies of this DockerImage in order.

        :param repo_branch: A set-like collection containing tuples of (git_url, branch).
        :param repo_branch: A dick containing mapping of git_url to its local path.
        :return: A deque containing dependency images.
        """
        self.clone_repo()
        deps = deque([self])
        obj = self
        while (obj._git_url_base, obj._branch) not in repo_branch:  # pylint: disable=W0212
            if not obj._git_url_base:  # pylint: disable=W0212
                break
            obj = obj.base_image()
            deps.appendleft(obj)
        return deps

    def base_image(self) -> DockerImage:
        """Get the base DockerImage of this DockerImage.
        """
        image = DockerImage(
            git_url=self._git_url_base,
            branch=self._branch,
            branch_fallback=self._branch_fallback,
            repo_path=self._repo_path
        )
        image.clone_repo()
        return image

    def _copy_ssh(self, copy_ssh_to: str):
        if copy_ssh_to:
            ssh_src = Path.home() / ".ssh"
            if not ssh_src.is_dir():
                logger.warning("~/.ssh does NOT exists!")
                return
            ssh_dst = self._path / copy_ssh_to
            try:
                shutil.rmtree(ssh_dst)
            except FileNotFoundError:
                pass
            shutil.copytree(ssh_src, ssh_dst, ignore=_ignore_socket)
            logger.info("~/.ssh has been copied to {}", ssh_dst)

    def build(self,
              tag_build: str = None,
              copy_ssh_to: str = "") -> Tuple[str, str, float, str]:
        """Build the Docker image.

        :param tag_build: The tag of the Docker image to build.
            If None (default), then it is determined by the branch name.
            When the branch is master the "latest" tag is used,
            otherwise the next tag is used.
            If an empty string is specifed for tag_build,
            it is also treated as the latest tag.
        :param copy_ssh_to: If True, SSH keys are copied into a directory named ssh
            under the current local Git repository. 
        :return: A tuple of the format (image_name_built, tag_built, time_taken, "build").
        """
        start = time.perf_counter_ns()
        self.clone_repo()
        self._copy_ssh(copy_ssh_to)
        if tag_build is None:
            tag_build = branch_to_tag(self._branch)
        elif tag_build == "":
            tag_build = "latest"
        if not self._git_url_base:  # self is a root image
            _retry_docker(lambda: _pull_image_timing(*self._base_image.split(":")))
        logger.info("Building the Docker image {}...", self._name)
        self._update_base_tag(tag_build)
        docker.from_env().images.build(
            path=str(self._path),
            tag=f"{self._name}:{tag_build}",
            rm=True,
            pull=False,
            cache_from=None
        )
        self._tag_build = tag_build
        self._remove_ssh(copy_ssh_to)
        end = time.perf_counter_ns()
        return self._name, tag_build, (end - start) / 1E9, "build"

    def _remove_ssh(self, copy_ssh_to: str):
        if copy_ssh_to:
            try:
                shutil.rmtree(self._path / copy_ssh_to)
            except FileNotFoundError:
                pass

    def _update_base_tag(self, tag_build: str) -> None:
        if not self._git_url_base:  # self is a root image
            return
        dockerfile = self._path / DockerImage.DOCKERFILE
        with dockerfile.open() as fin:
            lines = fin.readlines()
        for idx, line in enumerate(lines):
            if line.startswith("FROM "):
                lines[idx] = line[:line.rfind(":")] + f":{tag_build}\n"
                break
        with dockerfile.open("w") as fout:
            fout.writelines(lines)

    def node(self):
        """Convert this DockerImage to a Node.
        """
        return Node(
            git_url=self._git_url,
            branch=self._branch,
            branch_effective=self._branch_effective
        )

    def base_node(self):
        """Convert the base image of this DockerImage to a Node.
        """
        return self.base_image().node()


class DockerImageBuilder:
    """A class for build many Docker images at once.
    """
    def __init__(
        self,
        branch_urls: Union[Dict[str, List[str]], str, Path],
        branch_fallback: str = "dev"
    ):
        if isinstance(branch_urls, (str, Path)):
            with open(branch_urls, "r") as fin:
                branch_urls = yaml.load(fin, Loader=yaml.FullLoader)
        self._branch_urls = branch_urls
        self._branch_fallback = branch_fallback
        self._graph = None
        self._repo_branch: Dict[str, List[Node]] = {}
        self._repo_path = {}
        self._roots = set()

    def _build_graph_branch(self, branch, urls):
        for url in urls:
            deps: Sequence[DockerImage] = DockerImage(
                git_url=url,
                branch=branch,
                branch_fallback=self._branch_fallback,
                repo_path=self._repo_path
            ).get_deps(self._graph.nodes)
            if deps[0].is_root():
                self._add_root_node(deps[0].node())
            else:
                self._add_nodes(deps[0].base_node(), deps[0].node())
            for idx in range(1, len(deps)):
                self._add_nodes(deps[idx - 1].node(), deps[idx].node())

    def _find_identical_node(self, node: Node) -> Union[Node, None]:
        """Find node in the graph which has identical branch as the specified dependency.
        Notice that a node in the graph is represented as (git_url, branch).

        :param node: A dependency of the type DockerImage. 
        """
        logger.debug("Finding identical node of {} in the graph ...", node)
        nodes: List[Node] = self._repo_branch.get(node.git_url, [])
        logger.debug("Nodes associated with the repo {}: {}", node.git_url, nodes)
        if not nodes:
            return None
        path = self._repo_path[node.git_url]
        for n in nodes:
            if self._compare_git_branches(
                path, n.branch_effective, node.branch_effective
            ):
                return n
        return None

    def _compare_git_branches(self, path: str, b1: str, b2: str) -> bool:
        """Compare whether 2 branches of a repo are identical.

        :param path: The path to a local Git repository.
        :param b1: A branches.
        :param b2: Another branches.
        :return: True if there are no differences between the 2 branches and false otherwise.
        """
        repo = Repo(path)
        logger.debug("Comparing branches {} and {} of the local repo {}", b1, b2, path)
        if b1 == b2:
            return True
        commit1 = self._get_branch_commit(repo, b1)
        commit2 = self._get_branch_commit(repo, b2)
        return not commit1.diff(commit2)

    @staticmethod
    def _get_branch_commit(repo, branch: str):
        for ref in repo.refs:
            if ref.name.endswith("/" + branch):
                return ref.commit
        raise LookupError(f"Branch {branch} is not found in the local repo {repo}!")

    def _add_root_node(self, node):
        logger.debug("Adding root node {} into the graph ...", node)
        inode = self._find_identical_node(node)
        if inode is None:
            self._graph.add_node(node)
            self._repo_branch.setdefault(node.git_url, [])
            self._repo_branch[node.git_url].append(node)
            self._roots.add(node)
            return
        self._add_identical_branch(inode, node.branch_effective)

    def _add_nodes(self, node1: Node, node2: Node) -> None:
        logger.debug("Adding nodes ({}, {}) into the graph ...", node1, node2)
        inode1 = self._find_identical_node(node1)
        if inode1 is None:
            raise LookupError(f"{node1} is expected in the graph but not found!")
        inode2 = self._find_identical_node(node2)
        # In the following 2 situations we need to create a new node for node2
        # 1. node2 does not have an identical node (inode2 is None)
        # 2. node2 has an identical node inode2 in the graph
        #     but inode2's parent is different from the parent of node2 (which is inode1)
        if inode2 is None:
            self._graph.add_edge(inode1, node2)
            self._repo_branch.setdefault(node2.git_url, [])
            self._repo_branch[node2.git_url].append(node2)
            return
        if next(self._graph.predecessors(inode2)) != inode1:
            self._graph.add_edge(inode1, node2)
            return
        # reuse inode2
        self._add_identical_branch(inode2, node2.branch_effective)

    def _add_identical_branch(self, node: Node, branch: str):
        attr = self._graph.nodes[node]
        attr.setdefault("identical_branches", set())
        attr["identical_branches"].add(branch)

    def _build_graph(self):
        if self._graph is not None:
            return
        self._graph = nx.DiGraph()
        for branch, urls in self._branch_urls.items():
            self._build_graph_branch(branch, urls)

    def save_graph(self) -> None:
        """Save the underlying graph structure to files.
        """
        #nx.write_yaml(self._graph, "graph.yaml")
        with open("edges.txt", "w") as fout:
            for edge in self._graph.edges:
                fout.write(str(edge) + "\n")
        with open("nodes.txt", "w") as fout:
            for node in self._graph.nodes:
                identical_branches = self._graph.nodes[node].get(
                    "identical_branches", set()
                )
                fout.write(f"{node}: {list(identical_branches)}\n")
        with open("branches.txt", "w") as fout:
            fout.write(json.dumps(self._repo_branch, indent=4))

    #def _login_servers(self) -> None:
    #    servers = set()
    #    for _, image in self.docker_images.items():
    #        if image.base_image.count("/") > 1:
    #            servers.add(image.base_image.split("/")[0])
    #        if image.name.count("/") > 1:
    #            servers.add(image.name.split("/")[0])
    #    for server in servers:
    #        sp.run(f"docker login {server}", shell=True, check=True)

    def build_images(
        self,
        tag_build: str = None,
        copy_ssh_to: str = "",
        push: bool = True,
        remove: bool = False,
    ) -> pd.DataFrame:
        """Build all Docker images in self.docker_images in order.

        :param tag_build: The tag of built images.
        :param copy_ssh_to: If True, SSH keys are copied into a directory named ssh
            under each of the local Git repositories. 
        :param push: If True, push the built Docker images to DockerHub.
        :return: A pandas DataFrame summarizing building information.
        """
        self._build_graph()
        data = []
        for node in self._roots:
            self._build_images_graph(
                node=node,
                tag_build=tag_build,
                copy_ssh_to=copy_ssh_to,
                push=push,
                remove=remove,
                data=data
            )
        frame = pd.DataFrame(data, columns=["repo", "tag", "seconds", "type"])
        return frame

    def _build_images_graph(
        self, node, tag_build: str, copy_ssh_to: str, push: bool, remove: bool,
        data: List
    ) -> None:
        res = self._build_image_node(
            node=node,
            tag_build=tag_build,
            copy_ssh_to=copy_ssh_to,
            push=push,
            data=data
        )
        children = self._graph.successors(node)
        for child in children:
            self._build_images_graph(
                node=child,
                tag_build=tag_build,
                copy_ssh_to=copy_ssh_to,
                push=push,
                remove=remove,
                data=data
            )
        if not remove:
            return
        # remove images associate with node
        client = docker.from_env()
        for image_name, tag, *_ in res:
            logger.info("Removing Docker image {}:{} ...", image_name, tag)
            client.images.remove(f"{image_name}:{tag}")

    def _build_image_node(
        self, node, tag_build: str, copy_ssh_to: str, push: bool,
        data: List[Tuple[str, str, float, str]]
    ) -> List[Tuple[str, str, float, str]]:
        image = DockerImage(
            git_url=node.git_url,
            branch=node.branch,
            branch_fallback=self._branch_fallback,
            repo_path=self._repo_path
        )
        res = []
        name, tag, time, type_ = image.build(
            tag_build=tag_build, copy_ssh_to=copy_ssh_to
        )
        # record building/pushing info
        res.append((name, tag, time, type_))
        # create new tags on the built images corresponding to other branches
        for br in self._graph.nodes[node].get("identical_branches", set()):
            if br == node.branch:
                continue
            tag_new = branch_to_tag(br)
            docker.from_env().images.get(f"{name}:{tag}").tag(name, tag_new, force=True)
            res.append((name, tag_new, 0, "build"))
        if push:
            for name, tag, *_ in res.copy():
                res.append(_retry_docker(lambda: _push_image_timing(name, tag)))  # pylint: disable=W0640
        data.extend(res)
        return res
