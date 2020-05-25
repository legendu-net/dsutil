import sys
import time
from timeit import default_timer as timer
import datetime
from typing import List, Callable, Union
import shutil
import tempfile
from pathlib import Path
import re
import subprocess as sp
import git
import pandas as pd
from loguru import logger
from . import shell
REPO = "https://github.com/dclong/{}.git"
PREFIX = "dclong/"
DEP = "dependencies.txt"
DOCKERFILE = "Dockerfile"


def run_cmd(cmd, shell: bool = False, check: bool = False) -> None:
    """Run a (Docker) command.
    :param cmd: The command to run.
    :param shell: Whether to run the command as a shell subprocess.
    :param check: Whether to check for errors. 
        If so, exceptions are thrown on errors.
    """
    msg = " ".join(str(elem) for elem in cmd) if isinstance(cmd, list) else cmd
    msg = "Running command: " + msg
    logger.debug(msg)
    sp.run(cmd, shell=shell, check=check)


def remove(choice: str = "") -> None:
    """Remove exited Docker containers and images without tags.
    """
    remove_containers(status="^Exited|^Created", choice=choice)
    remove_images(tag="none", choice=choice)
    print(containers())
    print(images())


def remove_containers(
    id_: str = "", name: str = "", status: str = "", choice: str = ""
) -> None:
    """Remove the specified Docker containers.
    :param id_: The id of the container to remove.
    :param name: A (regex) pattern of names of containers to remove.
    :param exited: Whether to remove exited containers.
    :param choice: One of "y" (auto yes), "n" (auto no) 
        or "i" (interactive, i.e., ask for confirmation on each case).
    """
    if id_:
        run_cmd(["docker", "rm", id_])
    if name:
        run_cmd(["docker", "rm", name])
    if status:
        conts = containers()
        conts = conts[conts.status.str.contains(status)]
        if conts.empty:
            return
        print("\n", conts, "\n")
        sys.stdout.flush()
        sys.stderr.flush()
        if not choice:
            choice = input(
                "Do you want to remove the above containers? (y - Yes, n - [No], i - interactive): "
            )
        for row in conts.itertuples():
            if choice == "y":
                run_cmd(["docker", "rm", row.container_id])
            elif choice == "i":
                choice_i = input(
                    f"Do you want to remove the container '{row.names}'? (y/N): "
                )
                if choice_i == "y":
                    run_cmd(["docker", "rm", row.container_id])
    print(containers())


def remove_images(
    id_: str = "", name: str = "", tag: str = "", choice: str = ""
) -> None:
    """Remove specified Docker images.
    :param id_: The id of the image to remove.
    :param name: A (regex) pattern of names of images to remove.
    :param tag: Remove images whose tags containing specified tag.
    """
    imgs = images()
    if id_:
        _remove_images(imgs[imgs.image_id.str.contains(id_)], choice=choice)
    if name:
        _remove_images(imgs[imgs.repository.str.contains(name)], choice=choice)
    if tag:
        _remove_images(imgs[imgs.tag.str.contains(tag)], choice=choice)
    print(images())


def _remove_images(imgs, choice: str = ""):
    if imgs.empty:
        return
    print("\n", imgs, "\n")
    sys.stdout.flush()
    sys.stderr.flush()
    print("-" * 80)
    if not choice:
        choice = input(
            "Do you want to remove the above images? (y - Yes, n - [No], i - interactive): "
        )
    for row in imgs.itertuples():
        image_name = row.repository + ":" + row.tag
        image = row.image_id if row.tag == "<none>" else image_name
        if choice == "y":
            run_cmd(["docker", "rmi", image])
        elif choice == "i":
            choice_i = input(
                f"Do you want to remove the image '{image_name}'? (y - Yes, n - [No]):"
            )
            if choice_i == "y":
                run_cmd(["docker", "rmi", image])


def containers() -> pd.DataFrame:
    """Get all Docker containers.
    :return: A DataFrame containing all Docker containers.
    """
    frame = shell.to_frame("docker ps -a", split_by_title=True)
    return frame


def images() -> pd.DataFrame:
    """Get all Docker images.
    :return: A DataFrame containing all Docker images.
    """
    return shell.to_frame("docker images", split_by_title=True)


def _push_images_timing(image: str) -> float:
    """Push a Docker image to Docker Hub and time the pushing.
    :param image: The full name of the image to push to Docker Hub.
    :return: The time (in seconds) used to push the Docker image.
    """
    start = timer()
    run_cmd(["docker", "push", image])
    end = timer()
    return image, end - start


def _push_images_retry(image: str) -> float:
    """Push a Docker image to Docker Hub. Automatically retry pushing once it fails.
    :param image: The full name of the image to push to Docker Hub.
    :return: The time (in seconds) used to push the Docker image.
    """
    try:
        return _push_images_timing(image)
    except sp.CalledProcessError:
        time.sleep(60)
        return _push_images_timing(image)


def push_images(
    path: Path,
    tag: str = "latest",
    tag_tran_fun: Callable = lambda tag: tag
) -> pd.DataFrame:
    """Push Docker images produced by building Git repositories in the specified path.
    :param path: A path containing the pulled Git repositories.
    :param tag: Docker images with the specified tag are pushed.
    :param tag_tran_fun: A function takeing a tag as the parameter 
        and generating a new tag to tag Docker images before pushing.
    """
    if not tag:
        tag = "latest"
    if not isinstance(path, Path):
        path = clone_repos(
            repos=path,
            branch="master" if tag == "latest" else "dev",
            repos_root=""
        )
    with Path(path, DEP).open() as fin:
        dependencies = fin.readlines()
    timing = []
    for repos in dependencies:
        repos = repos.strip()
        image = repos.replace("docker-", PREFIX)
        print("\n\n")
        timing.append(_push_images_retry(f"{image}:{tag}"))
        tag_new = tag_tran_fun(tag)
        if tag_new != tag:
            run_cmd(["docker", "tag", f"{image}:{tag}", f"{image}:{tag_new}"])
            print("\n\n")
            timing.append(_push_images_retry(f"{image}:{tag_new}"))
    frame = pd.DataFrame(timing, columns=["image", "seconds"])
    print("\n", frame, sep="")
    return frame


def tag_date(tag: str) -> str:
    """Get the current date as a 6-digit string.
    :return: The current in 6-digit format.
    """
    mmddhh = datetime.datetime.now().strftime("%m%d%H")
    return mmddhh if tag in ("", "latest") else f"{tag}_{mmddhh}"


def pull_images(path: Union[str, Path], branch: str):
    """Pull a Docker image and all its dependent images.
    :param path: The Docker image (and whose dependent images) to pull
        or a path containing the cloned Git repositories.
    """
    if not isinstance(path, Path):
        path = clone_repos(repos=path, branch=branch, repos_root="")
    # pull Docker images
    with (path / DEP).open() as fin:
        dependencies = fin.readlines()
    for idx, dep in enumerate(dependencies):
        dep = dep.strip()
        if idx == 0:
            run_cmd(["docker", "pull", _base_image(path / dep)], check=True)
        run_cmd(["docker", "pull", dep.replace("docker-", PREFIX)], check=True)


def build_images(
    path: Union[str, Path],
    cache: bool = True,
    no_cache_from: str = "",
    tag_base: str = "",
    tag_build: str = "next",
    push: bool = False,
) -> Path:
    """Build Docker image for the specified repository/directory.
    Depdendency images are built first in order if any.
    :param path: The repository to build Docker images from
        or a path containing the pulled Git repositories.
    :param cache: If True (default), cache is used. Otherwise, cache is not used.
    :param no_cache_from: Do not use cache from the specified repository/image.
    :param tag_build: The tag of built images.
    :param push: If True (default), push images to Docker Hub.
    """
    if not tag_build:
        tag_build = "latest"
    if not isinstance(path, Path):
        path = clone_repos(
            repos=path,
            branch="master" if tag_build == "latest" else "dev",
            repos_root=""
        )
    # build Docker images
    with (path / DEP).open() as fin:
        dependencies = fin.readlines()
    for idx, dep in enumerate(dependencies):
        dep = dep.strip()
        path_dep = path / dep
        if idx == 0:
            print("\n\n")
            # TODO: pull retry similar to push retry
            if tag_base:
                update_base_tag(path_dep, tag=tag_base)
            run_cmd(["docker", "pull", _base_image(path_dep)], check=True)
        else:
            update_base_tag(path_dep, tag=tag_build)
        if dep == no_cache_from:
            cache = False
        print(f"\n\nBuilding {dep}...")
        cmd = [
            "docker", "build", "-t",
            f"{dep.replace('docker-', PREFIX)}:{tag_build}", path_dep
        ]
        if not cache:
            cmd.append("--no-cache")
        run_cmd(cmd, check=True)
    if push:
        push_images(path=path, tag=tag_build, tag_tran_fun=tag_date)
    return path


def update_base_tag(path: Path, tag: str) -> None:
    """Change the tag of the base image in the Dockerfile to the specified one.
    :param path: The directory containing the Dockerfile.
    :param tag: The tag to change to.
    """
    dockerfile = path / DOCKERFILE
    with dockerfile.open() as fin:
        lines = fin.readlines()
    for idx, line in enumerate(lines):
        if line.startswith("FROM "):
            lines[idx] = line[:line.rfind(":")] + f":{tag}\n"
            break
    with dockerfile.open("w") as fout:
        fout.writelines(lines)


def clone_repos(repos: str, branch: str, repos_root: str = "") -> Path:
    """Pull the GitHub repository and dependent GitHub repositories of a Docker image.
    :param repos: The repository (and whose dependent repositories) to clone. 
        If you want to clone the GitHub repository corresponding to the Docker image dclong/jupyterhub-ds,
        you can either specified it as dclong/jupyterhub-ds or docker-jupyterhub-ds.
    :param repos_root: The root Docker/GitHub repository.
    :return: The path where the Git repositories are cloned to.
    """
    path = Path(tempfile.mkdtemp())
    dependencies = []
    _clone_repos_helper(
        path=path,
        repos_name=repos,
        branch=branch,
        repos_root=REPO.format(repos_root),
        dependencies=dependencies
    )
    with (path / DEP).open("w") as fout:
        for line in reversed(dependencies):
            fout.write(line + "\n")
    return path


def _clone_repos_helper(
    path: Path, repos_name: str, branch: str, repos_root: str,
    dependencies: List[str]
) -> None:
    """A helper function for the function clone_repos.
    """
    print("\n\n")
    repos_name = repos_name.strip("/").replace(PREFIX, "docker-")
    repos_url = REPO.format(repos_name)
    repo = git.Repo.clone_from(repos_url, path / repos_name)
    for rb in repo.remote().fetch():
        if rb.name.split("/")[1] == branch:
            repo.git.checkout(branch)
    dependencies.append(repos_name)
    if repos_url == repos_root:
        return
    base_image = _base_image(path / repos_name)
    if not base_image.startswith(PREFIX):
        return
    _clone_repos_helper(
        path=path,
        repos_name=base_image,
        branch=branch,
        repos_root=repos_root,
        dependencies=dependencies
    )


def _base_image(path: Path) -> str:
    """Get the name of the base image of the current repository/directory.
    :param path: the directory containing the Dockerfile.
    """
    dockerfile = path / DOCKERFILE
    with dockerfile.open() as fin:
        for line in fin:
            if line.startswith("FROM "):
                line = line[5:].strip()
                return line
        raise LookupError("The FROM line is not found in the Dockerfile!")
