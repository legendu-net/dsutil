"""Docker related utils.
"""
import sys
from typing import Union
import subprocess as sp
import pandas as pd
import docker
from .builder import DockerImage, DockerImageBuilder


def _get_image_repo(image):
    repo_digests = image.attrs["RepoDigests"]
    if repo_digests:
        return repo_digests[0].split("@", maxsplit=1)[0]
    repo_tags = image.attrs["RepoTags"]
    if repo_tags:
        return repo_tags[0].split(":", maxsplit=1)[0]
    return ""


def images() -> pd.DataFrame:
    """Return Docker images as a pandas DataFrame.
    """
    data = []
    for image in docker.from_env().images.list():
        repository = _get_image_repo(image)
        image_id = image.short_id[7:]
        created = image.attrs["Created"]
        size = image.attrs["Size"]
        if image.tags:
            for tag in image.tags:
                data.append(
                    {
                        "repository": repository,
                        "tag": tag.split(":")[1],
                        "image_id": image_id,
                        "created": created,
                        "size": size
                    }
                )
        else:
            data.append(
                {
                    "repository": repository,
                    "tag": "<none>",
                    "image_id": image_id,
                    "created": created,
                    "size": size
                }
            )
    return pd.DataFrame(data)


def containers() -> pd.DataFrame:
    """Return Docker containers as a pandas DataFrame.
    """
    data = [
        {
            "container_id": cont.short_id,
            "container_obj": cont,
            "image": cont.image.tags[0] if cont.image.tags else cont.image.short_id[7:],
            "command": cont.attrs["Config"]["Cmd"],
            "created": cont.attrs["Created"],
            "status": cont.status,
            "ports": cont.ports,
            "name": cont.name,
        } for cont in docker.from_env().containers.list(all=True)
    ]
    return pd.DataFrame(data)


def remove(aggressive: bool = False, choice: str = "") -> None:
    """Remove exited Docker containers and images without tags.
    """
    remove_containers(status="^exited|^created", choice=choice)
    remove_images(tag="none", choice=choice)
    if aggressive:
        remove_images(tag="[a-z]*_?[0-9]{4}", choice=choice)
        imgs = images().groupby("image_id").apply(  # pylint: disable=E1101
            lambda frame: frame.query("tag == 'next'") if frame.shape[0] > 1 else None
        )
        _remove_images(imgs, choice=choice)
    sp.run("docker ps", shell=True, check=True)
    sp.run("docker images", shell=True, check=True)


def remove_containers(
    id_: str = "", name: str = "", status: str = "", choice: str = ""
) -> None:
    """Remove the specified Docker containers.
    :param id_: The id of the container to remove.
    :param name: A (regex) pattern of names of containers to remove.
    :param status: Filter containers with the specified status.
    :param choice: One of "y" (auto yes), "n" (auto no) 
        or "i" (interactive, i.e., ask for confirmation on each case).
    """
    client = docker.from_env()
    if id_:
        client.containers.remove(id_)
    if name:
        client.containers.remove(name)
    if status:
        conts = containers()
        conts = conts[conts.status.str.contains(status, case=False)]
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
                client.containers.remove(row.container_id)
            elif choice == "i":
                choice_i = input(
                    f"Do you want to remove the container '{row.names}'? (y/N): "
                )
                if choice_i == "y":
                    client.containers.remove(row.container_id)
    sp.run("docker ps", shell=True, check=True)


def pull():
    """Automatically pull all valid images.
    """
    client = docker.from_env()
    imgs = images()
    imgs = imgs[imgs.repository != "<None>" & imgs.tag != "<None>"]
    for _, (repo, tag, *_) in imgs.iterrows():
        client.images.pull(repo, tag)


def remove_images(
    id_: str = "",
    name: str = "",
    tag: str = "",
    frame: Union[pd.DataFrame, None] = None,
    choice: str = ""
) -> None:
    """Remove specified Docker images.
    :param id_: The id of the image to remove.
    :param name: A (regex) pattern of names of images to remove.
    :param tag: Remove images whose tags containing specified tag.
    """
    if id_:
        imgs = images()
        _remove_images(imgs[imgs.image_id.str.contains(id_, case=False)], choice=choice)
    if name:
        imgs = images()
        _remove_images(imgs[imgs.repository.str.contains(name, case=False)], choice=choice)
    if tag:
        imgs = images()
        _remove_images(imgs[imgs.tag.str.contains(tag, case=False)], choice=choice)
    if frame is not None:
        _remove_images(frame, choice=choice)
    sp.run("docker images", shell=True, check=True)


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
    client = docker.from_env()
    for row in imgs.itertuples():
        image_name = row.repository + ":" + row.tag
        image = row.image_id if row.tag == "<none>" else image_name
        if choice == "y":
            client.images.remove(image)
        elif choice == "i":
            choice_i = input(
                f"Do you want to remove the image '{image_name}'? (y - Yes, n - [No]):"
            )
            if choice_i == "y":
                client.images.remove(image)


def stop(id_: str = "", name: str = "", status: str = "", choice: str = "") -> None:
    """Stop the specified Docker containers.
    :param id_: The id of the container to remove.
    :param name: A (regex) pattern of names of containers to remove.
    :param status: Stop containers with the specified status.
    :param choice: One of "y" (auto yes), "n" (auto no)
        or "i" (interactive, i.e., ask for confirmation on each case).
    """
    client = docker.from_env()
    if id_:
        client.stop(id_)
    if name:
        client.stop(name)
    if status:
        conts = containers()
        conts = conts[conts.status.str.contains(status, case=False)]
        if conts.empty:
            return
        print("\n", conts, "\n")
        sys.stdout.flush()
        sys.stderr.flush()
        if not choice:
            choice = input(
                "Do you want to stop the above containers? (y - Yes, n - [No], i - interactive): "
            )
        for row in conts.itertuples():
            if choice == "y":
                client.stop(row.container_id)
            elif choice == "i":
                choice_i = input(
                    f"Do you want to stop the container '{row.names}'? (y/N): "
                )
                if choice_i == "y":
                    client.stop(row.container_id)
    sp.run("docker ps", shell=True, check=True)
