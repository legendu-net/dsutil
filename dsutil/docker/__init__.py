"""Docker related utils.
"""
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
import pandas as pd
from loguru import logger
from .. import shell
from .docker import DockerImage
from .docker import DockerImageBuilder


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
