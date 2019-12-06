import dsutil
from pathlib import Path


def test_images():
    dsutil.docker.images()


def test_remove_images():
    dsutil.docker.remove_images(name="nimade")


def test_build_images():
    dsutil.docker.build_images(path="docker-python")