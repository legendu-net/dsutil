"""Testing the module dsutil.docker.
"""
import sys
import shutil
from pathlib import Path
import pytest
import dsutil
BASE_DIR = Path(__file__).parent


@pytest.mark.skipif(sys.platform == "darwin", reason="Skip test for Mac OS")
def test_remove_images():
    if shutil.which("docker"):
        dsutil.docker.remove_images(name="nimade")


def test_copy_ssh():
    builder = dsutil.docker.DockerImage(git_url="")
    builder.path = BASE_DIR
    builder._copy_ssh("ssh")


@pytest.mark.skipif(sys.platform == "darwin", reason="Skip test for Mac OS")
def test_DockerImageBuilder():
    if not shutil.which("docker"):
        return
    images = [
        "https://github.com/dclong/docker-python-portable.git",
        ]
    builder = dsutil.docker.DockerImageBuilder(images, branch="master")
    builder.build(tag_build="unittest")
