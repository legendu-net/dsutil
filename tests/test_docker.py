"""Testing the module dsutil.docker.
"""
import sys
import shutil
import pytest
import dsutil


@pytest.mark.skipif(sys.platform == "darwin", reason="Skip test for Mac OS")
def test_images():
    if shutil.which("docker"):
        dsutil.docker.images()


@pytest.mark.skipif(sys.platform == "darwin", reason="Skip test for Mac OS")
def test_remove_images():
    if shutil.which("docker"):
        dsutil.docker.remove_images(name="nimade")
