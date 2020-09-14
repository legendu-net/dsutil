"""Testing the module dsutil.docker.
"""
import sys
import pytest
import dsutil


@pytest.mark.skipif(sys.platform == "darwin", reason="Skip test for Mac OS")
def test_images():
    dsutil.docker.images()


@pytest.mark.skipif(sys.platform == "darwin", reason="Skip test for Mac OS")
def test_remove_images():
    dsutil.docker.remove_images(name="nimade")
