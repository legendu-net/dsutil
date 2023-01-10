"""Test poetry.py.
"""
from pathlib import Path
import aiutil

BASE_DIR = Path(__file__).resolve().parent


def test_version():
    aiutil.poetry.version()
    aiutil.poetry.version("1.0.0", proj_dir=BASE_DIR / "aiutil")
