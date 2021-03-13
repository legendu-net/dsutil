"""Test poetry.py.
"""
from pathlib import Path
import dsutil

BASE_DIR = Path(__file__).resolve().parent


def test_version():
    dsutil.poetry.version()
    dsutil.poetry.version('1.0.0', proj_dir=BASE_DIR / "dsutil")
