"""Test the dsutil.jupyter module.
"""
from pathlib import Path
import dsutil.jupyter

BASE_DIR = Path(__file__).parent


def test_nbconvert_notebooks():
    dsutil.jupyter.nbconvert_notebooks(BASE_DIR / "notebooks")
