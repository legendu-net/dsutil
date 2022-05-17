"""Test the aiutil.jupyter module.
"""
from pathlib import Path
import aiutil.jupyter

BASE_DIR = Path(__file__).parent


def test_nbconvert_notebooks():
    aiutil.jupyter.nbconvert_notebooks(BASE_DIR / "notebooks")
