"""Test the aiutil.jupyter module.
"""
from pathlib import Path
import aiutil.notebook.util

BASE_DIR = Path(__file__).parent


def test_nbconvert_notebooks():
    aiutil.notebook.util.nbconvert_notebooks(BASE_DIR / "notebooks")
