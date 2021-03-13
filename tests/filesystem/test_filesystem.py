"""Test dataframe.py.
"""
from pathlib import Path
import dsutil

BASE_DIR = Path(__file__).resolve().parent


def test_is_ess_empty():
    assert dsutil.filesystem.is_ess_empty(BASE_DIR) is False
    assert dsutil.filesystem.is_ess_empty(BASE_DIR / "ess_empty")
    assert dsutil.filesystem.is_ess_empty(BASE_DIR / "ess_empty/.ipynb_checkpoints")
