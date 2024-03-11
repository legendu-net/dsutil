"""Test dataframe.py.
"""

from pathlib import Path
import aiutil

BASE_DIR = Path(__file__).resolve().parent


def test_is_ess_empty():
    assert aiutil.filesystem.is_ess_empty(BASE_DIR) is False
    assert aiutil.filesystem.is_ess_empty(BASE_DIR / "ess_empty")
    assert aiutil.filesystem.is_ess_empty(BASE_DIR / "ess_empty/.ipynb_checkpoints")
