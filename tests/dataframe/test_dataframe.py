"""Test dataframe.py.
"""
from pathlib import Path
import aiutil.dataframe

BASE_DIR = Path(__file__).resolve().parent


def test_read_csv():
    path = BASE_DIR / "data"
    df = aiutil.dataframe.read_csv(path)
    assert df.shape == (2, 2)
