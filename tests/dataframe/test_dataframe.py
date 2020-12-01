"""Test dataframe.py.
"""
from pathlib import Path
import dsutil.dataframe
BASE_DIR = Path(__file__).resolve().parent


def test_read_csv():
    path = BASE_DIR / "data"
    df = dsutil.dataframe.read_csv(path)
    assert df.shape == (2, 2)