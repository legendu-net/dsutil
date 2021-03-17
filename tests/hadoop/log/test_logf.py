"""Test the dsutil.logf module.
"""
from pathlib import Path
import dsutil.hadoop.logf

BASE_DIR = Path(__file__).parent


def test_main():
    args = dsutil.hadoop.logf.parse_args([
        "filter", str(BASE_DIR / "application_1611634725250_1347938")
    ])
    dsutil.hadoop.logf.main(args)
