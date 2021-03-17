"""Test the dsutil.logf module.
"""
from pathlib import Path
import dsutil.hadoop.logf

BASE_DIR = Path(__file__).parent


def test_main():
    output = BASE_DIR / "log_s"
    args = dsutil.hadoop.logf.parse_args(
        [
            "filter",
            str(BASE_DIR / "application_1611634725250_1347938"), "-o",
            str(output)
        ]
    )
    dsutil.hadoop.logf.main(args)
    assert output.is_file()
