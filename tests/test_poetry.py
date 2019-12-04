import dsutil
from pathlib import Path

TEST_DIR = Path(__file__).resolve().parent


def test_version():
    dsutil.poetry.version()
    dsutil.poetry.version('1.0.0', proj_dir=TEST_DIR / "dsutil")
