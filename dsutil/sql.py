"""SQL related utils.
"""
from typing import Union
from pathlib import Path
import subprocess as sp
import sqlparse


def format(path: Union[Path, str]):
    """Format a SQL file.

    :param path: The path to a SQL file.
    """
    if isinstance(path, str):
        path = Path(path)
    query = sqlparse.format(
        path.read_text(),
        keyword_case="upper",
        identifier_case="lower",
        strip_comments=False,
        reindent=True,
        indent_width=2
    )
    path.write_text(query)
    cmd = f"pg_format --function-case 1 --type-case 3 --inplace {path}"
    sp.run(cmd, shell=True, check=True)
