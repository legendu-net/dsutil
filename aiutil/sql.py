"""SQL related utils.
"""
from pathlib import Path
import subprocess as sp
import sqlparse


def format(path: Path | str):
    """Format a SQL file.

    :param path: The path to a SQL file.
    """
    if isinstance(path, str):
        path = Path(path)
    query = sqlparse.format(
        path.read_text(encoding="utf-8"),
        keyword_case="upper",
        identifier_case="lower",
        strip_comments=False,
        reindent=True,
        indent_width=2,
    )
    path.write_text(query, encoding="utf-8")
    cmd = f"pg_format --function-case 1 --type-case 3 --inplace {path}"
    sp.run(cmd, shell=True, check=True)
