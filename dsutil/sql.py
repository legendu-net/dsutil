from typing import Union
from pathlib import Path
import subprocess as sp
import sqlparse


def format(path: Union[Path, str]):
    if isinstance(path, str):
        path = Path(path)
    queries = path.read_text()
    query = sqlparse.format(query,
        keyword_case="upper",
        identifier_case="lower",
        strip_comments=False,
        reindent=True,
        indent_width=2
    )
    path.write_text(query)
    cmd = f"pg_format --function-case 1 --type-case 3 --inplace {path}"
    sp.run(cmd, shell=True, check=True)
