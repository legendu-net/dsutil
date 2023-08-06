"""Shell command related utils.
"""
from typing import Union, Sequence
from pathlib import Path
import re
import subprocess as sp
import pandas as pd


def ls(path: Union[Path, str]) -> pd.DataFrame:
    """List files in the given path.
    This function is similar to the shell command `ls`.

    :param path: A path to a file or a directory.
    """
    if isinstance(path, str):
        path = Path(path)
    paths = list(path.iterdir()) if path.is_dir() else [path]
    return pd.DataFrame(
        {
            "paths": paths,
            "name": (p.name for p in paths),
        }
    )


def to_frame(
    cmd="",
    split: str = r"  +",
    header: Union[int, list[str], None] = None,
    skip: Union[int, Sequence[int]] = (),
    lines: Sequence[str] = (),
    split_by_header: bool = False,
) -> pd.DataFrame:
    """Convert the result of a shell command to a DataFrame.
    The headers are splitted by a regular expression
    while the columns are splitted by the right-most position of the headers.

    :param cmd: A shell command.
    :param split: A regular expression pattern for splitting a line into fields.
    :param header: An integer, list of string or None specifiying the header of the data frame.
        If header is an integer,
        the corresponding row of lines after removing empty and skipped rows is used as header of the data frame;
        if header is a list of string then it is used as the header of the data frame.
        if header is None, then default header is used for the data frame.
    :param skip: Indexes of rows to skip.
    :param lines: The output of the shell command.
    :param split_by_header: If true, the headers are splitted by a regular expression
        and the columns are splitted by the right-most position of the headers.
        Otherwise, all lines are splitted by the specified regular expression.
    :return: A pandas DataFrame.
    """

    def _reg_skip(skip, n) -> set[int]:
        if isinstance(skip, int):
            skip = [skip]
        return set(idx % n for idx in skip)

    if not lines:
        lines = sp.check_output(cmd, shell=True).decode().strip().split("\n")
    skip = _reg_skip(skip, len(lines))
    lines = [line for idx, line in enumerate(lines) if idx not in skip]
    if split_by_header:
        return _to_frame_title(split=split, lines=lines)
    return _to_frame_space(split=split, header=header, lines=lines)


def _to_frame_space(
    lines: list[str],
    split: str = r"  +",
    header: Union[int, list[str], None] = None,
) -> pd.DataFrame:
    """Convert the result of a shell command to a DataFrame.

    :param lines: The output of a shell command as lines of rows.
    :param split: A regular expression pattern for splitting a line into fields.
    :param header: An integer, list of string or None specifiying the header of the data frame.
        If header is an integer, the corresponding row of lines after removing empty and skipped rows is used as header of the data frame;
        if header is a list of string then it is used as the header of the data frame.
        if header is None, then default header is used for the data frame.
    :return: A pandas DataFrame.
    """
    data = [re.split(split, line.strip()) for line in lines if line.strip()]
    if isinstance(header, int):
        columns = [re.sub(r"\s+", "_", col.lower()) for col in data[header]]
        data = (row for idx, row in enumerate(data) if idx != header)
        frame = pd.DataFrame(data, columns=columns)
    elif isinstance(header, list):
        frame = pd.DataFrame(data, columns=header)
    else:
        frame = pd.DataFrame(data)
    return frame.astype(str)


def _to_frame_title(lines: list[str], split: str = r"  +") -> pd.DataFrame:
    """Convert the result of a shell command to a DataFrame.

    :param lines: The output of the shell command as list of lines.
    :param split: A regular expression pattern for splitting headers.
        Notice that non-header rows are splitted according the right-most position of the headers.
    :return: A pandas DataFrame.
    """
    headers = re.split(split, lines[0])
    n = len(headers)
    data = {}
    for idx in range(n - 1):
        start = lines[0].index(headers[idx])
        end = lines[0].index(headers[idx + 1])
        data[headers[idx]] = [line[start:end].strip() for line in lines[1:]]
    start = lines[0].index(headers[-1])
    data[headers[-1]] = [line[start:].strip() for line in lines[1:]]
    frame = pd.DataFrame(data)
    frame.columns = [col.strip().lower().replace(" ", "_") for col in frame.columns]
    return frame.astype(str)
