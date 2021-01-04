"""Utils for manipulating text files. 
"""
#!/usr/bin/env python3
# encoding: utf-8
from typing import Union, List
import sys
from pathlib import Path
from loguru import logger


def has_header(
    files: Union[str, Path, List[Union[str, Path]]],
    num_files_checking: int = 5
) -> bool:
    """Check whether the files have headers.

    :param files: the list of files to check.
    :param num_files_checking: the number of non-empty files to use to decide whether there are header lines.
    :return: True if the files have headers and False otherwise.
    """
    # i: file index
    for i in range(len(files)):
        with open(files[i], "r", encoding="utf-8") as fin:
            first_line = fin.readline()
            if first_line:
                possible_header = first_line
                break
    # k: current number of non-empty files
    k = 1
    for j in range(i, len(files)):
        if k >= num_files_checking:
            break
        with open(files[j], "r", encoding="utf-8") as fin:
            first_line = fin.readline()
            if first_line:
                k += 1
                if first_line != possible_header:
                    return False
    return True


def _merge_with_headers(
    files: Union[str, Path, List[Union[str, Path]]],
    output: Union[str, Path] = ""
) -> None:
    """Merge files with headers. Keep only one header.

    :param files: A list of files 
        or the path to a directory containing a list of files to merge.
    :param output: output files for merging the files.
    """
    with open(output, "wb") if output else sys.stdout.buffer as out:
        with open(files[0], "rb") as fin0:
            for line in fin0:
                out.write(line)
        for file in files[1:]:
            with open(file, "rb") as fin:
                fin.readline()
                for line in fin:
                    out.write(line)


def _merge_without_header(
    files: Union[str, Path, List[Union[str, Path]]],
    output: Union[str, Path] = ""
) -> None:
    """Merge files without header.

    :param files: A list of files 
        or the path to a directory containing a list of files to merge.
    :param output: output files for merging the files.
    """
    with open(output, "wb") if output else sys.stdout.buffer as fout:
        for file in files:
            with open(file, "rb") as fin:
                for line in fin:
                    fout.write(line)
                fout.write(b"\n")


def merge(
    files: Union[str, Path, List[Union[str, Path]]],
    output: str = "",
    num_files_checking: int = 5
) -> None:
    """Merge files. If there are headers in files, keep only one header in the single merged file.

    :param files: A list of files 
        or the path to a directory containing a list of files to merge.
    :param output: output files for merging the files.
    :param num_files_checking: number of files for checking whether there are headers in files.
    """
    if isinstance(files, str):
        files = Path(files)
    if isinstance(files, Path):
        files = list(files.iterdir())
    if num_files_checking <= 0:
        num_files_checking = 5
    num_files_checking = min(num_files_checking, len(files))
    if has_header(files, num_files_checking):
        _merge_with_headers(files, output)
        return
    _merge_without_header(files, output)


def dedup_header(file: Union[str, Path], output: Union[str, Path] = "") -> None:
    """Dedup headers in a file (due to the hadoop getmerge command).
    Only the header on the first line is kept and headers (identical line to the first line) 
    on other lines are removed.

    :param file: The path to the file to be deduplicated.
    :param output: The path of the output file. 
        If empty, then output to the standard output.
    """
    with open(file, "rb"
             ) as fin, open(output, "wb") if output else sys.stdout.buffer as fout:
        header = fin.readline()
        fout.write(header)
        for line in fin:
            if line != header:
                fout.write(line)


def select(
    path: Union[str, Path],
    columns: Union[str, List[str]],
    delimiter: str,
    output: str = ""
):
    """Select fields by name from a delimited file (not necessarily well structured).

    :param path: To path to a file (containing delimited values in each row).
    :param columns: A list of columns to extract from the file.
    :param delimiter: The delimiter of fields.
    :param output: The path of the output file. 
        If empty, then output to the standard output.
    """
    if isinstance(path, str):
        path = Path(path)
    if isinstance(columns, str):
        columns = [columns]
    with path.open("r", encoding="utf-8") as fin:
        header = fin.readline().split(delimiter)
        index = []
        columns_full = []
        for idx, field in enumerate(header):
            if field in columns:
                index.append(idx)
                columns_full.append(field)
        with (open(output, "w", encoding="utf-8") if output else sys.stdout) as fout:
            fout.write(delimiter.join(columns_full) + "\n")
            for line in fin:
                fields = line.split(delimiter)
                fout.write(delimiter.join([fields[idx] for idx in index]) + "\n")


def prune_json(input: Union[str, Path], output: Union[str, Path] = ""):
    """Prune fields (value_counts) from a JSON file.

    :param input: The path to a JSON file to be pruned.
    :param output: The path to output the pruned JSON file.
    """
    logger.info("Pruning the JSON fiel at {}...", input)
    if isinstance(input, str):
        input = Path(input)
    if isinstance(output, str):
        if output:
            output = Path(output)
        else:
            output = input.with_name(input.stem + "_prune.json")
    skip = False
    with input.open("r") as fin, output.open("w") as fout:
        for line in fin:
            line = line.strip()
            if line == '"value_counts": {':
                skip = True
                continue
            if skip:
                if line in ("}", "},"):
                    skip = False
            else:
                fout.write(line)
    logger.info("The pruned JSON file is written to {}.", output)
