"""Utils for manipulating text files. 
"""
#!/usr/bin/env python3
# encoding: utf-8
from typing import Union
import os
import sys
from pathlib import Path
from loguru import logger


def has_header(files, num_files: int = 5) -> bool:
    """Check whether the files have headers.

    :param files: the list of files to check.
    :param num_files: the number of non-empty files to use to decide whether there are header lines.
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
        if k >= num_files:
            break
        with open(files[j], "r", encoding="utf-8") as fin:
            first_line = fin.readline()
            if first_line:
                k += 1
                if first_line != possible_header:
                    return False
    return True


def _merge_with_headers(files, output: str = "") -> None:
    """Merge files with headers. Keep only one header.
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


def _merge_without_header(files, output: str = "") -> None:
    """Merge files without header.
    """
    with open(output, "wb") if output else sys.stdout.buffer as fout:
        for file in files:
            with open(file, "rb") as fin:
                for line in fin:
                    fout.write(line)
                fout.write(b"\n")


def merge(files, output: str = "", n: int = 5) -> None:
    """Merge files. If there are headers in files, keep only one header in the single merged file.

    :param files: list of files.
    :param output: output files for merging the files.
    :param n: number of files for checking whether there are headers in files.
    """
    if isinstance(files, str):
        files = [os.path.join(files, f) for f in os.listdir(files)]
        merge(files, output=output, n=n)
        return
    if not n:
        n = min(10, len(files))
    if has_header(files, n):
        _merge_with_headers(files, output)
        return
    _merge_without_header(files, output)


def dedup_header(file, output: str = ""):
    """Dedup headers in a file (due to the hadoop getmerge command).
    Only the header on the first line is kept and headers (identical line to the first line) 
    on other lines are removed.
    """
    with open(file, "rb"
             ) as fin, open(output, "wb") if output else sys.stdout.buffer as fout:
        header = fin.readline()
        fout.write(header)
        for line in fin:
            if line != header:
                fout.write(line)


def select(file, columns, delimiter, output: str = ""):
    """Select fields by name from a delimited file (not necessarily well structured).
    """
    with open(file, "r", encoding="utf-8") as fin:
        header = fin.readline().split(delimiter)
        index = []
        columns_full = []
        for i, field in enumerate(header):
            if field in columns:
                index.append(i)
                columns_full.append(field)
        with (open(output, "w", encoding="utf-8") if output else sys.stdout) as fout:
            fout.write(delimiter.join(columns_full) + "\n")
            for line in fin:
                fields = line.split(delimiter)
                fout.write(delimiter.join([fields[i] for i in index]) + "\n")


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
