#!/usr/bin/env python3
# encoding: utf-8

import os
import sys
from argparse import ArgumentParser
import glob
import re


def strip_margin(text: str) -> str:
    """Remove all leading white spaces in each row of a string.
    This function is same as Scala's stripMargin method of String.
    For example, the input string is:
    select a
        from table_a
    Then the output should be:
    select a
    from table_a

    :param text: the input string
    :return: A new string with all white spaces at the beginning of each row removed.
    """
    indent = len(min(re.findall(r"\n[ \t]*(?=\S)", text) or [""]))
    pattern = r"\n[ \t]{%d}" % (indent - 1)
    return re.sub(pattern, "\n", text)


def has_header(files, num_files: int = 5):
    """Check whether the files have headers.
 
    :param files: the list of files to check.
    :param n: the number of non-empty files to use to decide whether there are header lines.
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


def _merge_with_headers(files, output: str = ""):
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


def _merge_without_header(files, output: str = ""):
    """Merge files without header.
    """
    with open(output, "wb") if output else sys.stdout.buffer as fout:
        for file in files:
            with open(file, "rb") as fin:
                for line in fin:
                    fout.write(line)
                fout.write(b"\n")


def merge(files, output: str = "", n: int = 5):
    """Merge files. If there are headers in files, keep only one header in the single merged file.

    :param files: list of files.
    :param output: output files for merging the files.
    :param n: number of files for checking whether there are headers in files.
    """
    if isinstance(files, str):
        files = [os.path.join(files, f) for f in os.listdir(files)]
        return merge(files, output=output, n=n)
    if not n:
        n = min(10, len(files))
    if has_header(files, n):
        _merge_with_headers(files, output)
    else:
        _merge_without_header(files, output)


def merge_args(args=None, namespace=None):
    """Parse arguments for the function dedup_header.
    """
    parser = ArgumentParser(
        description="Merge text file with proper handling of headers."
    )
    parser.add_argument(
        "-f",
        "--files",
        dest="files",
        nargs="+",
        required=True,
        help="the files (Linux-style wildcards are supported) to merge."
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        required=False,
        default="",
        help="the (optional) output file."
    )
    parser.add_argument(
        "-n",
        "--num-deciding-header",
        dest="n",
        type=int,
        required=False,
        default=5,
        help="the number of non-empty files for deciding whether there are headers."
    )
    args = parser.parse_args(args=args, namespace=namespace)
    files = [file for pattern in args.files for file in glob.glob(pattern)]
    merge(files, args.output, args.n)


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


def dedup_header_args(args=None, namespace=None):
    """Parse arguments for the function dedup_header.
    """
    parser = ArgumentParser(description="Dedup headers in a text file.")
    parser.add_argument(
        "-f", "--file", dest="file", required=True, help="the file to dedup."
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        required=False,
        default="",
        help="the (optional) output file."
    )
    args = parser.parse_args(args=args, namespace=namespace)
    dedup_header(args.file, args.output)


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
