#!/usr/bin/env python3

import os
import re
import shutil
from typing import Iterable, Dict
from pathlib import Path
HOME = Path.home()


def copy_if_exists(src, dst=HOME) -> bool:
    """Copy a file.
    No exception is thrown if the source file does not exist.
    :param src: The path of the source file.
    :param dst: The path of the destination file.
    """
    if not os.path.exists(src):
        return False
    try:
        shutil.copy2(src, dst)
        return True
    finally:
        return False


def link_if_exists(src, dst=HOME, target_is_directory=True) -> bool:
    """Make a symbolic link of a file.
    No exception is thrown if the source file does not exist.
    :param src: The path of the source file.
    :param dst: The path of the destination file.
    """
    if not os.path.exists(src):
        return False
    if os.path.exists(dst):
        shutil.rmtree(dst)
    try:
        os.symlink(src, dst, target_is_directory=target_is_directory)
        return True
    finally:
        return False


def update_file(path: Path, pattern: str, replace: str) -> None:
    """Update a text file using regular expression substitution.
    :param file: The path to the text file to be updated.
    :param pattern: The pattern to substitute.
    :param replace: The text to replace the patterns to.
    """
    text = path.read_text()
    text = re.sub(pattern, replace, text)
    path.write_text(text)


def count_path(paths: Iterable[str]) -> Dict[str, int]:
    """Count frequence of paths and their parent paths.
    :param path: An iterable collection of paths.
    """
    freq = {}
    for path in paths:
        _count_path_helper(path, freq)
    return freq


def _count_path_helper(path: str, freq: dict):
    fields = path.rstrip('/').split('/')[:-1]
    path = ''
    for field in fields:
        path = path + field + '/'
        freq[path] = freq.get(path, 0) + 1
