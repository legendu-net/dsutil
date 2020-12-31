#!/usr/bin/env python3
"""Filesystem related util functions.
"""
import os
import re
import shutil
from typing import Union, Iterable, Dict, List, Tuple, Set, Callable
import math
from pathlib import Path
import subprocess as sp
from itertools import chain
import tempfile
from tqdm import tqdm
import pandas as pd
from loguru import logger
import git
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
    except Exception:
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
    except Exception:
        return False


def count_path(paths: Iterable[str], ascending=False) -> pd.Series:
    """Count frequence of paths and their parent paths.

    :param paths: An iterable collection of paths.
    """
    freq = {}
    for path in paths:
        _count_path_helper(path, freq)
    freq = pd.Series(freq, name="count").sort_values(ascending=ascending)
    return freq


def _count_path_helper(path: str, freq: dict):
    fields = path.rstrip("/").split("/")[:-1]
    path = ""
    for field in fields:
        path = path + field + "/"
        freq[path] = freq.get(path, 0) + 1


def zip_subdirs(root: Union[str, Path]) -> None:
    """Compress subdirectories into zip files.

    :param root: The root directory whose subdirs are to be zipped.
    """
    if isinstance(root, str):
        root = Path(root)
    for path in root.iterdir():
        if path.is_dir() and not path.name.startswith("."):
            file = path.with_suffix(".zip")
            print(f"{path} -> {file}")
            sp.run(f"zip -qr {file} {path} &", shell=True, check=True)


def flatten_dir(dir_):
    """Flatten a directory,
    i.e., move files in immediate subdirectories into the current directory.

    :param dir_: The directory to flatten.
    """
    if isinstance(dir_, str):
        dir_ = Path(dir_)
    for path in dir_.iterdir():
        if path.is_dir():
            _flatten_dir(path)
            path.rmdir()


def _flatten_dir(dir_):
    """Helper method of flatten_dir.
    """
    for path in dir_.iterdir():
        path.rename(path.parent.parent / path.name)


def split_dir(dir_: Union[str, Path], batch: int, wildcard: str = "*") -> None:
    """Split files in a directory into sub-directories.
    This function is for the convenience of splitting a directory 
    with a large number of files into smaller directories 
    so that those subdirs can zipped (into relatively smaller files) and uploaded to cloud quickly.

    :param dir_: The root directory whose files are to be splitted into sub-directories.
    :param wildcard: A wild card pattern specifying files to be included.
    :param batch: The number files that each subdirs should contain.
    """
    if isinstance(dir_, str):
        dir_ = Path(dir_)
    files = sorted(dir_.glob(wildcard))
    num_batch = math.ceil(len(files) / batch)
    nchar = len(str(num_batch))
    for index in tqdm(range(num_batch)):
        _split_dir_1(dir_ / f"{index:0>{nchar}}", files, index, batch)


def _split_dir_1(desdir, files, index, batch):
    """Helper method of split_dir.
    """
    desdir.mkdir(exist_ok=True)
    for path in files[(index * batch):((index + 1) * batch)]:
        path.rename(desdir / path.name)


def find_images(root_dir: Union[str, Path, List[str], List[Path]]) -> List[Path]:
    """Find all PNG images in a (sequence) of dir(s) or its/their subdirs.

    :param root_dir: A (list) of dir(s).
    """
    if isinstance(root_dir, (str, Path)):
        root_dir = [root_dir]
    images = []
    for path in root_dir:
        if isinstance(path, str):
            path = Path(path)
        images.extend(path.glob("**.png"))
    return images


def find_data_tables(
    root: Union[str, Path],
    filter_: Callable = lambda _: True,
    extensions: Iterable[str] = (),
    patterns: Iterable[str] = (),
) -> Set[str]:
    """Find keywords which are likely data table names.

    :param root: The root directory or a GitHub repo URL in which to find data table names.
    :param filter_: A function for filtering identified keywords (via regular expressions).
    By default, all keywords identified by regular expressions are kept.
    :param extensions: Addtional text file extensions to use.
    :param extensions: Addtional regular expression patterns to use.
    """
    if isinstance(root, str):
        if re.search(r"(git@|https://).*\.git", root):
            with tempfile.TemporaryDirectory() as tempdir:
                git.Repo.clone_from(root, tempdir, branch="master")
                logger.info(
                    "The repo {} is cloned to the local directory {}.", root, tempdir
                )
                return find_data_tables(tempdir, filter_=filter_)
        root = Path(root)
    if root.is_file():
        return _find_data_tables_file(root, filter_, patterns)
    extensions = {
        ".sql",
        ".py",
        ".ipy",
        ".ipynb",
        ".scala",
        ".java",
        ".txt",
        ".json",
    } | set(extensions)
    paths = (
        path for path in Path(root).glob("**/*")
        if path.suffix.lower() in extensions and path.is_file()
    )
    return set(
        chain.from_iterable(
            _find_data_tables_file(path, filter_, patterns) for path in paths
        )
    )


def _find_data_tables_file(file, filter_, patterns) -> Set[str]:
    if isinstance(file, str):
        file = Path(file)
    text = file.read_text().lower()
    patterns = {
        r"from\s+(\w+)\W*\s*",
        r"from\s+(\w+\.\w+)\W*\s*",
        r"join\s+(\w+)\W*\s*",
        r"join\s+(\w+\.\w+)\W*\s*",
        r"table\((\w+)\)",
        r"table\((\w+\.\w+)\)",
        r'"table":\s*"(\w+)"',
        r'"table":\s*"(\w+\.\w+)"',
    } | set(patterns)
    tables = chain.from_iterable(re.findall(pattern, text) for pattern in patterns)
    mapping = str.maketrans("", "", "'\"\\")
    tables = (table.translate(mapping) for table in tables)
    return set(table for table in tables if filter_(table))


def is_empty(dir_: Union[str, Path], filter_: Union[None, Callable] = lambda _: True):
    """Check whether a directory is empty.

    :param dir_: The directory to check.
    :param filter_: A filtering function (default True always) to limit the check to sub files/dirs.
    """
    if isinstance(dir_, str):
        dir_ = Path(dir_)
    paths = dir_.glob("**/*")
    return not any(True for path in paths if filter_(path))


def _ignore(path: Path) -> bool:
    path = path.resolve()
    if path.is_file() and path.name.startswith("."):
        return True
    if path.is_dir() and path.name in (
        ".ipynb_checkpoints", ".mypy_cache", ".mtj.tmp", "__pycache__"
    ):
        return True
    return False


def remove_ess_empty(path: Union[str, Path], ignore: Callable = _ignore) -> List[Path]:
    """Remove essentially empty directories under a path.

    :param path: The path to the directory to check.
    :param ignore: A bool function which returns True on files/directories to ignore.
    :return: A list of Path objects which failed to be removed.
    """
    fail = []
    for p in find_ess_empty(path, ignore=ignore):
        try:
            if p.is_file() or p.is_symlink():
                p.unlink()
            else:
                shutil.rmtree(p)
        except PermissionError:
            fail.append(p)
    return fail


def find_ess_empty(path: Union[str, Path], ignore: Callable = _ignore) -> List[Path]:
    """Find essentially empty sub directories under a directory.

    :param path: The path to the directory to check.
    :param ignore: A bool function which returns True on files/directories to ignore.
    :return: A list of directories which are essentially empty.
    """
    if isinstance(path, str):
        path = Path(path)
    ess_empty = {}
    ess_empty_dir = []
    _find_ess_empty(
        path=path, ignore=ignore, ess_empty=ess_empty, ess_empty_dir=ess_empty_dir
    )
    return ess_empty_dir


def _find_ess_empty(
    path: Path, ignore: Callable, ess_empty: Dict[Path, bool], ess_empty_dir: List[str]
):
    if is_ess_empty(path=path, ignore=ignore, ess_empty=ess_empty):
        ess_empty_dir.append(path)
        return
    for p in path.iterdir():
        if p.is_dir():
            _find_ess_empty(
                path=p, ignore=ignore, ess_empty=ess_empty, ess_empty_dir=ess_empty_dir
            )


def is_ess_empty(
    path: Path, ignore: Callable = _ignore, ess_empty: Dict[Path, bool] = None
):
    """Check if a directory is essentially empty.

    :param path: The path to the directory to check.
    :param ignore: A bool function which returns True on files/directories to ignore.
    :return: True if the directory is essentially empty and False otherwise.
    """
    if isinstance(path, str):
        path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"The file {path} does not exist!")
    if not os.access(path, os.R_OK):
        return False
    if path.is_symlink():
        return True
    path = path.resolve()
    if ess_empty is None:
        ess_empty = {}
    if path in ess_empty:
        return ess_empty[path]
    if ignore(path):
        return True
    for p in path.iterdir():
        if ignore(p):
            continue
        if p.is_file():
            return False
        if not is_ess_empty(p, ignore=ignore, ess_empty=ess_empty):
            ess_empty[path] = False
            return False
    ess_empty[path] = True
    return True


def update_file(
    path: Path,
    regex: List[Tuple[str, str]] = None,
    exact: List[Tuple[str, str]] = None,
    append: Union[str, Iterable[str]] = None,
    exist_skip: bool = True,
) -> None:
    """Update a text file using regular expression substitution.

    :param regex: A list of tuples containing regular expression patterns
    and the corresponding replacement text.
    :param exact: A list of tuples containing exact patterns and the corresponding replacement text.
    :param append: A string of a list of lines to append.
    When append is a list of lines, "\n" is automatically added to each line.
    :param exist_skip: Skip appending if already exists.
    """
    if isinstance(path, str):
        path = Path(path)
    text = path.read_text()
    if regex:
        for pattern, replace in regex:
            text = re.sub(pattern, replace, text)
    if exact:
        for pattern, replace in exact:
            text = text.replace(pattern, replace)
    if append:
        if not isinstance(append, str):
            append = "\n".join(append)
        if not exist_skip or append not in text:
            text += append
    path.write_text(text)
