#!/usr/bin/env python3
"""Filesystem related util functions.
"""
import os
import re
import shutil
from typing import Union, Iterable, Dict, List, Set, Callable
import math
from pathlib import Path
import subprocess as sp
from itertools import chain
import tempfile
from tqdm import tqdm
import nbformat
from loguru import logger
from yapf.yapflib.yapf_api import FormatCode
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
    except:
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
    except:
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

    :param paths: An iterable collection of paths.
    """
    freq = {}
    for path in paths:
        _count_path_helper(path, freq)
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
    if isinstance(root_dir, list):
        images = []
        for path in root_dir:
            images.extend(find_images(path))
        return images
    if isinstance(root_dir, str):
        root_dir = Path(root_dir)
    images = []
    _find_images(root_dir, images)
    return images


def _find_images(root_dir: Path, images: List):
    """Helper function of find_images.
    """
    for file in root_dir.iterdir():
        if file.is_file():
            if file.suffix == ".png":
                images.append(file)
        else:
            _find_images(file, images)


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
        if re.search("(git@|https://).*\.git", root):
            with tempfile.TemporaryDirectory() as tempdir:
                sp.run(f"git clone {root} {tempdir}", shell=True, check=True)
                logger.info(
                    f"The repo {root} is cloned to the local directory {tempdir}."
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
        "from\s+(\w+)\W*\s*",
        "from\s+(\w+\.\w+)\W*\s*",
        "join\s+(\w+)\W*\s*",
        "join\s+(\w+\.\w+)\W*\s*",
        "table\((\w+)\)",
        "table\((\w+\.\w+)\)",
        '"table":\s*"(\w+)"',
        '"table":\s*"(\w+\.\w+)"',
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


def _format_cell(cell: Dict, style_file: str) -> bool:
    """Format a cell in a Jupyter notebook.
    """
    if cell["cell_type"] != "code":
        return False
    code = cell["source"]
    lines = code.split("\n")
    if not lines:
        return False
    try:
        formatted, _ = FormatCode(code, style_config=style_file)
    except:
        return False
    # remove the trailing new line
    formatted = formatted.rstrip("\n")
    if formatted != code:
        cell["source"] = formatted
        return True
    return False


def format_notebook(path: str, style_file: str = ".style.yapf"):
    """Format code in a Jupyter/Lab notebook.
    :param path: Path to a notebook.
    :param style_file: [description], defaults to ".style.yapf"
    """
    notebook = nbformat.read(path, as_version=nbformat.NO_CONVERT)
    nbformat.validate(notebook)
    changed = False
    for cell in notebook.cells:
        changed |= _format_cell(cell, style_file=style_file)
    if changed:
        nbformat.write(notebook, path, version=nbformat.NO_CONVERT)
        logger.info("The notebook {} is formatted.", path)
    else:
        logger.info("No change is made to the notebook {}.", path)
