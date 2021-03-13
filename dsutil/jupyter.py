#!/usr/bin/env python3
"""Jupyter/Lab notebooks related utils.
"""
import os
from typing import Union
from pathlib import Path
import subprocess as sp
import itertools as it
import tempfile
import nbformat
from loguru import logger
from nbconvert import HTMLExporter
from yapf.yapflib.yapf_api import FormatCode

HOME = Path.home()


def _format_cell(cell: dict, style_file: str) -> bool:
    """Format a cell in a Jupyter notebook.

    :param cell: A cell in the notebook.
    :param style_file: The path to a style file for formatting.
    :return: True if the cell is formatted (correctly) and False otherwise.
    """
    if cell["cell_type"] != "code":
        return False
    code = cell["source"]
    lines = code.split("\n")
    if not lines:
        return False
    try:
        formatted, _ = FormatCode(code, style_config=style_file)
    except Exception as err:
        logger.debug(
            "Failed to format the cell with the following code:\n{}"
            "\nThe following error message is thrown:\n{}", code, err
        )
        return False
    # remove the trailing new line
    formatted = formatted.rstrip("\n")
    if formatted != code:
        cell["source"] = formatted
        return True
    return False


def format_notebook(path: Union[str, Path], style_file: str = ""):
    """Format code in a Jupyter/Lab notebook.

    :param path: A (list of) path(s) to notebook(s).
    :param style_file: [description], defaults to ".style.yapf"
    """
    if not style_file:
        fd, style_file = tempfile.mkstemp()
        with os.fdopen(fd, "w") as fout:
            fout.write("[style]\nbased_on_style = facebook\ncolumn_limit = 88\n")
    if isinstance(path, (str, Path)):
        path = [path]
    for p in path:
        _format_notebook(p, style_file)


def nbconvert_notebooks(root_dir: Union[str, Path], cache: bool = False) -> None:
    """Convert all notebooks under a directory and its subdirectories using nbconvert.

    :param root_dir: The directory containing notebooks to convert.
    :param cache: If True, previously generated HTML files will be used if they are still update to date.
    """
    if isinstance(root_dir, str):
        root_dir = Path(root_dir)
    notebooks = root_dir.glob("**/*.ipynb")
    exporter = HTMLExporter()
    for notebook in notebooks:
        html = notebook.with_suffix(".html")
        if cache and html.is_file(
        ) and html.stat().st_mtime >= notebook.stat().st_mtime:
            continue
        code, _ = exporter.from_notebook_node(nbformat.read(notebook, as_version=4))
        html.write_text(code, encoding="utf-8")


def _format_notebook(path: Path, style_file: str):
    if isinstance(path, str):
        path = Path(path)
    if path.suffix != ".ipynb":
        raise ValueError(f"{path} is not a notebook!")
    logger.info('Formatting code in the notebook "{}".', path)
    notebook = nbformat.read(path, as_version=nbformat.NO_CONVERT)
    nbformat.validate(notebook)
    changed = False
    for cell in notebook.cells:
        changed |= _format_cell(cell, style_file=style_file)
    if changed:
        nbformat.write(notebook, path, version=nbformat.NO_CONVERT)
        logger.info('The notebook "{}" is formatted.\n', path)
    else:
        logger.info('No change is made to the notebook "{}".\n', path)


def _get_jupyter_paths():
    proc = sp.run("jupyter --path", shell=True, check=True, capture_output=True)
    lines = proc.stdout.decode().strip().split("\n")
    lines = (line.strip() for line in lines)
    return [line for line in lines if line.startswith("/")]


def _find_path_content(path, pattern):
    if isinstance(path, str):
        path = Path(path)
    for p in path.glob("**/*"):
        if p.is_file():
            try:
                if pattern in p.read_text():
                    yield p
            except:
                pass


def _find_path_path(path, pattern):
    if isinstance(path, str):
        path = Path(path)
    for p in path.glob("**/*"):
        if pattern in str(p):
            yield p


def find_jupyter_path(pattern, content: bool):
    """Find Jupyter/Lab paths match a pattern.

    :param pattern: The pattern to search for.
    :param content: If True, search file content for the pattern;
    otherwise, ssearch path name for the pattern.
    """
    paths = _get_jupyter_paths()
    if content:
        paths = [_find_path_content(path, pattern) for path in paths]
    else:
        paths = [_find_path_path(path, pattern) for path in paths]
    return list(it.chain.from_iterable(paths))
