#!/usr/bin/env python3
"""Jupyter/Lab notebooks related utils.
"""
from typing import Union
from pathlib import Path
import subprocess as sp
import itertools as it
import nbformat
from nbconvert import HTMLExporter

HOME = Path.home()


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
        if (
            cache
            and html.is_file()
            and html.stat().st_mtime >= notebook.stat().st_mtime
        ):
            continue
        code, _ = exporter.from_notebook_node(nbformat.read(notebook, as_version=4))
        html.write_text(code, encoding="utf-8")


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
