"""PYPI related utils.
"""
from typing import Set
import re
import requests


def dep(pkg: str, recursive: bool = False) -> set[str]:
    """Parse dependencies of the specified Python package.

    :param pkg: The Python package whose dependencies to parse.
    :param recursive: If true, pass dependencies recursively.
    :return: A set of Python package names
        which are dependencies of the specified Python package.
    """
    if recursive:
        return _dep_recur(pkg)
    return _dep(pkg)


def _dep(pkg):
    url = f"https://pypi.org/pypi/{pkg}/json"
    deps = requests.get(url, timeout=10).json()["info"]["requires_dist"]
    if deps is None:
        return set()
    return set(dep for dep in deps if "extra ==" not in dep)


def _dep_recur(pkg: str):
    deps = set()
    _dep_recur_helper(pkg, deps)
    return deps


def _dep_recur_helper(pkg: str, deps: Set[str]):
    for dep in _dep(pkg):
        dep = re.split(r" |;|\(|\[", dep, maxsplit=1)[0]
        if dep not in deps:
            _dep_recur_helper(dep, deps)
            deps.add(dep)
