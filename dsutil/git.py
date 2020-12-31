#!/usr/bin/env python3
# encoding: utf-8

import subprocess as sp
from pathlib import Path
from typing import List, Set
from loguru import logger


def _git_status(gcmd) -> List[bytes]:
    proc = sp.run(gcmd + ["status"], check=True, stdout=sp.PIPE, stderr=sp.STDOUT)
    lines = [line.strip() for line in proc.stdout.splitlines()]
    # get rid of the leading #
    for idx, line in enumerate(lines):
        if line.startswith(b"#"):
            lines[idx] = line[1:].strip()
    return [line.lower() for line in lines if line != b""]


def _changes_status(status, changes: List[str]) -> None:
    mapping = {
        b"new file:": "new",
        b"deleted:": "deleted",
        b"modified:": "modified",
        b"renamed:": "renamed",
        b"untracked files:": "untracked",
        b"your branch is ahead": "unpushed",
    }
    for line in status:
        line = line.lower()
        for key in mapping:
            if line.startswith(key):
                changes.append(mapping.pop(key))
                break


def check(path: str = ".", file_mode: bool = False) -> None:
    """Check status of git repositories.
    Git submodule is not supported currently.

    :param path: The path of the directory under which Git repositories are to be checked.
    """
    path = Path(path).resolve()
    logger.info('Checking status of Git repositories under "{}"...', path)
    _check_helper(path=path, file_mode=file_mode)


def _check_helper(path: Path, file_mode: bool = False) -> None:
    if (path / ".git").is_dir():
        _check_git_repos(path, file_mode=file_mode)
        return
    for p in path.iterdir():
        if p.is_dir():
            _check_helper(p, file_mode=file_mode)


def _git_current_branch(gcmd) -> bytes:
    proc = sp.run(gcmd + ["branch"], check=True, stdout=sp.PIPE, stderr=sp.STDOUT)
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip() != b""]
    return next(line[1:].strip() for line in lines if line.startswith(b"*"))


def _git_remotes(gcmd) -> Set[bytes]:
    proc = sp.run(gcmd + ["remote", "-v"], check=True, stdout=sp.PIPE, stderr=sp.STDOUT)
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip() != b""]
    return set(line.split()[0] for line in lines)


def _check_git_repos(path, file_mode) -> None:
    gcmd = ["git", "-C", path, "-c", f"core.filemode={file_mode}"]
    status = _git_status(gcmd)
    changes = []
    _changes_status(status, changes)
    current_branch = _git_current_branch(gcmd)
    remotes = _git_remotes(gcmd)
    if not remotes:
        changes.append("no remote")
    if changes:
        logger.warning(f'{path} [{current_branch.decode()}]: {"|".join(changes)}')


def _startswith(status, keyword):
    for line in status:
        if line.startswith(keyword):
            return True
    return False
