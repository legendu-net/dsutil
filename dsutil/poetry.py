"""This module makes it easy to work with poetry to managing your Python project.
"""
import sys
import os
import shutil
from pathlib import Path
from typing import Union, List, Iterable
import subprocess as sp
import toml
from loguru import logger
from .filesystem import update_file
DIST = "dist"
README = "readme.md"
TOML = "pyproject.toml"


def _project_dir() -> Path:
    """Get the root directory of the Poetry project.
    :return: The root directory of the Poetry project.
    """
    path = Path.cwd()
    while path.parent != path:
        if (path / TOML).is_file():
            return path
        path = path.parent
    raise RuntimeError(
        f"The current work directory {Path.cwd()} is not a (subdirectory of a) Python Poetry project."
    )


def _project_name(proj_dir: Path) -> str:
    """Get the name of the project.
    :param proj_dir: The root directory of the Poetry project.
    :return: The name of the project.
    """
    return toml.load(proj_dir / TOML)["tool"]["poetry"]["name"]


def _project_version(proj_dir: Path) -> str:
    """Get the version of the project.
    :param proj_dir: The root directory of the Poetry project.
    """
    return toml.load(proj_dir / TOML)["tool"]["poetry"]["version"]


def _update_version_readme(ver: str, proj_dir: Path) -> None:
    """Update the version information in readme.
    :param ver: The new version.
    :param proj_dir: The root directory of the Poetry project.
    """
    update_file(proj_dir / README, regex=[(r"\d+\.\d+\.\d+", f"{ver}")])


def _update_version_toml(ver: str, proj_dir: Path) -> None:
    """Update the version information in the TOML file.
    :param ver: The new version.
    :param proj_dir: The root directory of the Poetry project.
    """
    update_file(
        proj_dir / TOML, regex=[(r"version = .\d+\.\d+\.\d+.", f'version = "{ver}"')]
    )


def _update_version_init(ver: str, proj_dir: Path) -> None:
    """Update the version information in the file __init__.py.
    :param ver: The new version.
    :param proj_dir: The root directory of the Poetry project.
    """
    pkg = _project_name(proj_dir)
    for path in (proj_dir / pkg).glob("**/*.py"):
        update_file(
            path, regex=[(r"__version__ = .\d+\.\d+\.\d+.", f'__version__ = "{ver}"')]
        )


def _update_version(ver: str, proj_dir: Path) -> None:
    """Update versions in files.
    :param ver: The new version.
    :param proj_dir: The root directory of the Poetry project.
    """
    if ver:
        _update_version_init(ver=ver, proj_dir=proj_dir)
        _update_version_toml(ver, proj_dir=proj_dir)
        _update_version_readme(ver=ver, proj_dir=proj_dir)
        sp.run(["git", "diff"], check=True)


def version(
    ver: str = "",
    commit: bool = False,
    proj_dir: Path = None,
):
    """List or update the version of the package.
    :param ver: The new version to use.
        If empty, then the current version of the package is printed.
    :param proj_dir: The root directory of the Poetry project.
    """
    if proj_dir is None:
        proj_dir = _project_dir()
    if ver:
        _update_version(ver=ver, proj_dir=proj_dir)
        if commit:
            cmd = f"cd {proj_dir} && git add . && git commit -m 'bump up version' && git push"
            sp.run(cmd, shell=True, check=False)
    else:
        print(_project_version(proj_dir))


def add_tag_release(proj_dir: Union[str, Path, None] = None) -> None:
    """Add a tag to the latest commit on the master branch for release.
    The tag is decided based on the current version of the project.

    :param tag: The tag (defaults to the current version of the package) to use.
    """
    # get current branch
    proc = sp.run(
        "git branch --show-current", shell=True, check=True, capture_output=True
    )
    current_branch = proc.stdout.decode().strip()
    # checkout the master branch
    sp.run("git checkout master && git pull", shell=True, check=True)
    print()
    # get tag
    if proj_dir is None:
        proj_dir = _project_dir()
    tag = "v" + _project_version(proj_dir)
    proc = sp.run(f"git tag -l {tag}", shell=True, check=True, capture_output=True)
    if proc.stdout:
        sp.run(f"git checkout {current_branch}", shell=True, check=True)
        raise ValueError(
            f"The tag {tag} already exists! Please merge new changes to the master branch first."
        )
    # tag the master branch
    sp.run(f"git tag {tag}", shell=True, check=True)
    # push tag
    proc = sp.run("git remote", shell=True, check=True, capture_output=True)
    for remote in proc.stdout.decode().strip().split("\n"):
        sp.run(f"git push {remote} {tag}", shell=True, check=True)
    print()
    # checkout the old branch
    sp.run(f"git checkout {current_branch}", shell=True, check=True)


def format_code(
    inplace: bool = False,
    commit: bool = False,
    proj_dir: Path = None,
    files: Iterable[Union[Path, str]] = ()
):
    """Format code.

    :param inplace: If true (defaults to False), format code inplace.
    Otherwise, changes are printed to terminal only.
    :param commit: If true (defaults to False),
    commit code formatting changes automatically.
    :param proj_dir: [description], defaults to None
    """
    cmd = ["yapf"]
    if inplace:
        cmd.append("-ir")
        logger.info("Formatting code...")
    else:
        cmd.append("-dr")
        logger.info("Checking code formatting...")
    if files:
        cmd.extend(files)
    else:
        if proj_dir is None:
            proj_dir = _project_dir()
        # source dir
        pkg = _project_name(proj_dir)
        cmd.append(str(proj_dir / pkg))
        # tests dir
        test = proj_dir / "tests"
        if test.is_dir():
            cmd.append(str(test))
    proc = sp.run(cmd, check=False, stdout=sp.PIPE)
    if proc.returncode:
        cmd[1] = "-ir"
        logger.warning(
            "Please format the code: {}\n{}", " ".join(cmd), proc.stdout.decode()
        )
        sys.stdout.flush()
        sys.stderr.flush()
    if inplace and commit:
        cmd = f"cd {proj_dir} && git add . && git commit -m 'format code' && git push"
        sp.run(cmd, shell=True, check=False)


def _lint_code(proj_dir: Union[Path, None], linter: Union[str, List[str]]):
    funcs = {
        "pylint": _lint_code_pylint,
        "pytype": _lint_code_pytype,
    }
    if isinstance(linter, str):
        linter = [linter]
    pyvenv_path = _pyvenv_path()
    for lint in linter:
        funcs[lint](proj_dir, pyvenv_path)


def _pyvenv_path() -> str:
    path = Path(".venv/pyvenv.cfg")
    if not path.is_file():
        return ""
    with path.open("r") as fin:
        for line in fin:
            if line.startswith("home = "):
                return line[7:].strip()
    return ""


def _lint_code_pytype(proj_dir: Union[Path, None], pyvenv_path: str):
    logger.info("Linting code using pytype ...")
    if not proj_dir:
        proj_dir = _project_dir()
    pkg = _project_name(proj_dir)
    if not pyvenv_path:
        pyvenv_path = _pyvenv_path()
    if pyvenv_path:
        pyvenv_path += ":"
    cmd = f"PATH={pyvenv_path}{proj_dir}/.venv/bin:$PATH pytype {proj_dir / pkg} {proj_dir / 'tests'}"
    try:
        sp.run(cmd, shell=True, check=True)
    except sp.CalledProcessError:
        logger.error("Please fix errors: {}", cmd)


def _lint_code_pylint(proj_dir: Union[Path, None], pyvenv_path: str):
    logger.info("Linting code using pylint ...")
    if not proj_dir:
        proj_dir = _project_dir()
    if not pyvenv_path:
        pyvenv_path = _pyvenv_path()
    pkg = _project_name(proj_dir)
    if not pyvenv_path:
        pyvenv_path = _pyvenv_path()
    if pyvenv_path:
        pyvenv_path += ":"
    cmd = f"PATH={pyvenv_path}{proj_dir}/.venv/bin:$PATH pylint -E {proj_dir / pkg}"
    try:
        sp.run(cmd, shell=True, check=True)
    except sp.CalledProcessError:
        logger.error("Please fix errors: {}", cmd)


def build_package(
    proj_dir: Union[Path, None] = None,
    linter: Union[str, Iterable[str]] = ("pylint", "pytype")
) -> None:
    """Build the package using poetry.
    :param dst_dir: The root directory of the project.
    :param proj_dir: The root directory of the Poetry project.
    """
    if proj_dir is None:
        proj_dir = _project_dir()
    if os.path.exists(DIST):
        shutil.rmtree(DIST)
    _lint_code(proj_dir=proj_dir, linter=linter)
    format_code(proj_dir=proj_dir)
    logger.info("Building the package...")
    sp.run(f"cd '{proj_dir}' && poetry build", shell=True, check=True)


def install_package(options: List[str] = (), proj_dir: Path = None):
    """Install the built package.
    :param options: A list of options to pass to pip3.
    """
    if proj_dir is None:
        proj_dir = _project_dir()
    pkg = list(proj_dir.glob("dist/*.whl"))
    if not pkg:
        logger.error("No built package is found!")
        return
    if len(pkg) > 1:
        logger.error("Multiple built packages are found!")
        return
    cmd = ["pip3", "install", "--user", "--upgrade", pkg[0]]
    cmd.extend(options)
    sp.run(cmd, check=True)
