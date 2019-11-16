"""This module makes it easy to work with poetry to managing your Python project.
"""
import sys
import os
import glob
import shutil
from pathlib import Path
from typing import List
import subprocess as sp
import toml
from loguru import logger
from .filesystem import update_file
DIST = 'dist'
README = 'readme.md'
TOML = 'pyproject.toml'


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
        f'The current work directory {Path.cwd()} is not a (subdirectory of a) Python Poetry project.'
    )


def _project_name(proj_dir: Path) -> str:
    """Get the name of the project.
    :param proj_dir: The root directory of the Poetry project.
    :return: The name of the project.
    """
    return toml.load(proj_dir / TOML)['tool']['poetry']['name']


def _project_version(proj_dir: Path) -> str:
    """Get the version of the project.
    :param proj_dir: The root directory of the Poetry project.
    """
    return toml.load(proj_dir / TOML)['tool']['poetry']['version']


def _update_version_readme(ver: str, proj_dir: Path) -> None:
    """Update the version information in readme.
    :param ver: The new version.
    :param proj_dir: The root directory of the Poetry project.
    """
    pkg = _project_name(proj_dir)
    update_file(proj_dir / README, rf'{pkg}-\d+\.\d+\.\d+', f'{pkg}-{ver}')


def _update_version_toml(ver: str, proj_dir: Path) -> None:
    """Update the version information in the TOML file.
    :param ver: The new version.
    :param proj_dir: The root directory of the Poetry project.
    """
    update_file(proj_dir / TOML, r'version = .\d+\.\d+\.\d+.', f'version = "{ver}"')


def _update_version_init(ver: str, proj_dir: Path) -> None:
    """Update the version information in the file __init__.py.
    :param ver: The new version.
    :param proj_dir: The root directory of the Poetry project.
    """
    pkg = _project_name(proj_dir)
    update_file(proj_dir / pkg / "__init__.py",
        r'__version__ = .\d+\.\d+\.\d+.',
        f'__version__ = "{ver}"'
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
        sp.run(['git', 'diff'], check=True)


def _list_version(proj_dir: Path):
    print(_project_version(proj_dir))


def version(
    ver: str = '',
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
    else:
        _list_version(proj_dir)


def _format_code(inplace: bool = False, proj_dir: Path = None):
    if proj_dir is None:
        proj_dir = _project_dir()
    cmd = ['yapf', '-r']
    if inplace:
        cmd.append('-i')
        logger.info('Formatting code...')
    else:
        cmd.append('-d')
        logger.info('Checking code formatting...')
    pkg = _project_name(proj_dir)
    cmd.append(str(proj_dir / pkg))
    proc = sp.run(cmd, check=False, stdout=sp.PIPE)
    if proc.returncode:
        print(proc.stdout.decode())
        sys.stdout.flush()
        sys.stderr.flush()
        logger.warning('Please format the code (yapf -ir .)!')


def build_package(proj_dir: Path = None) -> None:
    """Build the package using poetry.
    :param dst_dir: The root directory of the project.
    :param proj_dir: The root directory of the Poetry project.
    """
    if proj_dir is None:
        proj_dir = _project_dir()
    if os.path.exists(DIST):
        shutil.rmtree(DIST)
    pkg = _project_name(proj_dir)
    try:
        logger.info(f'Checking code for errors (pylint -E {pkg}) ...')
        with open(os.devnull, 'w') as devnull:
            sp.run(['pylint', '-E', str(proj_dir / pkg)], check=True, stderr=devnull)
    except sp.CalledProcessError:
        logger.error('Please fix errors in code before building the package!')
        return
    _format_code(proj_dir=proj_dir)
    logger.info('Building the package...')
    sp.run("cd '{proj_dir}' && poetry env use python3 && poetry build", shell=True, check=True)


def install_package(options: List[str] = (), proj_dir: Path = None):
    """Install the built package.
    :param options: A list of options to pass to pip3.
    """
    if proj_dir is None:
        proj_dir = _project_dir()
    pkg = list(proj_dir.glob('dist/*.whl'))
    if not pkg:
        logger.error('No built package is found!')
        return
    if len(pkg) > 1:
        logger.error('Multiple built packages are found!')
        return
    cmd = ['pip3', 'install', '--user', '--upgrade', pkg[0]]
    cmd.extend(options)
    sp.run(cmd, check=True)
