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


def _update_version_readme(ver: str, pkg: str):
    """Update the version information in readme.
    :param ver: The new version.
    :param pkg: The package name.
    """
    update_file(README, rf'{pkg}-\d+\.\d+\.\d+', f'{pkg}-{ver}')


def _update_version_toml(ver: str):
    """Update the version information in the TOML file.
    :param ver: The new version.
    """
    update_file(TOML, r'version = .\d+\.\d+\.\d+.', f'version = "{ver}"')


def _update_version_init(ver: str, pkg: str):
    """Update the version information in the file __init__.py.
    :param ver: The new version.
    :param pkg: The package name.
    """
    update_file(
        f'{pkg}/__init__.py', r'__version__ = .\d+\.\d+\.\d+.',
        f'__version__ = "{ver}"'
    )


def _update_version(ver: str, pkg: str):
    if ver:
        _update_version_init(ver=ver, pkg=pkg)
        _update_version_toml(ver)
        _update_version_readme(ver=ver, pkg=pkg)
        sp.run(['git', 'diff'], check=True)


def _list_version():
    ver = toml.load(TOML)['tool']['poetry']['version']
    print(ver)


def version(
    ver: str = '',
    pkg: str = '',
):
    """List or update the version of the package.
    :param ver: The new version to use.
        If empty, then the current version of the package is printed.
    :param pkg: The name of the package.
    """
    if not pkg:
        pkg = Path.cwd().name
    if ver:
        _update_version(ver=ver, pkg=pkg)
    else:
        _list_version()


def _format_code(inplace: bool = False, pkg: str = ''):
    if not pkg:
        pkg = Path.cwd().name
    cmd = ['yapf', '-r']
    if inplace:
        cmd.append('-i')
        logger.info('Formatting code...')
    else:
        cmd.append('-d')
        logger.info('Checking code formatting...')
    cmd.append(pkg)
    proc = sp.run(cmd, check=False, stdout=sp.PIPE)
    if proc.returncode:
        print(proc.stdout.decode())
        sys.stdout.flush()
        sys.stderr.flush()
        logger.warning('Please format the code (yapf -ir .)!')


def build_package(dst_dir: str = '', pkg: str = '') -> None:
    """Build the package using poetry.
    :param dst_dir: The root directory of the project.
    :param pkg: The name of the package.
    """
    if not pkg:
        pkg = Path.cwd().name
    if os.path.exists(DIST):
        shutil.rmtree(DIST)
    try:
        logger.info('Checking code for errors...')
        with open(os.devnull, 'w') as devnull:
            sp.run(['pylint', '-E', pkg], check=True, stderr=devnull)
    except sp.CalledProcessError:
        logger.error('Please fix errors in code before building the package!')
        return
    _format_code(pkg=pkg)
    logger.info('Building the package...')
    sp.run('poetry env use python3 && poetry build', shell=True, check=True)
    if not dst_dir:
        return
    logger.info(f'Copying the built package to {dst_dir}...')
    for file in glob.glob(os.path.join(dst_dir, f'{pkg}-*')):
        os.remove(file)
    for file in glob.glob('dist/*'):
        shutil.copy2(file, dst_dir)


def install_package(options: List[str] = ()):
    """Install the built package.
    :param options: A list of options to pass to pip3.
    """
    pkg = glob.glob('dist/*.whl')
    if not pkg:
        logger.error('No built package is found!')
        return
    if len(pkg) > 1:
        logger.error('Multiple built packages are found!')
        return
    cmd = ['pip3', 'install', '--user', '--upgrade', pkg[0]]
    cmd.extend(options)
    sp.run(cmd, check=True)
