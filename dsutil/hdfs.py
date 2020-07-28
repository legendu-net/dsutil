"""Wrapping HDFS commands.
"""
import subprocess as sp
import pandas as pd
from loguru import logger
from .shell import to_frame
HDFS = "/apache/hadoop/bin/hdfs"


def ls(path: str, recursive: bool = False) -> pd.DataFrame:
    """Return the results of hdfs dfs -ls /hdfs/path as a DataFrame.
    :param path: A HDFS path.
    """
    cols = [
        'permissions',
        'replicas',
        'userid',
        'groupid',
        'filesize',
        'mdate',
        'mtime',
        'filename',
    ]
    cmd = f'{HDFS} dfs -ls {"-R" if recursive else ""} {path}'
    frame = to_frame(cmd, split=r' +', skip=0, header=cols)
    frame.mtime = pd.to_datetime(frame.mdate + ' ' + frame.mtime)
    frame.drop('mdate', axis=1, inplace=True)
    return frame


def count(path: str) -> pd.DataFrame:
    """Return the results of hdfs dfs -count -q -v /hdfs/path as a DataFrame.
    :param path: A HDFS path.
    """
    cmd = f'{HDFS} dfs -count -q -v {path}'
    frame = to_frame(cmd, split=r' +', header=0)
    frame.columns = frame.columns.str.lower()
    return frame


def du(path: str, depth: int = 1) -> pd.DataFrame:
    """Get the size of HDFS paths.
    :param path: A HDFS path.
    :param depth: The depth (by default 1) of paths to calculate sizes for.
    Note that any depth less than 1 is treated as 1.
    """
    index = len(path.rstrip('/'))
    if depth > 1:
        paths = ls(path, recursive=True).filename
        frames = [
            _du_helper(path) for path in paths if path[index:].count('/') + 1 == depth
        ]
        return pd.concat(frames)
    return _du_helper(path)


def _du_helper(path: str) -> pd.DataFrame:
    cmd = f'{HDFS} dfs -du {path}'
    frame = to_frame(cmd, split=r' +', header=['size', 'path'])
    return frame


def exists(path: str) -> bool:
    """Check if a HDFS path exist.
    :param path: A HDFS path.
    :return: True if the HDFS path exists and False otherwise.
    """
    # TODO: double check the implementation is good!
    cmd = f"{HDFS} dfs -test -e {path}"
    try:
        sp.run(cmd, shell=True, check=True)
        return True
    except sp.CalledProcessError:
        return False


def remove(path: str) -> None:
    """Remove a HDFS path.
    :param path: A HDFS path.
    """
    cmd = f"{HDFS} dfs -rm -r {path}"
    sp.run(cmd, shell=True, check=True)


def num_partitions(path: str) -> int:
    """Get the number of partitions of a HDFS path.
    :param path: A HDFS path.
    """
    cmd = f"{HDFS} dfs -ls {path}/part-* | wc -l"
    return int(sp.check_output(cmd, shell=True))


def get(path: str, dst_dir: str = '') -> None:
    """Download a HDFS path to local.
    :param path: A HDFS path.
    :param dst_dir: The local directory to download the HDFS path to.
    """
    cmd = f"{HDFS} dfs -get {path} {dst_dir}"
    sp.run(cmd, shell=True, check=True)
