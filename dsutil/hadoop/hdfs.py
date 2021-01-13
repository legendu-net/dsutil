"""Wrapping HDFS commands.
"""
from typing import List, Dict, Union
from pathlib import Path
import os
import subprocess as sp
import pandas as pd
from loguru import logger
from ..shell import to_frame
from ..filesystem import count_path


class Hdfs():
    """A class abstring the hdfs command.
    """
    def __init__(self, bin: str = "/apache/hadoop/bin/hdfs"):
        self.bin = bin

    def ls(self, path: str, recursive: bool = False) -> pd.DataFrame:
        """Return the results of hdfs dfs -ls /hdfs/path as a DataFrame.

        :param path: A HDFS path.
        :param recursive: If True, list content of the HDFS path recursively.
        :return: The result of hdfs dfs -ls as a pandas DataFrame.
        """
        cols = [
            "permissions",
            "replicas",
            "userid",
            "groupid",
            "bytes",
            "mdate",
            "mtime",
            "path",
        ]
        cmd = f'{self.bin} dfs -ls {"-R" if recursive else ""} {path}'
        logger.info("Running command: {}. Might take several minutes.", cmd)
        frame = to_frame(cmd, split=r" +", skip=0, header=cols)
        frame.bytes = frame.bytes.astype(int)
        frame.mtime = pd.to_datetime(frame.mdate + " " + frame.mtime)
        frame.drop("mdate", axis=1, inplace=True)
        return frame

    def count(self, path: str) -> pd.DataFrame:
        """Return the results of hdfs dfs -count -q -v /hdfs/path as a DataFrame.

        :param path: A HDFS path.
        :return: The result of hdfs dfs -count as a pandas DataFrame.
        """
        cmd = f"{self.bin} dfs -count -q -v {path}"
        frame = to_frame(cmd, split=r" +", header=0)
        frame.columns = frame.columns.str.lower()
        return frame

    def du(self, path: str, depth: int = 1) -> pd.DataFrame:
        """Get the size of HDFS paths.

        :param path: A HDFS path.
        :param depth: The depth (by default 1) of paths to calculate sizes for.
            Note that any depth less than 1 is treated as 1.
        :return: Disk usage of the HDFS path as a pandas DataFrame.
        """
        index = len(path.rstrip("/"))
        if depth > 1:
            paths = self.ls(path, recursive=True).filename
            frames = [
                self._du_helper(path)
                for path in paths if path[index:].count("/") + 1 == depth
            ]
            return pd.concat(frames)
        return self._du_helper(path)

    def _du_helper(self, path: str) -> pd.DataFrame:
        cmd = f"{self.bin} dfs -du {path}"
        frame = to_frame(cmd, split=r" +", header=["size", "path"])
        return frame

    def exists(self, path: str) -> bool:
        """Check if a HDFS path exist.

        :param path: A HDFS path.
        :return: True if the HDFS path exists and False otherwise.
        """
        cmd = f"{self.bin} dfs -test -e {path}"
        try:
            sp.run(cmd, shell=True, check=True)
            return True
        except sp.CalledProcessError:
            return False

    def remove(self, path: str) -> None:
        """Remove a HDFS path.
        :param path: A HDFS path.
        """
        cmd = f"{self.bin} dfs -rm -r {path}"
        sp.run(cmd, shell=True, check=True)

    def num_partitions(self, path: str) -> int:
        """Get the number of partitions of a HDFS path.

        :param path: A HDFS path.
        :return: The number of partitions under the HDFS path.
        """
        cmd = f"{self.bin} dfs -ls {path}/part-* | wc -l"
        return int(sp.check_output(cmd, shell=True))

    def get(
        self,
        hdfs_path: str,
        local_dir: Union[str, Path] = "",
        is_file: bool = False
    ) -> None:
        """Download data from HDFS into a local directory. 

        :param hdfs_path: The HDFS path (can be both a file or a directory) to copy.
        :param local_dir: The local directory to copy HDFS files into.
        :param is_file: A boolean indicator of whether the HDFS path is a file or a directory.
        """
        if isinstance(local_dir, str):
            local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        if is_file:
            cmd = f"{self.bin} dfs -get {hdfs_path} {local_dir}"
        else:
            cmd = f"{self.bin} dfs -get {hdfs_path}/* {local_dir}"
        sp.run(cmd, shell=True, check=True)
        print(
            f"Content of the HDFS path {hdfs_path} has been fetch into the local directory {local_dir}"
        )

    @staticmethod
    def _file_size_1(path: str, size: int, dir_size: Dict[str, int]):
        while path != "/":
            path = os.path.dirname(path)
            dir_size.setdefault(path, 0)
            dir_size[path] += size

    def _file_size(self, files):
        dir_size = {}
        for path, bytes_ in files.bytes[~files.permissions.str.
                                        startswith("d")].iteritems():
            self._file_size_1(path, bytes_, dir_size)
        return dir_size

    def count_path(self, path: str) -> pd.Series:
        """Count frequence of paths and their parent paths.

        :param path: An iterable collection of paths.
        :return: Frequency of paths as a pandas Series.
        """
        frame = self.ls(path, recursive=True)
        return count_path(frame.path)

    def size(self, path: str) -> pd.DataFrame:
        """Calculate sizes of subdirs and subfiles under a path.

        :param path: A HDFS path.
        :return: Size information of the HDFS path as a pandas DataFrame.
        """
        files = self.ls(path, recursive=True)
        files.set_index("path", inplace=True)
        dir_size = self._file_size(files)
        bytes_ = pd.Series(dir_size, name="bytes")
        files.update(bytes_)
        files.reset_index(inplace=True)
        files.insert(6, "metabytes", round(files.bytes / 1E6, 2))
        return files.sort_values("bytes", ascending=False)

    def mkdir(self, path: str) -> None:
        """Create a HDFS path.

        :param path: The HDFS path to create.
        """
        cmd = f"{self.bin} dfs -mkdir -p {path}"
        sp.run(cmd, shell=True, check=True)
        print(f"The HDFS path {path} has been created.")

    def put(
        self,
        local_path: Union[str, Path],
        hdfs_path: str,
        create_hdfs_path: bool = False
    ) -> None:
        """Copy data from local to HDFS.
        :param local_path: A local path to copy to HDFS.
        :param hdfs_path: The HDFS path/directory to copy data into.
        :param create_hdfs_path: If true, create the HDFS path if not exists.
        """
        if create_hdfs_path:
            self.mkdir(hdfs_path)
        cmd = f"{self.bin} dfs -put -f {local_path} {hdfs_path}"
        sp.run(cmd, shell=True, check=True)
        print(
            f"The local path {local_path} has been uploaded into the HDFS path {hdfs_path}"
        )

    def fetch_partition_names(self,
                              path: str,
                              extension: str = ".parquet") -> List[str]:
        """Get Parquet partition names (with the parent directory) under a HDFS path.

        :param path: A HDFS path.
        :param extension: Return partitions with the specified file extension.
        :return: A list of the partition names (with the parent directory).
        """
        paths = self.ls(path).path
        return [path for path in paths if path.endswith(extension)]
