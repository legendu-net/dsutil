import hashlib
from typing import Union, List, Tuple
from pathlib import Path


def rmd5(path: Union[str, Path]) -> List[Tuple[str, str]]:
    """Calculate md5sums recursively for the given path.
    :param path: The path of a file or directory.
    :returns: A sorted list of tuples of the format (file_path, md5sum).
    """
    md5sums = []
    _rmd5(Path(path), md5sums)
    return sorted(md5sums)


def _rmd5(path: Path, res: List[Tuple[str, str]]) -> None:
    """Helper function of rmd5.
    :param path: The Path object of a file or directory.
    :param res: A list to record the result.
    """
    if path.is_file():
        md5sum = hashlib.md5(path.read_text()).hexdigest()
        res.append((path.__str__(), md5sum))
        return
    for p in path.iterdir():
        _rmd5(p, res)
