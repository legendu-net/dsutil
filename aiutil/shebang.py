"""Update shebang of Python scripts in a directory.
"""
from typing import Union
from pathlib import Path
from argparse import ArgumentParser, Namespace
from magic import Magic


def _update_shebang(path: Path, shebang: str):
    with path.open("r") as fin:
        lines = fin.readlines()
    if lines[0].startswith("#!"):
        if "python" in lines[0]:
            lines[0] = shebang
    with path.open("w") as fout:
        fout.writelines(lines)


def update_shebang(script_dir: Union[Path, str], shebang: str):
    """Update the Shebang of scripts in the given script dir.

    :param script_dir: A directory containing scripts whose shebang are to be updated.
    :param shebang: The new shebang to use.
    """
    if isinstance(script_dir, str):
        script_dir = Path(script_dir)
    if not shebang.startswith("#!"):
        shebang = "#!" + shebang
    if not shebang.endswith("\n"):
        shebang += "\n"
    magic = Magic(mime=True, uncompress=True)
    for path in script_dir.iterdir():
        if path.is_file() and magic.from_file(str(path)).startswith("text/"):
            _update_shebang(path, shebang)


def parse_args(args=None, namespace=None) -> Namespace:
    """Parse command-line arguments.

    :param args: The arguments to parse.
        If None, the arguments from command line is parsed.
    :param namespace: An inital Namespace object.
    :return: A Namespace object containing parsed options.
    """
    parser = ArgumentParser(description="Update shebang of scripts.")
    parser.add_argument(
        "-d",
        "--script-dir",
        dest="script_dir",
        required=True,
        help="The directory containing scripts whose shebang are to be updated.",
    )
    parser.add_argument(
        "-s",
        "--sb",
        "--shebang",
        dest="shebang",
        required=True,
        help="The new shebang to use.",
    )
    return parser.parse_args(args=args, namespace=namespace)


def main():
    """Main function for the module."""
    args = parse_args()
    update_shebang(args.script_dir, args.shebang)


if __name__ == "__main__":
    main()
