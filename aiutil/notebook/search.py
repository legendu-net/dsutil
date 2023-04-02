#!/usr/bin/env python3
"""Search Jupyter/Lab notebooks.
"""
from typing import TypeAlias, Sequence
import json
from pathlib import Path
from collections import Counter
from argparse import ArgumentParser, Namespace
from loguru import logger

Criterion: TypeAlias = str | list[str] | dict[str, list[str]]


def _reg_criterion(criterion: str | list[str] | dict[str, list[str]]):
    """Convert different criterion specification into universal dict specification."""
    if isinstance(criterion, str):
        if criterion == "":
            criterion = []
        else:
            criterion = [criterion]
    if isinstance(criterion, list):
        criterion = {"include": criterion, "exclude": []}
    return criterion


class Cell:
    """Class representing a cell in a notebook."""

    def __init__(self, cell: dict):
        """Initialize a cell.
        :param cell: A cell in the dict representation.
        """
        self._cell = cell
        self._source = self.source(0)

    def source(self, indent: int = 0) -> str:
        """Get the source code in the cell.
        :param indent: The number of spaces to add before each line of code.
        """
        return "".join(" " * indent + line for line in self._cell["source"])

    def match_keyword(self, keyword: Criterion) -> bool:
        """Match source code of this cell against the specified keyword criterion.
        :param keyword: A keyword criterion.
        """
        kwd = _reg_criterion(keyword)
        return all(k in self._source for k in kwd["include"]) and not any(
            k in self._source for k in kwd["exclude"]
        )

    def match_type(self, type_: str) -> bool:
        """Match this cell's type against the specified one.
        :param type_: The type of cell to match.
        If empty, it matches all types.
        """
        if type_ == "":
            return True
        return self._cell["cell_type"] == type_


class Notebook:
    """A class representing a Jupyter/Lab notebook."""

    def __init__(self, path: str | Path):
        """Initialize a notebook.
        :param path: The path to a notebook.
        """
        self.path = Path(path) if isinstance(path, str) else path
        self._notebook = self._read_notebook()
        self.lang = self._get_lang().lower()
        self._cells = [Cell(cell) for cell in self._notebook["cells"]]

    def _get_lang(self) -> str:
        """Get the language of the notebook."""
        if "metadata" not in self._notebook:
            return "python"
        metadata = self._notebook["metadata"]
        if "kernelspec" in metadata:
            kernelspec = metadata["kernelspec"]
            if "language" in kernelspec:
                return kernelspec["language"]
            if "name" in kernelspec:
                return kernelspec["name"]
        elif "language_info" in metadata:
            return metadata["language_info"]["name"]
        return "python"

    def _read_notebook(self) -> dict:
        """Read the content of the notebook as a dict."""
        with self.path.open(encoding="utf-8") as fin:
            return json.load(fin)

    def match_language(self, language: Criterion) -> bool:
        """Match the language of the notebook against the specified language criterion.
        :param language: A language criterion.
        """
        lang = _reg_criterion(language)
        return all(self.lang == l.lower() for l in lang["include"]) and not any(
            self.lang == l.lower() for l in lang["exclude"]
        )

    def cells(self, keyword: Criterion, type_: str = "") -> list[Cell]:
        """Get a list of cells in this notebook."""
        return [
            cell
            for cell in self._cells
            if cell.match_type(type_) and cell.match_keyword(keyword)
        ]

    def __repr__(self) -> str:
        return f"Notebook({self.path})"


def print_nb_cells(
    nb_cells: tuple[tuple[Notebook, Cell], ...], num_notebooks: int, num_cells: int
):
    """Print cells of notebooks.
    :param nb_cells: Notebook cells to print.
    :param num_notebooks: The max number of notebooks to print.
    :param num_cells: The max number of cells per notebook to print.
    """
    n = len(nb_cells)
    print(f"Matched {n} notebooks")
    print(
        f"Display {min(n, num_notebooks)} notebooks each with up to {num_cells} cells\n"
    )
    for nb, cells in nb_cells[:num_notebooks]:
        print(f"{nb.path}: {nb.lang}")
        for idx, cell in enumerate(cells[:num_cells]):
            print(
                f"    ------------------------------------ Cell {idx} ------------------------------------"
            )
            print(cell.source(4))
        print(
            "========================================================================================\n\n"
        )


def search_notebooks(
    notebooks: list[Notebook],
    keyword: Criterion = "",
    type_: str = "",
    language: Criterion = "",
):
    """Search notebooks using the specified criterions.
    :param notebooks: A list of notebooks among which to search.
    :param keyword: A keyword criterion.
    :param type_: A type criterion.
    :param language: A language criterion.
    """
    notebooks = [nb for nb in notebooks if nb.match_language(language)]
    return tuple((nb, cells) for nb in notebooks if (cells := nb.cells(keyword, type_)))


def list_languages(notebooks: list[Notebook]) -> list[tuple[str, int]]:
    """List languages used in notebooks.
    :param notebooks: A list of notebooks.
    """
    counter = Counter(nb.lang for nb in notebooks)
    counter = list(counter.items())
    counter.sort(key=lambda t: -t[1])
    return counter


def find_notebooks(paths: Sequence[str]) -> list[Notebook]:
    """Find all notebooks under the given paths.
    :param paths: A list of paths.
    """
    notebooks = set()
    for path in paths:
        path = Path(path)
        if path.is_file():
            if path.suffix == ".ipynb":
                notebooks.add(path)
            else:
                logger.warning(f"The file {path} is not a notebook!")
        elif path.is_dir():
            for p in path.glob("**/*.ipynb"):
                notebooks.add(p)
    return [Notebook(path) for path in notebooks]


def _list_langs_args(args):
    notebooks = find_notebooks(args.paths)
    counter = list_languages(notebooks)
    for lang, freq in counter:
        print(f"{lang}: {freq}")
    print()


def _search_notebooks_args(args):
    notebooks = find_notebooks(args.paths)
    lang = {
        "include": args.lang_include,
        "exclude": args.lang_exclude,
    }
    kwd = {
        "include": args.kwd_include,
        "exclude": args.kwd_exclude,
    }
    nb_cells = search_notebooks(notebooks, keyword=kwd, language=lang)
    print_nb_cells(nb_cells, args.num_notebooks, args.num_cells)


def parse_args(args=None, namespace=None) -> Namespace:
    """Parse command-line arguments.
    :param args: The arguments to parse.
        If None, the arguments from command-line are parsed.
    :param namespace: An inital Namespace object.
    """
    parser = ArgumentParser(description="Search for notebooks.")
    subparsers = parser.add_subparsers(dest="sub_cmd", help="Sub commands.")
    _subparse_search(subparsers)
    _subparse_list(subparsers)
    return parser.parse_args(args=args, namespace=namespace)


def _subparse_list(subparsers):
    subparser_list = subparsers.add_parser(
        "list",
        aliases=["l", "ls"],
        help="List languages used by notebooks.",
    )
    subparser_list.add_argument(
        "-p",
        "--paths",
        dest="paths",
        nargs="+",
        required=True,
        help="Paths to notebooks or directories containing notebooks.",
    )
    subparser_list.set_defaults(func=_list_langs_args)


def _subparse_search(subparsers):
    subparser_search = subparsers.add_parser(
        "search",
        aliases=["s"],
        help="Search for notebooks.",
    )
    subparser_search.add_argument(
        "-p",
        "--paths",
        dest="paths",
        nargs="+",
        required=True,
        help="Paths to notebooks or directories containing notebooks.",
    )
    subparser_search.add_argument(
        "-l",
        "--lang-include",
        dest="lang_include",
        nargs="*",
        default=(),
        help="The language of notebooks.",
    )
    subparser_search.add_argument(
        "-L",
        "--lang-exclude",
        dest="lang_exclude",
        nargs="*",
        default=(),
        help="Languages that notebooks shouldn't include.",
    )
    subparser_search.add_argument(
        "-k",
        "--kwd-include",
        dest="kwd_include",
        nargs="*",
        default=(),
        help="Keywords to search for in cells of notebooks.",
    )
    subparser_search.add_argument(
        "-K",
        "--kwd-exclude",
        dest="kwd_exclude",
        nargs="*",
        default=(),
        help="Keywords that cells of notebooks shouldn't include.",
    )
    subparser_search.add_argument(
        "-n",
        "--num-notebooks",
        dest="num_notebooks",
        type=int,
        default=10,
        help="Number of matched notebooks to display.",
    )
    subparser_search.add_argument(
        "-c",
        "--num-cells",
        dest="num_cells",
        type=int,
        default=10,
        help="Number of matched cells in each notebook to display.",
    )
    subparser_search.set_defaults(func=_search_notebooks_args)


def main() -> None:
    """The main function of the script."""
    args = parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
