"""Manipulating PDFs.
"""
from __future__ import annotations
from typing import Union, Iterable
from PyPDF2 import PdfFileWriter, PdfFileReader


def extract_pages(file: str, subfiles: dict[str, Union[int, Iterable[int]]]) -> None:
    """Extract pages from a PDF file and write into sub PDF file.

    :param file: The raw PDF file to extract pages from.
    :param subfiles: A dictionary specifying sub PDF files
        and their corresponding indexes (0-based) of pages from the raw PDF file.
        For example,
        the following code extract pages 0-4 as first.pdf, 
        pages 5 and 7 as second.pdf,
        and page 6 as third.pdf from raw.pdf.
        .. highlight:: python
        .. code-block:: python

        from dsutil.pdf import extract_pages
        extract_pages("raw.pdf", {"first.pdf": range(5), "second.pdf": [5, 7], "third.pdf": 6})
    """
    with open(file, "rb") as fin:
        reader = PdfFileReader(fin)
        for subfile, indexes in subfiles.items():
            _extract_pages(reader, indexes, subfile)


def _extract_pages(reader: PdfFileReader, indexes: Union[int, Iterable[int]], output: str) -> None:
    """A helper function for extract_pages.

    :param reader: A PdfFileReader object.
    :param indexes: Index (0-based) of pages to extract.
    :param output: The path of the sub PDF file to write the extracted pages to.
    """
    writer = PdfFileWriter()
    if isinstance(indexes, int):
        indexes = [indexes]
    for index in indexes:
        writer.addPage(reader.getPage(index))
    with open(output, "wb") as fout:
        writer.write(fout)
