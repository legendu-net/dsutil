from PyPDF2 import PdfFileWriter, PdfFileReader
from typing import List, Mapping, Sequence


def extract_pages(file: str, subfiles: Mapping[str, Sequence[int]]) -> None:
    """Extract pages from a PDF file and write into sub PDF file.
    :param file: The raw PDF file to extract pages from.
    :param subfiles: A dictionary specifying sub PDF files 
    and the corresponding list of pages from the raw PDF file.
    """
    fin = open(file, 'rb')
    reader = PdfFileReader(fin)
    for file, indexes in subfiles.items():
        _extract_pages(reader, indexes, file)
    fin.close()


def _extract_pages(reader: PdfFileReader, indexes: Sequence[int], output: str) -> None:
    """A helper function for extract_pages.
    :param reader: A PdfFileReader object.
    :param indexes: Index (0-based) of pages to extract.
    :param output: The path of the sub PDF file to write the extracted pages to.
    """
    writer = PdfFileWriter()
    for index in indexes:
        writer.addPage(reader.getPage(index))
    with open(output, 'wb') as fout:
        writer.write(fout)
