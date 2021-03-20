"""Module for log filtering.
"""
from __future__ import annotations
from typing import Union, Optional, Sequence, TextIO
from pathlib import Path
import sys
import re
from collections import deque
from difflib import SequenceMatcher
from tqdm import tqdm
from loguru import logger

DASH_50 = "-" * 50
DASH_100 = "-" * 100


class LogDeduper:
    """Dedup similar log lines.
    """
    def __init__(self, threshold: float = 0.5):
        self._lines = []
        self._threshold = threshold

    def similarity(self, line):
        """Calcualte similarity between 2 lines.

        :param line1: A line of logging message.
        :param line2: Another line of logging message.
        :return: A similarity score (between 0 and 1) between the 2 lines.
        """
        return max(
            (
                SequenceMatcher(None, line, target).ratio()
                for target in reversed(self._lines)
            ),
            default=0
        )

    def add(self, line, line_num):
        """Add a line.

        :param line: A line of logging message.
        :param line_num: The row number (0-based) of the line.
        """
        if self.similarity(line) < self._threshold:
            self._lines.append(f"L{line_num}: {line}\n")

    def write(self, fout: TextIO):
        """Write deduplicated log into a file.

        :param fout: A file handler for outputing log.
        """
        for line in self._lines:
            fout.write(line)


class LogFilter:
    """A class for log filtering.
    """
    KEYWORDS = (
        "spark.yarn.executor.memoryOverhead",
        "not found",
        "OOM",
        "Error",
        "error",
        "Exception",
        "exception",
    )
    PATTERNS = (
        (r"\d{2,}[-/]\d{2,}[-/]\d{2,}\s\d+:\d+:\d+", "YYYY/MM/DD HH:MM:SS"),
        (r"\d{,3}.\d{,3}.\d{,3}.\d{,3}(:\d+)?", "0.0.0.0:0"),
        (r"streamId=\d+", "streamId=*"),
        (r"chunkIndex=\d+", "chunkIndex=*"),
        (r"send RPC \d+", "send RPC *"),
    )

    def __init__(
        self,
        log_file: Union[str, Path],
        context_size: int = 5,
        keywords: Sequence[str] = KEYWORDS,
        patterns: Sequence[str] = PATTERNS,
        output: Union[str, Path] = "",
        threshold: float = 0.5,
        dump_by_keyword: bool = False,
    ):
        self._log_file = (log_file
                          if isinstance(log_file, Path) else Path(log_file)).resolve()
        self._context_size: int = context_size
        self._keywords: Sequence[str] = keywords
        self._patterns: Sequence[str] = patterns
        self._num_rows: int = 0
        self._lookup: dict[str, dict[str, int]] = {kwd: {} for kwd in self._keywords}
        self._queue: deque = deque()
        self._output: str = self._get_output(output)
        self._threshold: float = threshold
        self._dump_by_keyword: bool = dump_by_keyword

    def _get_output(self, output: Union[str, Path]) -> Path:
        """Get a valid output file.

        :param output: The path to the output file.
        """
        if output == "" or Path(output).resolve() == self._lookup:
            return self._log_file.with_name(
                self._log_file.stem + "_s" + self._log_file.suffix
            )
        if isinstance(output, str):
            output = Path(output)
        return output.resolve()

    def _regularize(self, line) -> str:
        """Get rid of substrings with patterns specified by the regular expressions.

        :param line: A line of logging message.
        :return: The regularized the line message.
        """
        for pattern, replace in self._patterns:
            line = re.sub(pattern, replace, line)
        return line

    def _dump_queue(self, lines) -> None:
        """Dump content in the queue.

        :param lines: A list to dump the queue to.
        """
        lines.append(DASH_100 + "\n")
        lines.extend(self._queue)
        self._queue.clear()

    def _keep(self, idx: int, line: str) -> bool:
        """Check whether the line should be kept.

        :param idx: The original row number (0-based) of the line. 
        :param line: A line of logging message.
        :return: True if the line is to be kept and False otherwise.
        """
        if " ./" in line or "-XX:OnOutOfMemoryError=" in line:
            return False
        for kwd in self._keywords:
            if kwd in line:
                line = self._regularize(line)
                if line in self._lookup[kwd]:
                    return False
                self._lookup[kwd][line] = idx
                return True
        return False

    def _count_rows(self):
        """Count the total number of rows.
        """
        if self._num_rows:
            return
        logger.info("Counting total number of rows ...")
        with open(self._log_file, "r") as fin:
            self._num_rows = sum(1 for line in fin)
        logger.info("Total number of rows: {:,}", self._num_rows)

    def _scan_error_lines(self) -> None:
        print()
        logger.info("Scanning for error lines in the log ...")
        lines = [DASH_50 + " Possible Error Lines " + DASH_50 + "\n"]
        with open(self._log_file, "r") as fin:
            dump_flag = -1
            for idx, line in tqdm(enumerate(fin), total=self._num_rows):
                self._queue.append(f"L{idx}: {line}")
                keep = self._keep(idx, line)
                if keep:
                    dump_flag = 0
                    continue
                if dump_flag == -1:
                    if len(self._queue) > self._context_size:
                        self._queue.popleft()
                    continue
                dump_flag += 1
                if dump_flag >= self._context_size:
                    self._dump_queue(lines)
                    dump_flag = -1
            if dump_flag >= 0:
                self._dump_queue(lines)
        with open(self._output, "w") as fout:
            fout.writelines(lines)
        logger.info("Possible Error Lines have been dumped into {}", self._output)

    def filter(self) -> None:
        """Filter informative liens from a Spark application log.
        """
        self._count_rows()
        self._scan_error_lines()
        self._dedup_log()

    def _dedup_log(self):
        print()
        # create dir for dumping errors by keyword
        if self._dump_by_keyword:
            dir_ = self._output.parent / (self._log_file.stem + "_k")
            dir_.mkdir(parents=True, exist_ok=True)
            logger.info(
                "Error lines will be dumped by keyword into the directory {}.", dir_
            )
        else:
            dir_ = None
        # dedup error lines
        fout = open(self._output, "a")
        fout.write("\n" + DASH_50 + " Deduped Error Lines " + DASH_50 + "\n")
        for kwd, lines in self._lookup.items():
            if not lines:
                continue
            logger.info('Deduplicating error lines corresponding to "{}" ...', kwd)
            self._dedup_log_1(kwd, lines, fout, dir_)
        fout.close()

    def _dedup_log_1(
        self, kwd: str, lines: dict[str, int], fout: TextIO, dir_: Optional[Path]
    ) -> None:
        deduper = LogDeduper(self._threshold)
        lines = sorted(lines.items())
        for line, idx in tqdm(lines):
            deduper.add(line, idx)
        deduper.write(sys.stdout)
        deduper.write(fout)
        if not self._dump_by_keyword:
            return
        with (dir_ / kwd).open("w") as fout_kwd:
            for line, idx in lines:
                fout_kwd.write(f"L{idx}: {line}\n")
