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
    def __init__(self, threshold: float = 0.7):
        self.lines = []
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
                for target in reversed(self.lines)
            ),
            default=0
        )

    def add(self, line, line_num):
        """Add a line.

        :param line: A line of logging message.
        :param line_num: The row number (0-based) of the line.
        """
        if self.similarity(line) < self._threshold:
            self.lines.append(f"L{line_num}: {line}")

    def write(self, fout: TextIO):
        """Write deduplicated log into a file.

        :param fout: A file handler for outputing log.
        """
        for line in self.lines:
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
        "failures",
    )
    PATTERNS = (
        (r"\d{2,}[-/]\d{2,}[-/]\d{2,}\s\d+:\d+:\d+", "YYYY/MM/DD HH:MM:SS"),
        (r"\d{,3}\.\d{,3}\.\d{,3}\.\d{,3}(:\d+)?", "0.0.0.0:0"),
        (r"streamId=\d+", "streamId=*"),
        (r"chunkIndex=\d+", "chunkIndex=*"),
        (r"send RPC \d+", "send RPC *"),
    )

    def __init__(
        self,
        log_file: Union[str, Path],
        context_size: int = 5,
        keywords: Sequence[str] = KEYWORDS,
        patterns: Sequence[tuple[str, str]] = PATTERNS,
        output: Union[str, Path] = "",
        threshold: float = 0.7,
        dump_by_keyword: bool = False,
    ):
        self._log_file = (log_file
                          if isinstance(log_file, Path) else Path(log_file)).resolve()
        self._context_size: int = context_size
        self._keywords: Sequence[str] = keywords
        self._patterns: Sequence[tuple[str, str]] = patterns
        self._num_rows: int = 0
        self._lookup: dict[str, dict[str, int]] = {kwd: {} for kwd in self._keywords}
        self._queue: deque = deque()
        self._output: Path = self._get_output(output)
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
        with open(self._log_file, "r", encoding="utf-8") as fin:
            self._num_rows = sum(1 for line in fin)
        logger.info("Total number of rows: {:,}", self._num_rows)

    def _scan_error_lines(self) -> None:
        print()
        logger.info("Scanning for error lines in the log ...")
        lines = [DASH_50 + " Possible Error Lines " + DASH_50 + "\n"]
        with open(self._log_file, "r", encoding="utf-8") as fin:
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
        with open(self._output, "w", encoding="utf-8") as fout:
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
        lines_unique = []
        for kwd, lines in self._lookup.items():
            if not lines:
                continue
            logger.info('Deduplicating error lines corresponding to "{}" ...', kwd)
            lines_unique.extend(self._dedup_log_1(kwd, lines, dir_))
        lines_unique = [self._error_priority(line) for line in lines_unique]
        lines_unique.sort()
        with self._output.open("a", encoding="utf-8") as fout:
            self._write_lines_unique(lines_unique, fout)
        self._write_lines_unique(lines_unique, sys.stdout)

    @staticmethod
    def _write_lines_unique(lines_unique: list[tuple[int, str, str]], fout: TextIO):
        fout.write("\n" + DASH_50 + " Deduped Error Lines " + DASH_50 + "\n")
        fout.write(
            "http://www.legendu.net/misc/blog/A-comprehensive-list-of-issues-in-spark-applications\n\n"
        )
        for _, line, url in lines_unique:
            fout.write(line)
            fout.write(f"Possible causes and solutions: {url}\n\n")

    @staticmethod
    def _error_priority(line: str) -> tuple[int, str, str]:
        """Return priority (with a smaller value means higher priority) of an error line.

        :param line: An error line.
        :return: The priority of the error line.
        """
        patterns = [
            (
                "SIGILL", 1,
                "http://www.legendu.net/en/blog/spark-issue:-shell-related"
            ),
            (
                "(?i)command not found", 1,
                "http://www.legendu.net/en/blog/spark-issue:-shell-related"
            ),
            (
                "(?i)panicked at.*RUST_BACKTRACE", 1,
                "http://www.legendu.net/misc/blog/spark-issue:-rust-panic/"
            ),
            (
                "(?i)libc.*not found", 1,
                "http://www.legendu.net/misc/blog/spark-issue-libc-not-found/"
            ),
            (
                r"(?i)ArrowInvalid", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                r"(?i)TypeError: .*has no len()", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                r"(?i)CalledProcessError: Command .* returned non-zero exit status", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                r"(?i)error: Found argument .* which wasn't expected", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                r"(?i)RuntimeError: Result vector from pandas_udf was not the required length",
                1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                "(?i)object has no attribute", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                "(?i)No such file or directory:", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                "(?i)error: the following arguments are required:", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                "(?i)error: unrecognized arguments:", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                "(?i)error: argument", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                "(?i)ModuleNotFoundError: No module named", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                "(?i)SyntaxError: invalid syntax", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                "(?i)NameError: name .* is not defined", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                "(?i)IndentationError: unexpected indent", 1,
                "http://www.legendu.net/misc/blog/Spark-issue:-Pure-Python-code-errors"
            ),
            (
                "(?i)NSQuotaExceededException", 1,
                "http://www.legendu.net/misc/blog/spark-issue:-namespace-quota-is-exceeded"
            ),
            (
                r"(?i)RuntimeException: Unsupported literal type class", 1,
                " http://www.legendu.net/en/blog/spark-issue:-RuntimeException:-unsupported-literal-type-class"
            ),
            (
                r"(?i)TypeError: withReplacement", 1,
                "http://www.legendu.net/misc/blog/spark-issue:-TypeError-withReplacement"
            ),
            (
                "(?i)table not found", 1,
                "http://www.legendu.net/misc/blog/spark-issue-table-not-found/"
            ),
            (
                "(?i)SparkContext: A master URL must be set", 1,
                "http://www.legendu.net/misc/blog/spark-issue-a-master-url-must-be-set-in-your-configuration/"
            ),
            (
                r"(?i)org\.apache\.spark\.sql\.AnalysisException.*cannot resolve", 1,
                "http://www.legendu.net/misc/blog/spark-issue-AnalysisException-cannot-resolve"
            ),
            (
                r"(?i)org\.apache\.spark\.sql\.AnalysisException.*Path does not exist",
                1,
                "http://www.legendu.net/misc/blog/spark-issue-AnalysisException-Path-does-not-exist"
            ),
            (
                r"(?i)org\.apache\.hadoop\.security\.AccessControlException.*Permission denied",
                1,
                "http://www.legendu.net/misc/blog/Spark-Issue-AccessControlException-Permission-denied"
            ),
            (
                r"(?i)org\.apache\.spark\.InsertOperationConflictException.*Failed to hold insert operation lock",
                1,
                "http://www.legendu.net/misc/blog/spark-issue-InsertOperationConflictException-failed-to-hold-insert-operation-lock"
            ),
            (
                r"(?i)max number of executor failures", 1,
                "http://www.legendu.net/misc/blog/spark-issue:-max-number-of-executor-failures-reached"
            ),
            (
                r"(?i)IllegalArgumentException: System memory \d* must be at least", 1,
                "http://www.legendu.net/misc/blog/spark-issue:-IllegalArgumentException:-System-memory-must-be-at-least"
            ),
            (
                r"(?i)InvalidResourceRequestException", 1,
                "http://www.legendu.net/misc/blog/spark-issue:-InvalidResourceRequestException"
            ),
            (
                "(?i)serialized results is bigger than spark.driver.maxResultSize", 2,
                "http://www.legendu.net/misc/blog/spark-issues-total-size-bigger-than-maxresultsize/"
            ),
            (
                "(?i)Block rdd_.* could not be removed as it was not found on disk or in memory",
                2,
                "http://www.legendu.net/misc/blog/spark-issue:-block-could-not-be-removed-as-it-was-not-found-on-disk-or-in-memory"
            ),
            (
                r"(?i)java\.io\.FileNotFoundException", 2,
                "http://www.legendu.net/misc/blog/spark-issue-file-not-found-exception/"
            ),
            (
                "(?i)Container killed by YARN for exceeding memory limits", 2,
                "http://www.legendu.net/misc/blog/spark-issue-Container-killed-by-YARN-for-exceeding-memory-limits/"
            ),
            (
                r"(?i)org\.apache\.spark\.memory\.SparkOutOfMemoryError", 3,
                "http://www.legendu.net/misc/tag/spark-issue.html"
            ),
            (
                "(?i)Container from a bad node", 3,
                "http://www.legendu.net/misc/tag/spark-issue.html"
            ),
            (
                "(?i)No live nodes contain block BP", 3,
                "http://www.legendu.net/misc/tag/spark-issue.html"
            ),
            (
                "(?i)ReplicaNotFoundException: Replica not found for", 3,
                "http://www.legendu.net/misc/tag/spark-issue.html"
            ),
        ]
        for pattern, priority, url in patterns:
            if re.search(pattern, line):
                return priority, line, url
        return 10, line, "http://www.legendu.net/misc/tag/spark-issue.html"

    def _dedup_log_1(self, kwd: str, lines: dict[str, int],
                     dir_: Optional[Path]) -> list[str]:
        deduper = LogDeduper(self._threshold)
        lines = sorted(lines.items())
        for line, idx in tqdm(lines):
            deduper.add(line, idx)
        #deduper.write(sys.stdout)
        #deduper.write(fout)
        if self._dump_by_keyword:
            with (dir_ / kwd).open("w") as fout_kwd:
                for line, idx in lines:
                    fout_kwd.write(f"L{idx}: {line}\n")
        return deduper.lines
