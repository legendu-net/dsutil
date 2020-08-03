import sys
import os
import re
from collections import deque
from difflib import SequenceMatcher
import time
DASH_50 = "-" * 50


class LogCluster:
    """
    clustering similar lines
    """
    def __init__(self):
        self.cluster = []

    def similarity(self, line1, line2):
        return SequenceMatcher(None, line1, line2).ratio()

    def add(self, line, line_num):
        LINE = "L{line_num}: {line}\n"
        similarity = max(
            (self.similarity(line, target) for target in self.cluster), default=0
        )
        if similarity < 0.5:
            self.cluster.append(LINE.format(line=line, line_num=line_num))

    def write(self, fout):
        fout.write(DASH_50 + "SUMMARY" + DASH_50 + "\n")
        for line in self.cluster:
            fout.write(line)


class LogFilter:
    KEYWORDS = (
        "Exception",
        "Error",
        "User class threw exception",
        "OOM",
        "spark.yarn.executor.memoryOverhead",
        "FileAlreadyExists",
        "InvalidResourceRequestException",
        "exec /bin/bash ",
        "has no attribute",
        "not found",
    )
    PATTERNS = (
        r"\d\d[\/](0?[1-9]|1[0-2])[\/](0?[1-9]|[12][0-9]|3[01])\s\d+[:]\d+:\d+",
        r"((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.|$)){4}",
    )
    MSG = "\rProcessing line {line_num} ({progress}%); Time Used: {time_used}s; Time Left: {time_left}s"

    def __init__(
        self,
        log_file,
        context_size=5,
        keywords=KEYWORDS,
        patterns=PATTERNS,
        case_sensitive: bool = True,
        output_file: str = "",
    ):
        self._log_file = log_file
        self.context_size = context_size
        self.keywords = keywords if keywords else LogFilter.KEYWORDS
        self.keywords = keywords
        self.patterns = patterns if patterns else LogFilter.PATTERNS
        self.patterns = [re.compile(p) for p in self.patterns]
        self.case_sensitive = case_sensitive
        if not self.case_sensitive:
            self.keywords = [kw.lower() for kw in self.keywords]
        self.num_rows = None
        self.step = None
        self.unique = set()
        self.lookup = {}
        self.queue = deque()
        self.output_file = self._output_file(output_file)

    def _output_file(self, output_file):
        if output_file:
            return output_file
        title, ext = os.path.splitext(self._log_file)
        return title + "_s" + ext

    def regularize(self, line):
        """Get rid of substrings with patterns specified by the regular expressions.
        """
        for pattern in self.patterns:
            line = pattern.sub("", line)
        return line

    def dump_queue(self, lines):
        lines.append("-" * 100 + "\n")
        lines.extend(self.queue)
        self.queue.clear()

    def keep(self, idx, line):
        """Check whether the line should be kept.
        """
        if not self.case_sensitive:
            line = line.lower()
        if any(kw in line for kw in self.keywords):
            line = self.regularize(line)
            if line not in self.unique:
                self.unique.add(line)
                self.lookup[line] = idx
                return True
        return False

    def calc_rows(self):
        if self.num_rows is not None:
            return
        print('Calculating total number of rows ...')
        self.num_rows = sum(1 for line in open(self._log_file, 'r'))
        print('Total number of rows: ', '{:,}'.format(self.num_rows))
        self.step = max(self.num_rows // 1000, 1000)

    def filter(self):
        self.calc_rows()
        lines = [DASH_50 + 'START' + DASH_50 + '\n']
        with open(self._log_file, 'r') as fin:
            dump_flag = -1
            time_begin = time.time()
            for idx, line in enumerate(fin):
                line_with_num = f'L{idx}: {line}'
                self.queue.append(line_with_num)
                keep = self.keep(idx, line)
                # fill up context_head with anything only if found
                if len(self.queue) < self.context_size and not keep:
                    self.queue.append(line_with_num)
                    continue
                if not keep and dump_flag == -1:
                    self.queue.popleft()
                elif not keep and dump_flag >= 0:
                    dump_flag += 1
                    if dump_flag == self.context_size:
                        self.dump_queue(lines)
                        dump_flag = -1
                else:
                    dump_flag = 0
                # print progress
                if idx % self.step == 0:
                    time_end = time.time()
                    time_used = time_end - time_begin
                    time_left = time_used / idx * (self.num_rows - idx)
                    msg = LogFilter.MSG.format(
                        line_num='{:,}'.format(idx),
                        progress=round(idx / self.num_rows * 100, 1),
                        time_used=round(time_used, 1),
                        time_left=round(time_left, 1)
                    )
                    sys.stdout.write(msg)
            print('\n')
            lines.append(DASH_50 + 'EOF' + DASH_50 + '\n')
            self.dump_queue(lines)
        # dedup to get a summary
        cluster = LogCluster()
        for line in self.unique:
            cluster.add(line, self.lookup[line])
        cluster.write(sys.stdout)
        with open(self.output_file, 'w') as fout:
            cluster.write(fout)
            fout.writelines(lines)
        sys.stdout.write(f'\nFile saved in {self.output_file}\n')
