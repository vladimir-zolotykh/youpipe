#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK
from typing import Iterator, TextIO
import os
import re
import itertools
import gzip
import bz2
import argparse
from pathlib import Path
import argcomplete

IterPath = Iterator[Path]
IterLine = Iterator[str]
IterHandle = Iterator[TextIO]


def find_logs(top, pat: str = "") -> IterPath:
    """Iterate logfiles
    www/bar/access-log-0208.bz2
    www/bar/access-log
    www/foo/access-log-0208.gz
    """
    for dir, _, names in os.walk(top):
        for name in names:
            if not pat or re.match(pat, name):
                yield Path(dir) / name


def open_files(logfiles: IterPath) -> IterHandle:
    for log in logfiles:
        fd: TextIO
        if log.suffix == ".gz":
            fd = gzip.open(log, "rt")
        elif log.suffix == ".bz2":
            fd = bz2.open(log, "rt")
        else:
            fd = open(log, "rt")
        yield fd


def read_lines(log_handle: IterHandle) -> IterLine:
    for h in log_handle:
        yield from h


def filter_lines(lines: IterLine, pat: str = "") -> IterLine:
    for line in lines:
        if not pat or re.search(pat, line):
            yield line


def count_bytes(lines: IterLine) -> int:
    total: int = 0
    for line in lines:
        try:
            cnt = int(line.rsplit(None, 1)[1])
        except (ValueError, IndexError):
            cnt = 0
        total += cnt
    return total


parser = argparse.ArgumentParser(
    description="Configure datapipe",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("--include-files", default="", help="include file")
parser.add_argument("--include-lines", default="", help="include line")
parser.add_argument(
    "--action",
    action="append",
    choices=["print", "count-bytes"],
    default=[],
    help="select action",
)
parser.add_argument("top_dir", default="www", help="root dir of log files tree")

if __name__ == "__main__":
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    logfiles: IterPath = find_logs(args.top_dir, args.include_files)
    handles: IterHandle = open_files(logfiles)
    lines: IterLine = read_lines(handles)
    filtered_lines: IterLine = filter_lines(lines, args.include_lines)
    lines1, lines2 = itertools.tee(filtered_lines, 2)
    if "print" in args.action:
        for line in lines1:
            print(line, end="")
    if "count-bytes" in args.action:
        print(count_bytes(lines2))
