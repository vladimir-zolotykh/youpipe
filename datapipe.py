#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK
from typing import Iterator, TextIO
import os
import re
import gzip
import bz2
import argparse
import argcomplete


def iter_logfile(top, pat: str = "") -> Iterator[str]:
    """Iterate logfiles
    www/bar/access-log-0208.bz2
    www/bar/access-log
    www/foo/access-log-0208.gz
    """
    for dir, _, names in os.walk(top):
        for name in names:
            if not pat or re.match(pat, name):
                yield os.path.join(dir, name)


def iter_handle(logfiles: Iterator[str]) -> Iterator[TextIO]:
    for log in logfiles:
        fd: TextIO
        if log.endswith(".gz"):
            fd = gzip.open(log, "rt")
        elif log.endswith(".bz2"):
            fd = bz2.open(log, "rt")
        else:
            fd = open(log, "rt")
        yield fd


def iter_line(log_hadle: Iterator[TextIO]) -> Iterator[str]:
    for h in log_hadle:
        yield from h


def iter_select(lines: Iterator[str], pat: str = "") -> Iterator[str]:
    for line in lines:
        if not pat or re.search(pat, line):
            yield line


def count_bytes(lines: Iterator[str]) -> int:
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
    nargs="+",
    choices=["print", "count-bytes"],
    default="print",
    help="select action",
)
parser.add_argument("top_dir", default="www", help="root dir of log files tree")

if __name__ == "__main__":
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    # logfiles = iter_logfile("www", "access-log")
    logfiles = iter_logfile(args.top_dir, args.include_files)
    handles = iter_handle(logfiles)
    lines = iter_line(handles)
    selected_lines = iter_select(lines, args.include_lines)  # , "156.63.68.202")
    if "print" in args.action:
        for line in selected_lines:
            print(line, end="")
    else:
        print(count_bytes(selected_lines))
