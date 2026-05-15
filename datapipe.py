#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK
from typing import Iterator, FileIO
import os
import re
import gzip
import bz2


def iter_logfile(top) -> Iterator[str]:
    """Iterate logfiles
    www/bar/access-log-0208.bz2
    www/bar/access-log
    www/foo/access-log-0208.gz
    """
    for dir, _, names in os.walk(top):
        for name in names:
            yield os.path.join(dir, name)


def iter_handle(logfiles: Iterator[str]) -> Iterator[FileIO]:
    for log in logfiles:
        fd: FileIO
        if log.endswith(".gz"):
            fd = gzip.open(log)
        elif log.endswith(".bz2"):
            fd = bz2.open(log)
        else:
            fd = open(log)
        yield fd


def iter_line(log_hadle: Iterator[FileIO]) -> Iterator[str]:
    for h in log_hadle:
        yield from h


def iter_select(line: Iterator[str], pat: str) -> Iterator[str]:
    if re.search(pat, line):
        yield line


if __name__ == "__main__":
    logfiles = iter_logfile("www")
    handles = iter_handle(logfiles)
    lines = iter_line(handles)
    selected_lines = iter_select(lines, "Python")
    for line in selected_lines:
        print(line)
