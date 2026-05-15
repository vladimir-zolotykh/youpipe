#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK
from typing import Iterator
import os


def iter_logfile(top) -> Iterator[str]:
    """Iterate logfiles
    www/bar/access-log-0208.bz2
    www/bar/access-log
    """
    for dir, _, names in os.walk(top):
        for name in names:
            yield os.path.join(dir, name)


if __name__ == "__main__":
    for path in iter_logfile("www"):
        print(path)
