# test_logpipe.py
import gzip
import bz2
from pathlib import Path

import pytest

from datapipe import (
    find_logs,
    open_files,
    read_lines,
    filter_lines,
    count_bytes,
)


@pytest.fixture
def sample_tree(tmp_path: Path) -> Path:
    """
    Create a small log tree:

    logs/
        access-log
        access-log.gz
        access-log.bz2
        random.txt
    """
    root = tmp_path / "logs"
    root.mkdir()

    plain = root / "access-log"
    plain.write_text("GET /index.html 100\n" "GET /about.html 200\n" "BROKEN LINE\n")

    gz = root / "access-log.gz"
    with gzip.open(gz, "wt") as fd:
        fd.write("POST /api 300\n" "POST /upload 400\n")

    bz = root / "access-log.bz2"
    with bz2.open(bz, "wt") as fd:
        fd.write("DELETE /tmp 500\n")

    other = root / "random.txt"
    other.write_text("ignore me\n")

    return root


def test_find_logs_returns_all_files(sample_tree: Path):
    logs = sorted(find_logs(sample_tree))

    names = [p.name for p in logs]

    assert "access-log" in names
    assert "access-log.gz" in names
    assert "access-log.bz2" in names
    assert "random.txt" in names


def test_find_logs_pattern_filter(sample_tree: Path):
    logs = list(find_logs(sample_tree, r"access-log.*"))

    names = sorted(p.name for p in logs)

    assert names == [
        "access-log",
        "access-log.bz2",
        "access-log.gz",
    ]


def test_open_files_reads_plain_gz_and_bz2(sample_tree: Path):
    logfiles = find_logs(sample_tree, r"access-log.*")

    handles = open_files(logfiles)

    content = []

    for fd in handles:
        content.append(fd.read())

    joined = "".join(content)

    assert "GET /index.html 100" in joined
    assert "POST /api 300" in joined
    assert "DELETE /tmp 500" in joined


def test_read_lines_yields_all_lines(sample_tree: Path):
    logfiles = find_logs(sample_tree, r"access-log.*")
    handles = open_files(logfiles)

    lines = list(read_lines(handles))

    assert "GET /index.html 100\n" in lines
    assert "POST /upload 400\n" in lines
    assert "DELETE /tmp 500\n" in lines


def test_filter_lines_without_pattern_returns_all():
    lines = iter(
        [
            "alpha\n",
            "beta\n",
            "gamma\n",
        ]
    )

    result = list(filter_lines(lines))

    assert result == [
        "alpha\n",
        "beta\n",
        "gamma\n",
    ]


def test_filter_lines_with_pattern():
    lines = iter(
        [
            "GET /index\n",
            "POST /upload\n",
            "GET /about\n",
        ]
    )

    result = list(filter_lines(lines, r"GET"))

    assert result == [
        "GET /index\n",
        "GET /about\n",
    ]


@pytest.mark.parametrize(
    "lines, expected",
    [
        (
            [
                "GET / 100\n",
                "POST /upload 200\n",
            ],
            300,
        ),
        (
            [
                "BROKEN\n",
                "GET /x abc\n",
                "GET /y 50\n",
            ],
            50,
        ),
        (
            [],
            0,
        ),
    ],
)
def test_count_bytes(lines, expected):
    assert count_bytes(iter(lines)) == expected


def test_full_pipeline(sample_tree: Path):
    logfiles = find_logs(sample_tree, r"access-log.*")
    handles = open_files(logfiles)
    lines = read_lines(handles)

    filtered = filter_lines(lines, r"POST")

    result = list(filtered)

    assert result == [
        "POST /api 300\n",
        "POST /upload 400\n",
    ]


def test_full_pipeline_count_bytes(sample_tree: Path):
    logfiles = find_logs(sample_tree, r"access-log.*")
    handles = open_files(logfiles)
    lines = read_lines(handles)

    filtered = filter_lines(lines, r"(GET|POST|DELETE)")

    total = count_bytes(filtered)

    assert total == 100 + 200 + 300 + 400 + 500
