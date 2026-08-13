"""replay.py's publish path is covered by test_poller.py's publish_rows
tests since it's the same function -- this only covers file handling."""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "poller"))

import replay


def test_file_timestamp_strips_full_pb_gz_suffix():
    # regression: Path.stem only strips one suffix, leaving ".pb" behind
    path = Path("/data/raw/date=2026-08-13/hour=01/20260813T010000.pb.gz")
    assert replay.file_timestamp(path) == datetime(2026, 8, 13, 1, 0, 0)


def test_find_raw_files_sorts_chronologically(tmp_path):
    partition = tmp_path / "date=2026-08-13" / "hour=01"
    partition.mkdir(parents=True)
    # write out of order on purpose
    for name in ["20260813T010040.pb.gz", "20260813T010000.pb.gz", "20260813T010020.pb.gz"]:
        (partition / name).write_bytes(b"")

    files = replay.find_raw_files(tmp_path)
    assert [f.name for f in files] == [
        "20260813T010000.pb.gz",
        "20260813T010020.pb.gz",
        "20260813T010040.pb.gz",
    ]
