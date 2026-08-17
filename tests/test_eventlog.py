"""Parser tests, mostly against real event logs committed to the repository.

The fixtures under `data/fixtures/` are Spark's own output from the runs in the README, not
hand-written JSON. That matters for a parser: a synthetic fixture encodes what I *believe* the
format is, and the interesting bugs live in the parts I believed wrongly — rolling directories,
compression, stage attempts, the difference between `Executor Run Time` and wall time.

A few small hand-built logs exist as well, for the failure paths a real log will not produce on
demand: a truncated line, an unknown codec, an empty log.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from skewdoc.errors import EventLogError
from skewdoc.eventlog import parse

FIXTURES = Path(__file__).resolve().parents[1] / "data" / "fixtures"
REAL_LOGS = ("join-naive-noaqe", "join-broadcast-noaqe", "agg-naive-noaqe")


def _task_end(stage: int, attempt: int, task_id: int, run_ms: int, read_bytes: int = 0,
              failed: bool = False, speculative: bool = False) -> str:
    return json.dumps({
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage,
        "Stage Attempt ID": attempt,
        "Task Info": {
            "Task ID": task_id, "Index": task_id, "Attempt": 0, "Launch Time": 1000,
            "Finish Time": 1000 + run_ms + 5, "Executor ID": "driver", "Host": "localhost",
            "Failed": failed, "Killed": False, "Speculative": speculative,
        },
        "Task Metrics": {
            "Executor Run Time": run_ms, "Executor CPU Time": run_ms * 1_000_000,
            "JVM GC Time": 0, "Memory Bytes Spilled": 0, "Disk Bytes Spilled": 0,
            "Peak Execution Memory": 0,
            "Shuffle Read Metrics": {
                "Local Bytes Read": read_bytes, "Remote Bytes Read": 0,
                "Total Records Read": read_bytes // 10, "Fetch Wait Time": 0,
            },
            "Shuffle Write Metrics": {"Shuffle Bytes Written": 0, "Shuffle Records Written": 0},
        },
    })


def _stage(stage: int, attempt: int, tasks: int, submitted: int = 900,
           completed: int = 3000) -> str:
    return json.dumps({
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {
            "Stage ID": stage, "Stage Attempt ID": attempt,
            "Stage Name": "collect at demo.py:1",
            "Number of Tasks": tasks, "Parent IDs": [], "Submission Time": submitted,
            "Completion Time": completed,
        },
    })


def write_log(path: Path, lines: list[str]) -> Path:
    path.write_text("\n".join(lines) + "\n")
    return path


@pytest.mark.parametrize("name", REAL_LOGS)
def test_every_committed_real_log_parses(name):
    application = parse(FIXTURES / name)
    assert application.app_id.startswith("local-")
    assert application.task_count > 0
    assert application.stages_with_tasks(), "a real run must have stages with tasks"
    assert application.conf("spark.sql.shuffle.partitions") == "64"


def test_a_real_log_carries_the_metrics_the_diagnosis_needs():
    application = parse(FIXTURES / "join-naive-noaqe")
    stage = max(application.stages_with_tasks(), key=lambda s: len(s.tasks))
    assert len(stage.tasks) == 64, "the shuffle stage should have one task per partition"
    assert sum(t.shuffle_read_bytes for t in stage.tasks) > 0
    assert sum(t.run_ms for t in stage.tasks) > 0
    assert all(t.wall_ms >= 0 for t in stage.tasks)
    assert stage.wall_ms is not None and stage.wall_ms > 0


def test_the_configuration_is_read_back_from_the_log():
    """Whether AQE was on is part of the result, and it comes from the log, not from a flag."""
    naive = parse(FIXTURES / "join-naive-noaqe")
    assert naive.conf("spark.sql.adaptive.enabled") == "false"
    assert naive.conf("spark.sql.autoBroadcastJoinThreshold") == "-1"


def test_rolling_parts_are_read_in_numeric_order(tmp_path):
    """events_10 must sort after events_2, which string ordering gets wrong."""
    directory = tmp_path / "eventlog_v2_local-1"
    directory.mkdir()
    write_log(directory / "events_2_local-1", [_task_end(0, 0, 1, 100)])
    write_log(directory / "events_10_local-1", [_task_end(0, 0, 2, 200), _stage(0, 0, 2)])
    application = parse(directory)
    stage = application.stages[(0, 0)]
    assert [t.task_id for t in stage.tasks] == [1, 2]


def test_a_stage_retry_is_kept_separate_from_its_first_attempt(tmp_path):
    """Mixing attempts invents skew: the first attempt's partial tasks join the second's."""
    path = write_log(tmp_path / "events", [
        _task_end(3, 0, 1, 100), _task_end(3, 0, 2, 100), _stage(3, 0, 2),
        _task_end(3, 1, 3, 4000), _task_end(3, 1, 4, 100), _stage(3, 1, 2),
    ])
    application = parse(path)
    assert set(application.stages) == {(3, 0), (3, 1)}
    assert [t.run_ms for t in application.stages[(3, 1)].tasks] == [4000, 100]
    # The newest attempt comes first, because that is the one that decided the outcome.
    assert [s.attempt for s in application.stages_with_tasks()] == [1, 0]


def test_a_truncated_line_names_the_file_and_line(tmp_path):
    path = tmp_path / "events"
    path.write_text(_task_end(0, 0, 1, 10) + "\n" + '{"Event": "SparkListenerTaskEnd"')
    with pytest.raises(EventLogError) as excinfo:
        parse(path)
    assert "events:2" in str(excinfo.value)
    assert "still running" in str(excinfo.value), "the likely cause belongs in the message"


def test_an_unknown_codec_says_which_package_is_missing(tmp_path):
    path = tmp_path / "events.snappy"
    path.write_bytes(b"\x00\x01")
    with pytest.raises(EventLogError, match="snappy"):
        parse(path)


def test_an_unrecognised_extension_is_refused(tmp_path):
    path = tmp_path / "events.gz"
    path.write_bytes(b"\x1f\x8b")
    with pytest.raises(EventLogError, match="unrecognised event-log extension"):
        parse(path)


def test_a_log_with_no_stages_says_so_rather_than_returning_nothing(tmp_path):
    path = write_log(tmp_path / "events", [json.dumps({"Event": "SparkListenerLogStart"})])
    with pytest.raises(EventLogError, match="spark.eventLog.enabled"):
        parse(path)


def test_a_missing_path_is_reported(tmp_path):
    with pytest.raises(EventLogError, match="no event log"):
        parse(tmp_path / "absent")


def test_blank_lines_are_tolerated(tmp_path):
    path = write_log(tmp_path / "events", [_task_end(0, 0, 1, 10), "", _stage(0, 0, 1), ""])
    assert parse(path).task_count == 1


def test_a_zstd_log_round_trips(tmp_path):
    """Spark 4 compresses event logs by default, so the reader must handle it."""
    zstandard = pytest.importorskip("zstandard")
    path = tmp_path / "events.zstd"
    payload = ("\n".join([_task_end(0, 0, 1, 42), _stage(0, 0, 1)]) + "\n").encode()
    path.write_bytes(zstandard.ZstdCompressor().compress(payload))
    application = parse(path)
    assert application.task_count == 1
    assert application.stages[(0, 0)].tasks[0].run_ms == 42
