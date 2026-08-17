"""Read Spark's own event log, which is where the truth about a stage's tasks lives.

Every claim this tool makes about a Spark job comes from the same source the History Server
uses: the JSON event log the driver writes as the job runs. Nothing is estimated, sampled, or
inferred from wall-clock timings around a `collect()`.

Why the event log rather than a `SparkListener`:

  * It is available after the fact. A job that ran last night can be diagnosed this morning,
    which is when someone actually asks.
  * It needs no JVM callback plumbing. Registering a Python listener means a py4j callback
    server and a Java shim; parsing a file needs neither, so this tool works against a cluster
    it has never connected to.
  * It is what an on-call engineer already has. `spark.eventLog.dir` on any managed platform
    (EMR, Dataproc, Databricks, a self-hosted History Server) is full of these files.

Three details that a first attempt gets wrong:

  * **Compression.** Spark 4 writes `.zstd` by default; older deployments use `.lz4`, `.snappy`
    or plain JSON. The reader dispatches on the extension and says clearly which codec is
    missing rather than failing with a Unicode error twelve frames deep.
  * **Rolling logs.** With `spark.eventLog.rolling.enabled` the log is a *directory*
    (`eventlog_v2_<app>/events_1_<app>`, `events_2_...`), and the parts must be read in
    numeric order. Lexicographic order puts `events_10` before `events_2`.
  * **Stage attempts.** A retried stage emits a second set of task events with the same stage
    id and a higher attempt number. Mixing the attempts together invents skew that did not
    happen, so `(stage_id, attempt)` is the unit of analysis throughout.
"""

from __future__ import annotations

import io
import json
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import EventLogError
from .logging_setup import get_logger

log = get_logger(__name__)

TASK_END = "SparkListenerTaskEnd"
STAGE_SUBMITTED = "SparkListenerStageSubmitted"
STAGE_COMPLETED = "SparkListenerStageCompleted"
APP_START = "SparkListenerApplicationStart"
ENV_UPDATE = "SparkListenerEnvironmentUpdate"
SQL_EXEC_START = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"

_PART_NUMBER = re.compile(r"events_(\d+)_")


@dataclass
class TaskRecord:
    """One finished task, reduced to the fields that decide whether a stage is skewed."""

    stage_id: int
    stage_attempt: int
    task_id: int
    index: int
    executor_id: str
    host: str
    launch_time_ms: int
    finish_time_ms: int
    run_ms: int
    cpu_ms: float
    gc_ms: int
    shuffle_read_bytes: int
    shuffle_read_records: int
    shuffle_write_bytes: int
    shuffle_write_records: int
    input_bytes: int
    input_records: int
    output_records: int
    memory_spilled: int
    disk_spilled: int
    peak_memory: int
    fetch_wait_ms: int
    failed: bool
    speculative: bool

    @property
    def wall_ms(self) -> int:
        """Launch to finish, which includes scheduler delay and result fetching.

        Kept alongside ``run_ms`` on purpose: a task whose wall time is far above its run time
        was waiting, not working, and that difference distinguishes a slow task from a slow
        cluster.
        """
        return max(0, self.finish_time_ms - self.launch_time_ms)


@dataclass
class StageRecord:
    stage_id: int
    attempt: int
    name: str
    num_tasks: int
    submission_time_ms: int | None = None
    completion_time_ms: int | None = None
    failure_reason: str | None = None
    parent_ids: list[int] = field(default_factory=list)
    tasks: list[TaskRecord] = field(default_factory=list)

    @property
    def key(self) -> tuple[int, int]:
        return (self.stage_id, self.attempt)

    @property
    def wall_ms(self) -> int | None:
        if self.submission_time_ms is None or self.completion_time_ms is None:
            return None
        return self.completion_time_ms - self.submission_time_ms

    @property
    def short_name(self) -> str:
        return self.name.split("at ")[0].strip() or self.name


@dataclass
class ApplicationLog:
    app_id: str
    app_name: str
    stages: dict[tuple[int, int], StageRecord]
    spark_conf: dict[str, str]
    sql_descriptions: list[str] = field(default_factory=list)
    source: str = ""

    @property
    def task_count(self) -> int:
        return sum(len(stage.tasks) for stage in self.stages.values())

    def stages_with_tasks(self) -> list[StageRecord]:
        """Stages that actually ran tasks, newest attempt first, in stage order."""
        return sorted(
            (s for s in self.stages.values() if s.tasks),
            key=lambda s: (s.stage_id, -s.attempt),
        )

    def conf(self, key: str, default: str = "") -> str:
        return self.spark_conf.get(key, default)


def _open_text(path: Path) -> io.TextIOBase:
    """Open one event-log part, decompressing by extension."""
    suffix = path.suffix.lower()
    if suffix in ("", ".json", ".inprogress", ".log"):
        return path.open("r", encoding="utf-8")
    if suffix == ".zstd":
        try:
            import zstandard
        except ImportError as exc:  # pragma: no cover - exercised by the error path test
            raise EventLogError(
                f"{path.name} is zstd-compressed, which needs the 'zstandard' package: "
                "pip install zstandard. Alternatively re-run the job with "
                "spark.eventLog.compression.codec=lz4 or spark.eventLog.compress=false."
            ) from exc
        reader = zstandard.ZstdDecompressor().stream_reader(path.open("rb"))
        return io.TextIOWrapper(reader, encoding="utf-8")
    if suffix == ".lz4":
        try:
            import lz4.frame
        except ImportError as exc:
            raise EventLogError(
                f"{path.name} is lz4-compressed, which needs the 'lz4' package: pip install lz4"
            ) from exc
        return io.TextIOWrapper(lz4.frame.open(path, "rb"), encoding="utf-8")
    if suffix == ".snappy":
        raise EventLogError(
            f"{path.name} uses Hadoop's snappy framing, which no pure-Python reader handles "
            "reliably. Re-run with spark.eventLog.compression.codec=zstd, or decompress it "
            "with the Spark distribution's own tooling first."
        )
    raise EventLogError(f"unrecognised event-log extension on {path.name}")


def _parts(path: Path) -> list[Path]:
    """Resolve a path to the ordered list of event-log parts it contains."""
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise EventLogError(f"no event log at {path}")
    parts = [p for p in path.iterdir() if p.name.startswith("events_")]
    if not parts:
        # A directory of application logs rather than one rolling log: point at the newest.
        candidates = sorted(
            (p for p in path.iterdir() if p.name.startswith("eventlog_v2_") or p.is_file()),
            key=lambda p: p.stat().st_mtime,
        )
        if not candidates:
            raise EventLogError(f"{path} contains no event-log files")
        return _parts(candidates[-1])
    # events_10 must sort after events_2, so order on the number rather than the string.
    def part_number(candidate: Path) -> int:
        match = _PART_NUMBER.search(candidate.name)
        return int(match.group(1)) if match else 0

    return sorted(parts, key=part_number)


def _events(path: Path) -> Iterator[dict[str, Any]]:
    for part in _parts(path):
        with _open_text(part) as handle:
            for number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    # A truncated final line is normal for a running application; anything
                    # else is a corrupt log and should not be silently skipped.
                    raise EventLogError(
                        f"{part.name}:{number}: not valid json ({exc}). If the application is "
                        "still running, wait for it to finish or copy the completed parts."
                    ) from exc


def _shuffle_read(metrics: dict[str, Any]) -> tuple[int, int, int]:
    read = metrics.get("Shuffle Read Metrics") or {}
    total = int(read.get("Local Bytes Read", 0)) + int(read.get("Remote Bytes Read", 0))
    return total, int(read.get("Total Records Read", 0)), int(read.get("Fetch Wait Time", 0))


def parse(path: str | Path) -> ApplicationLog:
    """Read an event log into stages and tasks."""
    target = Path(path)
    stages: dict[tuple[int, int], StageRecord] = {}
    app_id = ""
    app_name = ""
    spark_conf: dict[str, str] = {}
    sql_descriptions: list[str] = []

    for event in _events(target):
        kind = event.get("Event")
        if kind == APP_START:
            app_id = event.get("App ID", "") or ""
            app_name = event.get("App Name", "") or ""
        elif kind == ENV_UPDATE:
            spark_conf.update(event.get("Spark Properties") or {})
        elif kind == SQL_EXEC_START:
            description = event.get("description") or event.get("Description") or ""
            if description:
                sql_descriptions.append(description)
        elif kind in (STAGE_SUBMITTED, STAGE_COMPLETED):
            info = event.get("Stage Info") or {}
            key = (int(info["Stage ID"]), int(info.get("Stage Attempt ID", 0)))
            stage = stages.setdefault(
                key,
                StageRecord(
                    stage_id=key[0], attempt=key[1], name=info.get("Stage Name", ""),
                    num_tasks=int(info.get("Number of Tasks", 0)),
                    parent_ids=[int(p) for p in info.get("Parent IDs", [])],
                ),
            )
            stage.name = info.get("Stage Name", stage.name)
            stage.num_tasks = int(info.get("Number of Tasks", stage.num_tasks))
            if info.get("Submission Time"):
                stage.submission_time_ms = int(info["Submission Time"])
            if info.get("Completion Time"):
                stage.completion_time_ms = int(info["Completion Time"])
            if info.get("Failure Reason"):
                stage.failure_reason = str(info["Failure Reason"])
        elif kind == TASK_END:
            info = event.get("Task Info") or {}
            metrics = event.get("Task Metrics") or {}
            key = (int(event["Stage ID"]), int(event.get("Stage Attempt ID", 0)))
            stage = stages.setdefault(
                key, StageRecord(stage_id=key[0], attempt=key[1], name="", num_tasks=0)
            )
            read_bytes, read_records, fetch_wait = _shuffle_read(metrics)
            write = metrics.get("Shuffle Write Metrics") or {}
            input_metrics = metrics.get("Input Metrics") or {}
            output_metrics = metrics.get("Output Metrics") or {}
            stage.tasks.append(
                TaskRecord(
                    stage_id=key[0],
                    stage_attempt=key[1],
                    task_id=int(info.get("Task ID", -1)),
                    index=int(info.get("Index", -1)),
                    executor_id=str(info.get("Executor ID", "")),
                    host=str(info.get("Host", "")),
                    launch_time_ms=int(info.get("Launch Time", 0)),
                    finish_time_ms=int(info.get("Finish Time", 0)),
                    run_ms=int(metrics.get("Executor Run Time", 0)),
                    cpu_ms=int(metrics.get("Executor CPU Time", 0)) / 1e6,
                    gc_ms=int(metrics.get("JVM GC Time", 0)),
                    shuffle_read_bytes=read_bytes,
                    shuffle_read_records=read_records,
                    shuffle_write_bytes=int(write.get("Shuffle Bytes Written", 0)),
                    shuffle_write_records=int(write.get("Shuffle Records Written", 0)),
                    input_bytes=int(input_metrics.get("Bytes Read", 0)),
                    input_records=int(input_metrics.get("Records Read", 0)),
                    output_records=int(output_metrics.get("Records Written", 0)),
                    memory_spilled=int(metrics.get("Memory Bytes Spilled", 0)),
                    disk_spilled=int(metrics.get("Disk Bytes Spilled", 0)),
                    peak_memory=int(metrics.get("Peak Execution Memory", 0)),
                    fetch_wait_ms=fetch_wait,
                    failed=bool(info.get("Failed", False)),
                    speculative=bool(info.get("Speculative", False)),
                )
            )

    if not stages:
        raise EventLogError(
            f"{target} parsed cleanly but contains no stages. Was spark.eventLog.enabled set "
            "for the run that wrote it?"
        )

    application = ApplicationLog(
        app_id=app_id, app_name=app_name, stages=stages, spark_conf=spark_conf,
        sql_descriptions=sql_descriptions, source=str(target),
    )
    log.info(
        "parsed event log",
        extra={"app_id": app_id, "stages": len(stages), "tasks": application.task_count,
               "source": str(target)},
    )
    return application
