"""Build a Spark session whose behaviour is pinned, and whose event log is where we can read it.

Two things make a skew comparison honest, and both are session-level:

**The configuration is explicit.** Adaptive query execution changes the answer to every
question this tool asks, and it is on by default in Spark 3.2+. A benchmark that does not
state whether AQE was on is unreadable, so every knob that affects the result is set here and
recorded in the event log's environment update, where the report reads it back.

**The event log is per-run.** Each run writes to its own directory, so a comparison is between
two logs rather than between two guesses about which tasks belonged to which run.

Local mode is used deliberately. It is not a cluster, and the numbers here are not cluster
numbers; what local mode reproduces faithfully is the *shape* of the problem — one partition
holding most of the rows, one task doing most of the work — because that shape is decided by
the hash partitioner and the data, not by how many machines are involved. ADR-001 says what
that does and does not license anyone to conclude.
"""

from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from pathlib import Path

from .errors import SparkMissingError
from .logging_setup import get_logger

log = get_logger(__name__)

# Configuration that is the same for every run in a comparison. Anything that varies between
# strategies is passed in explicitly, so a diff of two runs' configs is exactly the change.
BASE_CONF = {
    "spark.ui.enabled": "false",
    "spark.eventLog.enabled": "true",
    # Uncompressed logs so a reader with no zstd package can still open the committed
    # fixtures. Production should leave compression on; the parser handles both.
    "spark.eventLog.compress": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.driver.host": "127.0.0.1",
    "spark.driver.bindAddress": "127.0.0.1",
    "spark.sql.session.timeZone": "UTC",
    "spark.rdd.compress": "true",
}


def require_pyspark() -> None:
    try:
        import pyspark  # noqa: F401
    except ImportError as exc:
        raise SparkMissingError(
            "PySpark is not installed. 'pip install -e \".[spark]\"' installs it, and a JVM "
            "(Java 17 or 21) must be on PATH. Diagnosis of an existing event log needs neither."
        ) from exc
    if not (os.environ.get("JAVA_HOME") or shutil.which("java")):
        raise SparkMissingError(
            "no JVM found: install Java 17 or 21 and set JAVA_HOME, or point it at an "
            "existing installation. 'skewdoc diagnose' works on a recorded event log "
            "without one."
        )


@contextmanager
def spark_session(
    app_name: str,
    log_dir: str | Path,
    shuffle_partitions: int = 64,
    aqe: bool = True,
    aqe_skew_join: bool = True,
    master: str = "local[4]",
    driver_memory: str = "2g",
    extra: dict[str, str] | None = None,
):
    """A session writing its event log into ``log_dir``, cleared first so it holds one run."""
    require_pyspark()
    from pyspark.sql import SparkSession

    target = Path(log_dir)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.eventLog.dir", target.resolve().as_uri())
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true" if aqe else "false")
        .config("spark.sql.adaptive.skewJoin.enabled", "true" if aqe_skew_join else "false")
        .config("spark.driver.memory", driver_memory)
    )
    for key, value in {**BASE_CONF, **(extra or {})}.items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    log.info("spark session started", extra={
        "app_name": app_name, "master": master, "aqe": aqe, "aqe_skew_join": aqe_skew_join,
        "shuffle_partitions": shuffle_partitions, "version": session.version,
        "log_dir": str(target),
    })
    try:
        yield session
    finally:
        session.stop()
        log.info("spark session stopped", extra={"app_name": app_name})


def event_log_path(log_dir: str | Path) -> Path:
    """The log this run wrote, whether Spark chose a rolling directory or a single file."""
    target = Path(log_dir)
    rolling = sorted(target.glob("eventlog_v2_*"))
    if rolling:
        return rolling[-1]
    files = sorted(
        (p for p in target.iterdir() if p.is_file() and not p.name.startswith(".")),
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        raise SparkMissingError(
            f"{target} holds no event log. Spark writes one only when "
            "spark.eventLog.enabled is true and the directory exists before the session "
            "starts."
        )
    return files[-1]
