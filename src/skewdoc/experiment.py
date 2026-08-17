"""Run one workload under one strategy, then read the event log back and diagnose it.

The shape of every measurement in this repository:

1. start a session with the strategy's configuration,
2. run the query once as a **warmup and throw the number away**, then run it ``repeats`` times
   and keep the median,
3. **check the result against the control**, because a fix that changes the answer is not a fix,
4. run it once more in a fresh session whose event log therefore contains exactly one execution,
   and diagnose that log.

Step 2 is not a formality. The same query, unchanged, took 6,457 ms then 3,483 ms then 2,846 ms
on three consecutive runs in one process: the JIT compiles the generated code, the page cache
fills, and the third run is 2.3x faster than the first. Any matrix that measures each cell once
therefore ranks its cells partly by *when they ran*, and the first version of this repository's
join matrix did exactly that — it reported salting as 1.7x faster than the naive plan, which was
the warmup rather than the salt.

Step 3 is the one people skip. Salting adds a column, isolation unions two frames, a two-stage
aggregation changes the plan completely: each of those is an opportunity to return a subtly
different number faster, which is worse than being slow. Every run therefore reports a checksum
over the result and the comparison marks a strategy as **wrong** rather than as **fast** when it
disagrees with the control.

Step 4 exists because a session that ran the query four times writes four executions into one
event log, and a diagnosis over that log would describe an average of them. One extra session
per cell buys a log with one execution in it.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from statistics import median
from typing import Any

from . import fixes, workloads
from .diagnose import ApplicationDiagnosis, Thresholds, diagnose
from .errors import WorkloadError
from .eventlog import parse
from .logging_setup import get_logger
from .session import event_log_path, spark_session
from .workloads import AGG_SKEW, AGG_WIDE, JOIN_SKEW, WorkloadSpec

log = get_logger(__name__)


@dataclass
class ResultFingerprint:
    """Enough of the answer to notice that a strategy changed it."""

    rows: int
    checksum: float
    columns: list[str] = field(default_factory=list)

    def matches(self, other: ResultFingerprint) -> bool:
        # A relative tolerance, because a two-stage sum of doubles adds in a different order
        # and floating-point addition is not associative. 1e-9 is far below any real
        # difference and far above the reordering error on these magnitudes.
        if self.rows != other.rows:
            return False
        scale = max(abs(self.checksum), abs(other.checksum), 1.0)
        return abs(self.checksum - other.checksum) / scale < 1e-9

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RunResult:
    workload: str
    strategy: str
    strategy_summary: str
    spec: dict[str, Any]
    wall_ms: float
    rows: int
    checksum: float
    correct: bool | None
    event_log: str
    spark_version: str
    diagnosis: dict[str, Any]
    stage_count: int
    task_count: int
    worst_stage_verdict: str
    worst_stage_time_ratio: float
    worst_stage_bytes_ratio: float
    slowest_task_ms: float
    median_task_ms: float
    total_task_ms: float
    spilled_bytes: int
    wall_ms_samples: list[float] = field(default_factory=list)
    warmup_ms: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _fingerprint(frame, workload: str) -> ResultFingerprint:
    """Materialise the query and reduce the answer to a comparable number."""
    from pyspark.sql import functions as F

    if workload == AGG_WIDE:
        # A sum over the collected values, because two strategies produce the same multiset in
        # a different order and comparing the arrays directly would fail on ordering alone.
        row = frame.select(
            F.col("key"), F.aggregate("amounts", F.lit(0.0), lambda acc, x: acc + x).alias("s")
        ).agg(
            F.count(F.lit(1)).alias("rows"), F.sum("s").alias("checksum"),
        ).collect()[0]
        return ResultFingerprint(rows=int(row["rows"]), checksum=float(row["checksum"] or 0.0),
                                 columns=frame.columns)
    if workload == AGG_SKEW:
        row = frame.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum("total_amount").alias("checksum"),
        ).collect()[0]
        return ResultFingerprint(rows=int(row["rows"]), checksum=float(row["checksum"] or 0.0),
                                 columns=frame.columns)
    row = frame.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("amount").alias("checksum"),
    ).collect()[0]
    return ResultFingerprint(rows=int(row["rows"]), checksum=float(row["checksum"] or 0.0),
                             columns=frame.columns)


def _build_query(spark, spec: WorkloadSpec, strategy: fixes.StrategyConf):
    fact = workloads.build_fact(spark, spec)
    if spec.name == JOIN_SKEW:
        dim = workloads.build_dim(spark, spec)
        if strategy.name == fixes.SALTED:
            return fixes.salted_join(fact, dim, strategy.salt)
        if strategy.name == fixes.BROADCAST:
            return fixes.broadcast_join(fact, dim)
        if strategy.name == fixes.ISOLATED:
            return fixes.isolated_join(fact, dim, strategy.hot_keys)
        return fixes.naive_join(fact, dim)
    if spec.name == AGG_SKEW:
        if strategy.name == fixes.SALTED:
            return fixes.salted_aggregation(fact, strategy.salt)
        return fixes.naive_aggregation(fact)
    if spec.name == AGG_WIDE:
        if strategy.name == fixes.SALTED:
            return fixes.salted_wide_aggregation(fact, strategy.salt)
        return fixes.naive_wide_aggregation(fact)
    raise WorkloadError(f"unknown workload {spec.name!r}")


def run(
    spec: WorkloadSpec,
    strategy: fixes.StrategyConf,
    log_root: str | Path = "build/eventlogs",
    shuffle_partitions: int = 64,
    control: ResultFingerprint | None = None,
    thresholds: Thresholds | None = None,
    master: str = "local[4]",
    repeats: int = 2,
) -> tuple[RunResult, ResultFingerprint]:
    """Run one cell of the matrix: timed with a warmup, then diagnosed from a clean log."""
    spec.validate()
    if repeats < 1:
        raise WorkloadError("repeats must be at least 1")
    timing_dir = Path(log_root) / f"{spec.name}-{strategy.slug}-timing"
    diagnosis_dir = Path(log_root) / f"{spec.name}-{strategy.slug}"
    app_name = f"skewdoc-{spec.name}-{strategy.slug}"

    samples: list[float] = []
    with spark_session(
        app_name + "-timing", timing_dir, shuffle_partitions=shuffle_partitions,
        aqe=strategy.aqe, aqe_skew_join=strategy.aqe_skew_join, master=master,
    ) as spark:
        version = spark.version
        frame = _build_query(spark, spec, strategy)
        started = time.perf_counter()
        fingerprint = _fingerprint(frame, spec.name)
        warmup_ms = (time.perf_counter() - started) * 1000
        for _ in range(repeats):
            frame = _build_query(spark, spec, strategy)
            started = time.perf_counter()
            _fingerprint(frame, spec.name)
            samples.append((time.perf_counter() - started) * 1000)

    # A separate session, so the log it writes holds exactly one execution of the query.
    with spark_session(
        app_name, diagnosis_dir, shuffle_partitions=shuffle_partitions,
        aqe=strategy.aqe, aqe_skew_join=strategy.aqe_skew_join, master=master,
    ) as spark:
        frame = _build_query(spark, spec, strategy)
        _fingerprint(frame, spec.name)

    application = parse(event_log_path(diagnosis_dir))
    application_diagnosis: ApplicationDiagnosis = diagnose(application, thresholds)
    worst = application_diagnosis.worst or (
        max(application_diagnosis.stages, key=lambda s: s.time.total)
        if application_diagnosis.stages else None
    )
    if worst is None:
        raise WorkloadError("the run produced no stages with tasks, which cannot be diagnosed")

    result = RunResult(
        workload=spec.name,
        strategy=strategy.name,
        strategy_summary=strategy.summary,
        spec=spec.as_dict(),
        wall_ms=round(median(samples), 1),
        rows=fingerprint.rows,
        checksum=fingerprint.checksum,
        correct=None if control is None else fingerprint.matches(control),
        event_log=str(event_log_path(diagnosis_dir)),
        spark_version=version,
        diagnosis=application_diagnosis.as_dict(),
        stage_count=len(application_diagnosis.stages),
        task_count=application.task_count,
        worst_stage_verdict=worst.verdict,
        worst_stage_time_ratio=worst.time.ratio_max_median,
        worst_stage_bytes_ratio=worst.read_bytes.ratio_max_median,
        slowest_task_ms=worst.time.maximum,
        median_task_ms=worst.time.median,
        total_task_ms=round(sum(s.time.total for s in application_diagnosis.stages), 1),
        spilled_bytes=sum(s.spilled_bytes for s in application_diagnosis.stages),
        wall_ms_samples=[round(value, 1) for value in samples],
        warmup_ms=round(warmup_ms, 1),
    )
    log.info("run complete", extra={
        "workload": spec.name, "strategy": strategy.name, "wall_ms": result.wall_ms,
        "warmup_ms": result.warmup_ms, "samples": result.wall_ms_samples,
        "verdict": result.worst_stage_verdict, "rows": result.rows,
        "correct": result.correct,
    })
    return result, fingerprint


def run_matrix(
    spec: WorkloadSpec,
    strategies: list[fixes.StrategyConf],
    log_root: str | Path = "build/eventlogs",
    shuffle_partitions: int = 64,
    thresholds: Thresholds | None = None,
    master: str = "local[4]",
    repeats: int = 2,
) -> dict[str, Any]:
    """Run every strategy against one workload, with the first one as the control.

    The control runs first on purpose: every later run's correctness is judged against its
    answer, and a comparison whose baseline came later is a comparison against a moving
    target.
    """
    if not strategies:
        raise WorkloadError("no strategies to run")
    results: list[RunResult] = []
    control: ResultFingerprint | None = None
    for strategy in strategies:
        result, fingerprint = run(
            spec, strategy, log_root=log_root, shuffle_partitions=shuffle_partitions,
            control=control, thresholds=thresholds, master=master, repeats=repeats,
        )
        if control is None:
            control = fingerprint
            result.correct = True
        results.append(result)

    baseline = results[0]
    for result in results:
        result_dict = result.as_dict()
        result_dict["speedup_vs_control"] = (
            round(baseline.wall_ms / result.wall_ms, 2) if result.wall_ms else 0.0
        )
    return {
        "workload": spec.name,
        "spec": spec.as_dict(),
        "shuffle_partitions": shuffle_partitions,
        "master": master,
        "repeats": repeats,
        "timing_protocol": (
            "one discarded warmup run, then the median of the repeats, all inside one session; "
            "the diagnosis comes from a separate session whose event log holds one execution"
        ),
        "control": {"strategy": baseline.strategy, "rows": baseline.rows,
                    "checksum": baseline.checksum},
        "runs": [
            result.as_dict() | {
                "speedup_vs_control": (
                    round(baseline.wall_ms / result.wall_ms, 2) if result.wall_ms else 0.0
                )
            }
            for result in results
        ],
        "all_correct": all(r.correct for r in results),
    }
