"""Decide whether a stage is skewed, and — the part that matters — whether it is *key* skew.

"The stage is slow because of skew" is the diagnosis everyone reaches for, and it is wrong
often enough to be expensive. A stage whose slowest task takes ten times the median can be
suffering from at least four different things, and they have different fixes:

  * **Key skew.** One partition holds far more data than the others, because the join or group
    key is unevenly distributed. Salting, an explicit broadcast, or AQE's skew-join splitting
    fix it. Repartitioning does not, because hashing the same key lands in the same place.
  * **A straggler.** The data is spread evenly and one *task* was slow anyway: a busy host, a
    long GC pause, a slow disk. Speculative execution helps; salting does nothing.
  * **Spill.** The partition fits in the shuffle but not in execution memory, so the task
    spills to disk and slows down out of proportion to its size. More memory, more partitions,
    or a different aggregation shape fix it; salting only helps if it also shrinks partitions.
  * **Partition starvation.** The stage has fewer tasks than the cluster has slots, so it is
    not skewed, it is *serial*. No amount of skew handling helps.

The comparison is always time against *the bytes that stage actually read*: shuffle bytes for a
reduce stage, input bytes for a map stage. Getting that wrong makes every map stage look like a
straggler, since its shuffle read is zero by definition.

The discriminator between the first two is the correlation between how much data a task read
and how long it took. Under key skew, the slowest task is also the biggest: skew in bytes and
skew in time move together. A straggler is slow while reading an ordinary amount, so the
ratios diverge. That is a measurement, not a judgement, and it is what this module computes.

Every threshold is named, defaulted from published guidance where there is any, and
overridable, because a threshold nobody can see is a magic number.
"""

from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass, field
from typing import Any

from .eventlog import ApplicationLog, StageRecord, TaskRecord
from .logging_setup import get_logger

log = get_logger(__name__)

# Verdicts, ordered by how much they cost to ignore.
KEY_SKEW = "key_skew"
STRAGGLER = "straggler"
SPILL = "spill"
STARVED = "partition_starvation"
HEALTHY = "healthy"

# Defaults. The 5x time ratio matches the factor Spark's own adaptive skew-join uses to decide
# a
# partition is skewed (spark.sql.adaptive.skewJoin.skewedPartitionFactor, default 5.0). The rest
# are set from what separates the classes cleanly on the measured workloads in this repository
# and are documented in ADR-002.
DEFAULT_TIME_RATIO = 5.0
DEFAULT_BYTES_RATIO = 2.0
DEFAULT_DOMINANCE = 0.30
# The dominance rule has to know how many tasks there are. "One task holds 30% of the stage's
# time" is damning in a 64-task stage and meaningless in a 4-task stage, where every task holds
# 25% by construction. The threshold is therefore the larger of the floor and this multiple of a
# balanced task's share, which stopped the rule reporting skew on every AQE-coalesced stage.
DEFAULT_DOMINANCE_MULTIPLE = 4.0
DEFAULT_MIN_TASKS = 4
DEFAULT_MIN_STAGE_MS = 250


@dataclass
class Distribution:
    """The shape of one per-task quantity across a stage."""

    count: int
    total: float
    minimum: float
    median: float
    p95: float
    maximum: float
    mean: float
    ratio_max_median: float
    gini: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile.

    Deliberately not interpolated: with eight tasks in a stage, an interpolated p95 is a
    weighted average of two tasks that both really happened, and reporting a duration no task
    took invites exactly the "which task was that?" question the report cannot answer.
    """
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))
    return float(ordered[rank])


def gini(values: list[float]) -> float:
    """Gini coefficient: 0 when every task is equal, approaching 1 as one task takes everything.

    A single ratio (max over median) says how bad the worst task is; the Gini says whether the
    imbalance is one outlier or a broad tilt, which changes the fix. Two hot keys among a
    thousand look nothing like a key space that is uniformly lumpy, and both can show a 6x
    max-to-median ratio.
    """
    if not values or all(v == 0 for v in values):
        return 0.0
    ordered = sorted(float(v) for v in values)
    total = sum(ordered)
    count = len(ordered)
    weighted = sum((index + 1) * value for index, value in enumerate(ordered))
    return float((2 * weighted) / (count * total) - (count + 1) / count)


def describe(values: list[float]) -> Distribution:
    numeric = [float(v) for v in values]
    if not numeric:
        return Distribution(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    median = statistics.median(numeric)
    maximum = max(numeric)
    return Distribution(
        count=len(numeric),
        total=sum(numeric),
        minimum=min(numeric),
        median=median,
        p95=_percentile(numeric, 0.95),
        maximum=maximum,
        mean=sum(numeric) / len(numeric),
        # A zero median means at least half the tasks did nothing measurable, so a ratio is
        # meaningless rather than infinite; the empty-partition share carries that signal.
        ratio_max_median=round(maximum / median, 2) if median > 0 else 0.0,
        gini=round(gini(numeric), 4),
    )


@dataclass
class Thresholds:
    time_ratio: float = DEFAULT_TIME_RATIO
    bytes_ratio: float = DEFAULT_BYTES_RATIO
    dominance: float = DEFAULT_DOMINANCE
    dominance_multiple: float = DEFAULT_DOMINANCE_MULTIPLE
    min_tasks: int = DEFAULT_MIN_TASKS
    min_stage_ms: int = DEFAULT_MIN_STAGE_MS

    def dominance_for(self, tasks: int) -> float:
        """The share of stage time one task must hold before that alone counts as skew."""
        if tasks <= 0:
            return 1.0
        return min(1.0, max(self.dominance, self.dominance_multiple / tasks))

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class StageDiagnosis:
    stage_id: int
    attempt: int
    name: str
    tasks: int
    read_source: str
    stage_wall_ms: int | None
    verdict: str
    confidence: str
    reason: str
    time: Distribution
    read_bytes: Distribution
    read_records: Distribution
    slowest_task_share: float
    empty_task_share: float
    spilling_tasks: int
    spilled_bytes: int
    distinct_executors: int
    slowest_executor: str
    recommendations: list[str] = field(default_factory=list)
    # The per-task samples, kept rather than only summarised. Five quantiles are enough to
    # classify a stage and not enough to draw it: a chart built from them is a reconstruction,
    # and a reconstruction of a distribution is exactly the thing this repository is arguing
    # against. Sixty-four floats per stage costs nothing.
    task_times_ms: list[float] = field(default_factory=list)
    task_read_bytes: list[float] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return self.verdict == HEALTHY

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for field_name in ("time", "read_bytes", "read_records"):
            payload[field_name] = getattr(self, field_name).as_dict()
        payload["healthy"] = self.healthy
        return payload

    @property
    def headline(self) -> str:
        return (
            f"stage {self.stage_id}.{self.attempt} ({self.tasks} tasks): {self.verdict} — "
            f"slowest task {self.time.maximum:.0f} ms against a median of "
            f"{self.time.median:.0f} ms"
        )


def _successful(stage: StageRecord) -> list[TaskRecord]:
    """Only successful, non-speculative attempts.

    A failed task's metrics describe the work it managed before dying, and a speculative copy
    duplicates a partition that is already counted. Including either makes the distribution
    describe the scheduler rather than the data.
    """
    return [t for t in stage.tasks if not t.failed and not t.speculative]


def diagnose_stage(
    stage: StageRecord, thresholds: Thresholds | None = None
) -> StageDiagnosis:
    thresholds = thresholds or Thresholds()
    tasks = _successful(stage)
    times = [float(t.run_ms) for t in tasks]
    # Which "bytes read" to compare against time depends on what kind of stage this is. A reduce
    # stage reads its data from the shuffle; a map stage reads it from storage and its shuffle
    # read is zero for every task. Using shuffle bytes for a map stage makes every uneven map
    # stage look like a straggler, because the ratio is 0 by construction — which is exactly the
    # mistake this repository's own agg workload caught.
    shuffle_bytes = [float(t.shuffle_read_bytes) for t in tasks]
    if sum(shuffle_bytes) > 0:
        read_bytes = shuffle_bytes
        read_records = [float(t.shuffle_read_records) for t in tasks]
        read_source = "shuffle_read"
    else:
        read_bytes = [float(t.input_bytes) for t in tasks]
        read_records = [float(t.input_records) for t in tasks]
        read_source = "input"

    time_dist = describe(times)
    bytes_dist = describe(read_bytes)
    records_dist = describe(read_records)

    total_time = time_dist.total or 1.0
    slowest_share = round(time_dist.maximum / total_time, 4)
    empty_share = (
        round(sum(1 for value in read_records if value == 0) / len(read_records), 4)
        if read_records else 0.0
    )
    spilling = [t for t in tasks if t.disk_spilled or t.memory_spilled]
    spilled_bytes = sum(t.disk_spilled for t in tasks)
    executors = {t.executor_id for t in tasks}
    slowest_executor = max(tasks, key=lambda t: t.run_ms).executor_id if tasks else ""

    verdict, confidence, reason, recommendations = _classify(
        tasks, time_dist, bytes_dist, slowest_share, empty_share, spilling, thresholds,
    )

    diagnosis = StageDiagnosis(
        stage_id=stage.stage_id,
        attempt=stage.attempt,
        name=stage.short_name,
        tasks=len(tasks),
        read_source=read_source,
        stage_wall_ms=stage.wall_ms,
        verdict=verdict,
        confidence=confidence,
        reason=reason,
        time=time_dist,
        read_bytes=bytes_dist,
        read_records=records_dist,
        slowest_task_share=slowest_share,
        empty_task_share=empty_share,
        spilling_tasks=len(spilling),
        spilled_bytes=spilled_bytes,
        distinct_executors=len(executors),
        slowest_executor=slowest_executor,
        recommendations=recommendations,
        task_times_ms=[round(value, 1) for value in times],
        task_read_bytes=[float(value) for value in read_bytes],
    )
    log.info("diagnosed stage", extra={
        "stage": stage.stage_id, "attempt": stage.attempt, "verdict": verdict,
        "tasks": len(tasks), "time_ratio": time_dist.ratio_max_median,
        "bytes_ratio": bytes_dist.ratio_max_median,
    })
    return diagnosis


def _classify(
    tasks: list[TaskRecord],
    time_dist: Distribution,
    bytes_dist: Distribution,
    slowest_share: float,
    empty_share: float,
    spilling: list[TaskRecord],
    thresholds: Thresholds,
) -> tuple[str, str, str, list[str]]:
    if not tasks:
        return HEALTHY, "low", "the stage ran no successful tasks", []

    if time_dist.total < thresholds.min_stage_ms:
        return (
            HEALTHY, "high",
            f"the stage's tasks used {time_dist.total:.0f} ms in total, below the "
            f"{thresholds.min_stage_ms} ms floor where a ratio is worth reporting",
            [],
        )

    if len(tasks) < thresholds.min_tasks:
        # Not skew: a stage with three tasks cannot be balanced across a cluster no matter how
        # the data is distributed.
        return (
            STARVED, "high",
            f"the stage ran {len(tasks)} task(s), fewer than the {thresholds.min_tasks} needed "
            "for a distribution to mean anything; the stage is serial rather than skewed",
            [
                "raise spark.sql.shuffle.partitions, or repartition before this stage, so "
                "there is enough parallelism to spread",
                "check whether an upstream coalesce or a single input file is deciding the "
                "task count",
            ],
        )

    time_skewed = (
        time_dist.ratio_max_median >= thresholds.time_ratio
        or slowest_share >= thresholds.dominance_for(len(tasks))
    )
    bytes_skewed = bytes_dist.ratio_max_median >= thresholds.bytes_ratio

    if time_skewed and bytes_skewed:
        confidence = (
            "high" if bytes_dist.ratio_max_median >= thresholds.time_ratio else "medium"
        )
        reason = (
            f"the slowest task ran {time_dist.ratio_max_median}x the median and read "
            f"{bytes_dist.ratio_max_median}x the median bytes, so the time follows the data: "
            "one partition holds much more than the others"
        )
        recommendations = [
            "if one side of the join fits in memory, broadcast it: the shuffle disappears and "
            "so does the skew",
            "otherwise salt the hot keys and replicate the small side across the salt values",
            "confirm spark.sql.adaptive.skewJoin.enabled is on, and remember it splits skewed "
            "*join* partitions only, not aggregations",
        ]
        if spilling:
            recommendations.append(
                f"{len(spilling)} task(s) also spilled, so the hot partition does not fit in "
                "execution memory; splitting it fixes both problems"
            )
        return KEY_SKEW, confidence, reason, recommendations

    if time_skewed and not bytes_skewed:
        reason = (
            f"the slowest task ran {time_dist.ratio_max_median}x the median while reading "
            f"only {bytes_dist.ratio_max_median}x the median bytes, so the data is spread "
            "evenly and the task was slow for another reason"
        )
        recommendations = [
            "look at the executor that ran the slowest task before reshaping any data: this "
            "pattern is a host, a GC pause, or a disk, not a key",
            "speculative execution (spark.speculation) targets exactly this case; salting does "
            "nothing for it",
        ]
        worst = max(tasks, key=lambda t: t.run_ms)
        if worst.gc_ms > 0.2 * max(1, worst.run_ms):
            recommendations.insert(
                0,
                f"the slowest task spent {worst.gc_ms} ms of {worst.run_ms} ms in GC, which is "
                "where to look first",
            )
        if worst.fetch_wait_ms > 0.2 * max(1, worst.run_ms):
            recommendations.insert(
                0,
                f"the slowest task spent {worst.fetch_wait_ms} ms of {worst.run_ms} ms waiting "
                "on shuffle fetches, so the problem is upstream or on the network",
            )
        return STRAGGLER, "medium", reason, recommendations

    if spilling:
        return (
            SPILL, "high",
            f"{len(spilling)} of {len(tasks)} tasks spilled "
            f"{sum(t.disk_spilled for t in tasks) / 1e6:.1f} MB to disk without a large "
            "imbalance, so partitions are too big for execution memory rather than uneven",
            [
                "raise the partition count so each task's working set fits, or give executors "
                "more memory",
                "check for a wide aggregation buffer: a high-cardinality group-by or a "
                "collect_list is the usual cause",
            ],
        )

    if empty_share >= 0.5 and len(tasks) >= thresholds.min_tasks:
        return (
            HEALTHY, "medium",
            f"{empty_share:.0%} of tasks read no records, so the stage is over-partitioned "
            "rather than skewed: parallelism is being spent on scheduling",
            ["lower spark.sql.shuffle.partitions for this stage, or let AQE coalesce it"],
        )

    return (
        HEALTHY, "high",
        f"the slowest task ran {time_dist.ratio_max_median}x the median and read "
        f"{bytes_dist.ratio_max_median}x the median bytes, both inside the thresholds",
        [],
    )


@dataclass
class ApplicationDiagnosis:
    app_id: str
    app_name: str
    source: str
    stages: list[StageDiagnosis]
    thresholds: Thresholds
    aqe_enabled: bool
    aqe_skew_join_enabled: bool
    shuffle_partitions: str

    @property
    def worst(self) -> StageDiagnosis | None:
        unhealthy = [s for s in self.stages if not s.healthy]
        if not unhealthy:
            return None
        return max(unhealthy, key=lambda s: s.time.total)

    @property
    def verdict(self) -> str:
        for candidate in (KEY_SKEW, SPILL, STRAGGLER, STARVED):
            if any(s.verdict == candidate for s in self.stages):
                return candidate
        return HEALTHY

    def as_dict(self) -> dict[str, Any]:
        return {
            "app_id": self.app_id,
            "app_name": self.app_name,
            "source": self.source,
            "verdict": self.verdict,
            "aqe_enabled": self.aqe_enabled,
            "aqe_skew_join_enabled": self.aqe_skew_join_enabled,
            "shuffle_partitions": self.shuffle_partitions,
            "thresholds": self.thresholds.as_dict(),
            "stages": [s.as_dict() for s in self.stages],
        }


def diagnose(application: ApplicationLog, thresholds: Thresholds | None = None
             ) -> ApplicationDiagnosis:
    thresholds = thresholds or Thresholds()
    stages = [diagnose_stage(stage, thresholds) for stage in application.stages_with_tasks()]
    return ApplicationDiagnosis(
        app_id=application.app_id,
        app_name=application.app_name,
        source=application.source,
        stages=stages,
        thresholds=thresholds,
        aqe_enabled=application.conf("spark.sql.adaptive.enabled", "true").lower() == "true",
        aqe_skew_join_enabled=application.conf(
            "spark.sql.adaptive.skewJoin.enabled", "true").lower() == "true",
        shuffle_partitions=application.conf("spark.sql.shuffle.partitions", "unset"),
    )
