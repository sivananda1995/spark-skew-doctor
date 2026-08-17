"""Reproducible skewed datasets, and the two jobs that suffer from them.

A skew demo that generates random data is not a demo, because the reader cannot tell whether
the fix worked or the dice changed. Everything here is a pure function of a seed and a size,
written with Spark's own deterministic primitives, so two runs produce byte-identical
partitions and the before/after comparison is a comparison.

The key distribution is the whole exercise. Real skew looks like a power law: a handful of
keys
carry most of the rows (the tenant that is ten times everyone else, the null-ish sentinel, the
`unknown` bucket, the one product everybody bought), with a long tail of ordinary keys behind
them. So the generator takes an explicit **hot share** and spreads it across a small number of
hot keys, then distributes the rest across the tail. That makes the severity a parameter
rather than an accident, and the sweep in `benchmark/bench_severity.py` uses it.

Two workloads, because they fail differently and that difference is the most useful thing in
this repository:

  * ``join_skew`` — a large fact table joined to a dimension on the skewed key. This is the case
    Spark's adaptive execution can repair on its own, by splitting the oversized partition.
  * ``agg_skew`` — a group-by on the skewed key with ``sum``, ``count`` and ``max``. Written
    expecting it to need salting, and it does not, for a reason worth knowing: Spark
    aggregates partially *before* the shuffle, so a composable aggregation is already
    two-stage and the hot group's rows are folded down map-side. The measurement is in the
    README.
  * ``agg_wide`` — the same group-by with ``collect_list``, an aggregation that cannot be
    folded down, because every row has to survive into the result. Here the hot group's rows
    really do all have to arrive in one task, the partial aggregation buffers them instead of
    reducing them, and this is where a two-stage aggregation earns its complexity.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .errors import WorkloadError
from .logging_setup import get_logger

log = get_logger(__name__)

JOIN_SKEW = "join_skew"
AGG_SKEW = "agg_skew"
AGG_WIDE = "agg_wide"
WORKLOADS = (JOIN_SKEW, AGG_SKEW, AGG_WIDE)


@dataclass
class WorkloadSpec:
    """Everything that decides what the data looks like. Committed with every result."""

    name: str = JOIN_SKEW
    fact_rows: int = 4_000_000
    dim_rows: int = 20_000
    hot_keys: int = 3
    hot_share: float = 0.72
    seed: int = 20260814
    payload_chars: int = 48

    def validate(self) -> None:
        if self.name not in WORKLOADS:
            raise WorkloadError(f"unknown workload {self.name!r}, expected one of {WORKLOADS}")
        if self.fact_rows < 1000:
            raise WorkloadError("fact_rows below 1000 cannot show a distribution")
        if self.dim_rows < 2:
            raise WorkloadError("dim_rows must be at least 2")
        if not 1 <= self.hot_keys <= max(1, self.dim_rows // 2):
            raise WorkloadError("hot_keys must be at least 1 and well under dim_rows")
        if not 0.0 <= self.hot_share < 1.0:
            raise WorkloadError("hot_share must be in [0, 1)")
        if self.payload_chars < 0:
            raise WorkloadError("payload_chars cannot be negative")

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_fact(spark, spec: WorkloadSpec):
    """The large side: ``hot_share`` of the rows land on ``hot_keys``, the rest on the tail.

    The hot/tail decision is made from the row id rather than from a random draw, so the split
    is exact instead of approximately right, and identical on every run.
    """
    from pyspark.sql import functions as F

    spec.validate()
    hot_cutoff = int(spec.fact_rows * spec.hot_share)
    frame = spark.range(0, spec.fact_rows, numPartitions=32).withColumnRenamed("id", "row_id")

    # Deterministic pseudo-random values from the row id: a hash, not an RNG, so partitioning
    # and ordering cannot change what any row contains.
    noise = F.abs(F.hash(F.col("row_id"), F.lit(spec.seed)))
    hot_key = F.pmod(noise, F.lit(spec.hot_keys))
    tail_key = F.lit(spec.hot_keys) + F.pmod(noise, F.lit(spec.dim_rows - spec.hot_keys))

    frame = frame.withColumn(
        "key", F.when(F.col("row_id") < hot_cutoff, hot_key).otherwise(tail_key).cast("int")
    ).withColumn(
        "amount", (F.pmod(noise, F.lit(100_000)) / F.lit(100.0)).cast("double")
    ).withColumn(
        "region", F.element_at(
            F.array(F.lit("eu-west"), F.lit("us-east"), F.lit("us-west"), F.lit("ap-south")),
            (F.pmod(noise, F.lit(4)) + F.lit(1)).cast("int"),
        )
    )
    if spec.payload_chars:
        # A payload column so a partition's size is dominated by bytes rather than by row
        # count, which is what makes spill reachable at a realistic row count.
        frame = frame.withColumn(
            "payload",
            F.rpad(F.sha2(F.col("row_id").cast("string"), 256), spec.payload_chars, "x"),
        )
    return frame


def build_dim(spark, spec: WorkloadSpec):
    """The small side: one row per key, with a label wide enough to matter in a shuffle."""
    from pyspark.sql import functions as F

    spec.validate()
    return (
        spark.range(0, spec.dim_rows, numPartitions=4)
        .withColumnRenamed("id", "key")
        .withColumn("key", F.col("key").cast("int"))
        .withColumn("label", F.concat(F.lit("key-"), F.col("key").cast("string")))
        .withColumn("tier", F.pmod(F.col("key"), F.lit(7)).cast("int"))
    )


def payload_column_present(spec: WorkloadSpec) -> bool:
    return spec.payload_chars > 0


def key_distribution(spark, spec: WorkloadSpec, top: int = 10) -> list[dict[str, Any]]:
    """What the generator actually produced, measured rather than asserted.

    This exists because a generator is code and code has bugs: the first version of this
    module put 72% of the rows on *one* key when it was asked for three, and the only way that
    showed up was counting.
    """
    from pyspark.sql import functions as F

    fact = build_fact(spark, spec)
    total = spec.fact_rows
    rows = (
        fact.groupBy("key").count()
        .orderBy(F.col("count").desc())
        .limit(top)
        .collect()
    )
    return [
        {"key": int(r["key"]), "rows": int(r["count"]), "share": round(r["count"] / total, 6)}
        for r in rows
    ]
