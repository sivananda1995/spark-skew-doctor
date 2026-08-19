"""The strategies, written so that each one is a small readable diff against the naive plan.

Five ways to run the same query, and the point of the repository is that they are not
interchangeable:

  * ``naive`` — a plain shuffle join or group-by with adaptive execution switched off. The
    control. Somebody has to run it or the improvement has no denominator.
  * ``aqe`` — the same query with adaptive execution and its skew-join rule on. This is the
    "have you tried turning it on" answer, and for joins it is often the whole fix, which is
    worth measuring before writing any code.
  * ``salted`` — split the hot keys into ``salt`` sub-keys and replicate the small side across
    all of them. Costs a replication of the dimension table and the explanation of a join key
    that is no longer the join key.
  * ``broadcast`` — send the small side to every executor and delete the shuffle. When it fits,
    it is strictly better than every other option here, and the honest framing is that most
    "skew problems" in the wild are a missing broadcast.
  * ``isolated`` — process the hot keys separately from the tail and union the results. The
    two-pass approach: more code, no replication, and it degrades gracefully when the hot set
    is known and small.

For aggregation there is no broadcast to reach for, and adaptive execution's skew handling
does not apply, so the two options are ``naive``/``aqe`` and a **two-stage aggregation**:
aggregate per salt, then combine. That only works for aggregations that compose — sum, count,
min, max — and `salted_aggregation` refuses anything else rather than returning a plausible
wrong number.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import WorkloadError
from .logging_setup import get_logger

log = get_logger(__name__)

NAIVE = "naive"
AQE = "aqe"
SALTED = "salted"
BROADCAST = "broadcast"
ISOLATED = "isolated"

JOIN_STRATEGIES = (NAIVE, AQE, SALTED, BROADCAST, ISOLATED)
AGG_STRATEGIES = (NAIVE, AQE, SALTED)

# Aggregations that survive being computed in two stages. sum of sums is a sum; count of
# counts is a sum; max of maxes is a max. An average of averages is not an average, and a
# count of distinct values cannot be combined from partial counts at all.
COMPOSABLE = {"sum", "count", "max", "min"}


@dataclass
class StrategyConf:
    """How a strategy is set up. Recorded with every result so a run can be reproduced."""

    name: str = NAIVE
    salt: int = 16
    aqe: bool = False
    aqe_skew_join: bool = False
    broadcast_small_side: bool = False
    hot_keys: tuple[int, ...] = ()

    @property
    def slug(self) -> str:
        """A filename-safe identity for this configuration.

        It has to include the adaptive-execution flag. It did not, and two cells of the same
        matrix — the same strategy with AQE off and on — wrote their event logs to the same
        directory, so the second silently overwrote the first. The matrix results were still
        right, because each cell parsed its log before the next one ran, but the *stored* logs
        were not, and re-diagnosing them afterwards gave the wrong answer for half the matrix.
        """
        parts = [self.name]
        if self.name == SALTED:
            parts.append(f"salt{self.salt}")
        if self.name == ISOLATED:
            parts.append("hot" + "-".join(str(k) for k in self.hot_keys))
        parts.append("aqe" if self.aqe else "noaqe")
        return "-".join(parts)

    @property
    def summary(self) -> str:
        parts = [self.name]
        if self.name == SALTED:
            parts.append(f"salt={self.salt}")
        if self.name == ISOLATED:
            parts.append(f"hot_keys={list(self.hot_keys)}")
        if self.aqe:
            parts.append("aqe" + ("+skewJoin" if self.aqe_skew_join else ""))
        return " ".join(parts)

    def as_dict(self) -> dict[str, Any]:
        payload = self.__dict__.copy()
        payload["hot_keys"] = list(self.hot_keys)
        return payload


def with_aqe(strategy: StrategyConf, enabled: bool = True) -> StrategyConf:
    """Turn adaptive execution on or off for any strategy.

    AQE is an axis, not a strategy, and treating it as one is what makes the interesting
    question answerable: given that adaptive execution is on in production by default, is the
    hand-written fix still worth its complexity? That needs every strategy measured both ways.
    """
    from dataclasses import replace

    return replace(strategy, aqe=enabled, aqe_skew_join=enabled)


def for_join(name: str, salt: int = 16, hot_keys: tuple[int, ...] = ()) -> StrategyConf:
    if name not in JOIN_STRATEGIES:
        raise WorkloadError(
            f"unknown join strategy {name!r}, expected one of {JOIN_STRATEGIES}"
        )
    if name == ISOLATED and not hot_keys:
        raise WorkloadError(
            "the isolated strategy needs the hot keys, which is its real cost: it only works "
            "when they are known. Run 'skewdoc keys' to find them."
        )
    return StrategyConf(
        name=name,
        salt=salt,
        aqe=name == AQE,
        aqe_skew_join=name == AQE,
        broadcast_small_side=name == BROADCAST,
        hot_keys=hot_keys,
    )


def for_agg(name: str, salt: int = 16) -> StrategyConf:
    if name not in AGG_STRATEGIES:
        raise WorkloadError(
            f"unknown aggregation strategy {name!r}, expected one of {AGG_STRATEGIES}. "
            "broadcast and isolated do not apply to a group-by: there is no small side to "
            "broadcast, and isolating hot keys is what salting already does."
        )
    return StrategyConf(name=name, salt=salt, aqe=name == AQE, aqe_skew_join=name == AQE)


def _salt_column(salt: int):
    """A deterministic salt from the row id, so the split is reproducible run to run.

    Hashing the row id rather than calling rand() matters: a random salt makes the query
    non-deterministic, and a non-deterministic partitioning key breaks task retries, because a
    retried task would place its rows differently from the attempt it replaced.
    """
    from pyspark.sql import functions as F

    return F.pmod(F.abs(F.hash(F.col("row_id"), F.lit(0x5A17))), F.lit(salt)).cast("int")


def salted_join(fact, dim, salt: int, key: str = "key"):
    """Split the large side across ``salt`` sub-keys and replicate the small side to match.

    The replication is the cost and it is not hidden: the dimension table becomes ``salt``
    times larger, so this trades shuffle *volume* for shuffle *balance*. It pays when the
    small side is small and the hot partition is enormous, which is exactly when skew hurts.
    """
    from pyspark.sql import functions as F

    salted_fact = fact.withColumn("_salt", _salt_column(salt))
    exploded = dim.withColumn(
        "_salt", F.explode(F.sequence(F.lit(0), F.lit(salt - 1)))
    ).withColumn("_salt", F.col("_salt").cast("int"))
    return salted_fact.join(
        exploded,
        (salted_fact[key] == exploded[key]) & (salted_fact["_salt"] == exploded["_salt"]),
        "inner",
    ).drop(exploded[key]).drop("_salt")


def broadcast_join(fact, dim, key: str = "key"):
    """Delete the shuffle rather than balance it.

    `spark.sql.autoBroadcastJoinThreshold` is set to -1 for every run in this repository, so
    this is an explicit hint rather than a coincidence of table statistics — which is also the
    honest way to ship it, because statistics go stale and a plan that silently stops
    broadcasting is a 3am problem.
    """
    from pyspark.sql.functions import broadcast

    return fact.join(broadcast(dim), on=key, how="inner")


def isolated_join(fact, dim, hot_keys: tuple[int, ...], key: str = "key"):
    """Broadcast-join the hot keys, shuffle-join the tail, union the two.

    Two passes over the fact table, which is the cost, in exchange for no replication and no
    salt column leaking into downstream code. It also degrades honestly: if the hot set is
    wrong, the result is still correct, just slower.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.functions import broadcast

    hot = list(hot_keys)
    hot_fact = fact.where(F.col(key).isin(hot))
    cold_fact = fact.where(~F.col(key).isin(hot))
    hot_dim = dim.where(F.col(key).isin(hot))
    cold_dim = dim.where(~F.col(key).isin(hot))
    hot_side = hot_fact.join(broadcast(hot_dim), on=key, how="inner")
    cold_side = cold_fact.join(cold_dim, on=key, how="inner")
    return hot_side.unionByName(cold_side)


def salted_aggregation(fact, salt: int, key: str = "key", aggregation: str = "sum"):
    """Aggregate per salt, then combine — the only way to spread a hot group.

    Adaptive execution cannot help here. Its skew rule splits an oversized *join* partition
    into slices and joins each against a copy of the matching build-side partition, which
    works because a join is a per-row operation. A group's rows have to meet somewhere by
    definition, so the only way to split the work is to aggregate partially and combine, which
    changes the query rather than the plan.
    """
    # Validated before pyspark is imported, and the order is load-bearing twice. A caller who
    # asked for an average gets the error at once rather than after a JVM has started, and the
    # rule is a property of the aggregation rather than of Spark, so the test that pins it runs
    # on a machine with no JVM. With the import first, that test failed with ModuleNotFoundError
    # in the JVM-free CI job while passing on any laptop that had pyspark installed: green
    # locally, red in CI, from an import placed one line too early.
    if aggregation not in COMPOSABLE:
        raise WorkloadError(
            f"{aggregation!r} cannot be computed in two stages. Only {sorted(COMPOSABLE)} "
            "compose: an average of averages is not an average, and a distinct count cannot "
            "be combined from partial counts. Use a sketch (approx_count_distinct) or accept "
            "the single-stage plan."
        )

    from pyspark.sql import functions as F
    salted = fact.withColumn("_salt", _salt_column(salt))
    partial = salted.groupBy(key, "_salt").agg(
        F.sum("amount").alias("_amount"),
        F.count(F.lit(1)).alias("_rows"),
        F.max("amount").alias("_max_amount"),
    )
    return partial.groupBy(key).agg(
        F.sum("_amount").alias("total_amount"),
        F.sum("_rows").alias("rows"),
        F.max("_max_amount").alias("max_amount"),
    )


def naive_join(fact, dim, key: str = "key"):
    return fact.join(dim, on=key, how="inner")


def naive_wide_aggregation(fact, key: str = "key"):
    """A group-by whose aggregate cannot be folded down before the shuffle.

    ``collect_list`` has to keep every value, so the partial aggregation Spark inserts before a
    shuffle buffers the hot group's rows rather than reducing them. This is the aggregation
    shape where a hot key genuinely lands on one task with all of its data — and, measured,
    the only aggregation in this repository where salting is worth its complexity.
    """
    from pyspark.sql import functions as F

    return fact.groupBy(key).agg(
        F.collect_list("amount").alias("amounts"),
        F.count(F.lit(1)).alias("rows"),
    )


def salted_wide_aggregation(fact, salt: int, key: str = "key"):
    """Collect per salt, then flatten: the two-stage trick for a non-composable aggregate.

    Concatenation is associative, so the lists can be built independently and joined
    afterwards; the result is the same multiset of values in a different order, which is why
    the correctness check compares a sum over the lists rather than the lists themselves.
    """
    from pyspark.sql import functions as F

    salted = fact.withColumn("_salt", _salt_column(salt))
    partial = salted.groupBy(key, "_salt").agg(
        F.collect_list("amount").alias("_amounts"),
        F.count(F.lit(1)).alias("_rows"),
    )
    return partial.groupBy(key).agg(
        F.flatten(F.collect_list("_amounts")).alias("amounts"),
        F.sum("_rows").alias("rows"),
    )


def naive_aggregation(fact, key: str = "key"):
    from pyspark.sql import functions as F

    return fact.groupBy(key).agg(
        F.sum("amount").alias("total_amount"),
        F.count(F.lit(1)).alias("rows"),
        F.max("amount").alias("max_amount"),
    )
