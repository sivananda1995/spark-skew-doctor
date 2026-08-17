# ADR-002: the classification thresholds, and the two mistakes they made first

- **Status:** accepted
- **Date:** 2026-08-17
- **Related:** [ADR-003](ADR-003-measurement-protocol.md)

## Context

The classifier answers a four-way question — key skew, straggler, spill, too few tasks — and every
branch needs a number. Numbers in a diagnostic tool are where credibility is won or lost: too
sensitive and every stage is "skewed" until nobody reads the output, too lax and it misses the case
it exists for.

## Decision

Five thresholds, all overridable on the command line, all printed in the JSON so a verdict can be
re-derived from its inputs.

| threshold | default | where it comes from |
| --- | --- | --- |
| `time_ratio` | 5.0 | Spark's own `spark.sql.adaptive.skewJoin.skewedPartitionFactor` defaults to 5.0 for deciding a partition is skewed. Using the engine's own definition means a stage this tool calls skewed is a stage Spark would try to split |
| `bytes_ratio` | 2.0 | Deliberately low. It is not there to detect skew; it is there to *separate* key skew from a straggler, and a straggler's byte ratio sits within a few percent of 1.0, so 2.0 is a wide gap either side |
| `dominance` | 30% of stage task time in one task, **or** 4x a balanced task's share, whichever is larger | See the first mistake below |
| `min_tasks` | 4 | Below four tasks a distribution has no shape: one task is 25% of the stage by construction. Such a stage is reported as serial, not skewed |
| `min_stage_ms` | 250 ms total task time | A 5 ms stage with a 10x ratio is 4 ms of noise. Reporting it trains people to ignore the report |

The comparison is always time against *the bytes that stage actually read*: shuffle bytes for a
reduce stage, input bytes for a map stage, chosen by whether any task read from the shuffle at all.

## The first mistake: a dominance rule that measured the task count

The rule "one task holding 30% of a stage's task time is skewed on its own" fired on nearly every
stage of every adaptive-execution run. The reason is arithmetic rather than statistical: adaptive
execution coalesces small shuffle partitions, so a stage that had 64 tasks now has 5, and in a
5-task stage one task holds 20% by construction. The rule was reporting the effect of a *fix* as a
problem.

Scaling it fixes that: the threshold is `max(0.30, 4 / task_count)`, so 64 tasks still trip at 30%
while 5 tasks need 80% — a share that really is one task being the whole stage.

The overcorrection is worth recording too. My first attempt made the threshold purely relative
(`4 / task_count` with no floor), and re-diagnosing the committed logs then reported the naive
join — 15.43x time ratio, 41.71x bytes ratio, unambiguously skewed — as **healthy**, because the
selection of the "worst" stage depends on a stage being flagged at all. A false negative on the
repository's own headline example is a far worse failure than the false positive it replaced, and
the only reason it was caught within a minute is that re-diagnosing a stored event log is free.

## The second mistake: comparing a map stage against its shuffle read

A map stage reads from storage and writes to the shuffle; its shuffle *read* is zero for every
task. Comparing an uneven map stage's time against a byte ratio that is 0.0 by construction
classified every one of them as a straggler — "the data is spread evenly and the task was slow
anyway" — when in fact the input was uneven, which is key skew arriving from a different direction.

The parser already had `Input Metrics`, so the fix was to pick the distribution that describes what
the stage actually read. The diagnosis now records which one it used (`read_source`), because a
reader comparing the report against the History Server needs to know which column they are
looking at.

## Consequences

- Every verdict is reproducible from the log plus the thresholds, and both travel together in the
  JSON.
- The `straggler` branch remains the least exercised by real data, for the reason ADR-001 gives:
  local mode does not produce interesting stragglers. Its rules are covered by unit tests over
  hand-built distributions, which is a weaker guarantee, and it is stated as such.
- Thresholds derived from one workload on one machine are a starting point, not a calibration. The
  README's future-work section proposes a mode that derives them from a directory of healthy logs,
  which is what a team should actually run.

## Alternatives considered

**A single skew score instead of a verdict.** One number is easier to threshold and useless for
deciding what to do: the whole point is that a 10x ratio means different things depending on
whether the bytes moved with it.

**Learn the thresholds from labelled runs.** There is no labelled corpus of skewed Spark stages,
and building one would mean labelling by the very rules being learned.

**Use Spark's own AQE decisions as ground truth** (a stage is skewed if AQE split it). Attractive,
and circular for this repository, whose most interesting question is what AQE does and does not
handle. It also only exists when AQE is on.
