# ADR-003: warmup, repeats, medians, and a correctness check on every strategy

- **Status:** accepted
- **Date:** 2026-08-17
- **Related:** [ADR-001](ADR-001-local-mode-and-what-it-proves.md)

## Context

The first version of the comparison harness ran each cell of the matrix once and reported the wall
time. It said salting was 1.7x faster than the naive plan. Salting is 0.81x. The entire difference
was measurement error, and the error was systematic rather than random: the naive plan ran first,
and the first Spark query in a process is slow.

Measured directly, the same query, unchanged, three times in one process:

| attempt | wall time |
| --- | --- |
| 1 | 6,457 ms |
| 2 | 3,483 ms |
| 3 | 2,846 ms |

The first run is 2.3x the third. Across the committed matrices the warmup run runs up to 3.4x
slower than the median of the runs after it. Whole-JVM code generation, JIT compilation of the
generated classes, and a cold page cache all land on the first execution.

A matrix that times each cell once therefore ranks its cells partly by the order they ran in, and
because the first cell is conventionally the control, the bias systematically flatters every fix.

## Decision

Each cell of every matrix is measured like this:

1. Start one session with the strategy's configuration.
2. Run the query once and **throw the number away**.
3. Run it `repeats` more times (3 for the published matrices) and keep the **median**.
4. Start a *second* session and run the query exactly once, so the event log being diagnosed
   contains one execution rather than four.
5. Reduce the result to a row count and a checksum, and compare it with the control's.

The warmup time and every sample are recorded in the JSON next to the median, so a reader can see
the effect rather than take the protocol's word for it.

Step 5 is not about speed at all. Salting adds a column, isolation unions two frames, a two-stage
aggregation rewrites the plan — each is an opportunity to return a subtly different answer faster,
which is the worst possible outcome. A strategy that disagrees with the control is reported as
**wrong** and is not ranked. All 16 published cells agreed with their control.

## Consequences

- A matrix costs roughly five query executions per cell instead of one, plus a second session
  start. The join matrix takes about four minutes on a laptop, which is affordable for something
  published.
- The median of three is a weak estimator, and it is honest about that: samples are published, so
  a reader can see the spread. The right answer for a paper would be more repeats and a confidence
  interval; the right answer for a repository whose point is the *ordering* of four strategies is
  three repeats and visible samples.
- Step 4 doubles the number of Spark sessions. The alternative — parsing the log and attributing
  stages to the fourth job — is more code and a subtler failure mode, since adaptive execution can
  produce more than one job per action.
- The published absolute times are still laptop times (ADR-001). What the protocol buys is that the
  *comparison* is not an artifact of ordering.

## Alternatives considered

**Randomise cell order instead of warming up.** Converts a systematic bias into a random one,
which is better and still noisy: with one sample per cell the warmup lands somewhere and distorts
whichever cell gets it.

**Report the minimum instead of the median.** Common in microbenchmarks and defensible, since the
minimum is the least-contaminated sample. Rejected because the interesting quantity here is what a
scheduled job would experience, and that is not the best case.

**One session for all cells.** Faster, and it makes the AQE axis impossible: adaptive execution is
a session-level configuration, and changing it mid-session changes what previous cells were
measuring.

**Trust `total_task_ms` from the event log instead of wall time.** It is a genuinely useful second
number and it is published — it is how the salting result is *explained*, since salting burns more
task time than the naive plan even when it balances better. But it is not what anybody waits for,
so wall time is the headline and task time is the diagnosis.
