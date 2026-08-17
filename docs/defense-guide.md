# Defending this project in an interview

Written for the person who has to answer questions about this repository under pressure. Everything
here is checkable from the code, and where the honest answer is "it does not do that", the honest
answer is here.

Read [ADR-003](adr/ADR-003-measurement-protocol.md) first. The measurement protocol is the part
most likely to be probed, and it is the part where I was wrong before I was right.

## The 90-second version

> Spark jobs get slow because one task does most of the work, and the standard response is to salt
> the hot key. I wanted to know whether that is still true, so I built two things: a diagnoser that
> reads Spark's own event log and classifies each stage, and a harness that runs the fixes against
> each other on real Spark and checks that each one still returns the right answer.
>
> The diagnoser's point is that "skewed" is four different problems. It compares each stage's task
> *time* distribution against the *bytes* those tasks read. If both are lopsided, it is key skew and
> reshaping the data will help. If time is lopsided and bytes are not, it is a straggler — a host, a
> GC pause — and salting will do nothing. Spill and too-few-tasks are the other two. Everything
> comes from the event log, so it needs no cluster, no JVM, and no rerun of the job.
>
> Then the measured part. On a 2-million-row join with 72% of rows on three keys, across 16 cells,
> salting won none of them. Broadcast was 6.5x, adaptive execution alone was 1.5x, and salting was
> 0.81x — slower than doing nothing, because replicating the dimension table 16 times costs more
> than the imbalance did. I also expected salting to be necessary for aggregation skew and was
> wrong twice: for `sum`/`count`/`max` Spark's map-side partial aggregation already folds the hot
> group down, and for `collect_list` the combining stage still has to assemble the hot key's array
> in one task.

## Questions you will be asked

**"Are you saying salting is useless?"**
No, and the README is careful about this. It says: on this workload, on Spark 4 with adaptive
execution available and a dimension table that fits in memory, broadcast then AQE beat salting, and
salting was slower than doing nothing. Salting is still the answer when the build side is too large
to broadcast, when AQE is unavailable (Spark 2.x, or switched off to stabilise plans), or when the
hot partition is so large that even a split partition does not fit. The point is that "we have skew,
add salt" should be a measurement, and the measurement takes one command.

**"Why is salting slower? Shouldn't a balanced shuffle always win?"**
Balance is not free. Salting with 16 buckets explodes the dimension table 16 times, so the shuffle
moves more bytes to move them more evenly. The numbers show both halves: the time ratio falls from
15.4x to 7.3x (it *did* balance) while total task time rises from 6.67 s to 8.20 s (it cost more
than it saved). That total-task-time column is why the result is explainable rather than mysterious.

**"How do you know your timings mean anything?"**
Because the first version didn't. Each cell ran once, and it reported salting as 1.7x *faster*. The
first Spark query in a process pays for JIT compilation and a cold page cache: three consecutive
identical runs took 6,457 ms, 3,483 ms, 2,846 ms. Now every cell discards a warmup and reports the
median of three, and the warmup times are published next to the medians so a reader can see the
effect. The correct number is 0.81x.

**"Why the event log rather than a SparkListener?"**
Because of when the question gets asked. A listener sees jobs it was attached to; the event log
exists after the fact, which is when someone says "last night's job took four hours". It also means
the tool needs no py4j callback server, no JVM, and no connection to the cluster — the diagnosis path
is pure Python over a file. The cost is that it cannot diagnose a running job, which is in the
out-of-scope list.

**"What does the classifier get wrong?"**
Two things it got wrong before, both in [ADR-002](adr/ADR-002-classification-thresholds.md). The
dominance rule ("one task holds 30% of stage time") fired on nearly every AQE stage, because AQE
coalesces 64 partitions into 5 and one of 5 tasks holds 20% by arithmetic; it now scales with task
count. And comparing a *map* stage's time against its shuffle-read bytes classified every uneven map
stage as a straggler, because a map stage's shuffle read is zero by definition; it now compares
against input bytes and records which it used. What remains weak is the straggler branch itself:
local mode does not produce real stragglers, so those rules are covered by unit tests over
hand-built distributions rather than by a real log, and the README says so.

**"Would this work on my cluster?"**
The diagnosis, yes, unchanged: point it at `spark.eventLog.dir`. Rolling logs, zstd and lz4
compression, and stage retries are handled. The comparison harness takes `--master`, but I have not
run it on a cluster, so the absolute numbers here are a laptop's. [ADR-001](adr/ADR-001-local-mode-and-what-it-proves.md)
separates what transfers (the shape of the skew, the classification, the ordering of the fixes)
from what does not (milliseconds, the size of the speedups, anything about network shuffle).

**"How do you know a fix didn't break the query?"**
Every strategy's result is reduced to a row count and a checksum and compared with the control's,
and a strategy that disagrees is reported as *wrong* rather than ranked as fast. This is not
theoretical: salting adds a column, isolation unions two frames, the two-stage aggregation rewrites
the plan. All 16 cells agreed. There is a float tolerance of 1e-9 relative, because a two-stage sum
adds in a different order and floating-point addition is not associative.

**"Why is the aggregation not diagnosed as skewed when 72% of rows are on three keys?"**
Because by the time the data reaches the shuffle, it isn't. Spark inserts a partial aggregation
before the exchange, so each map task emits one partial row per key and the hot group's million rows
are folded into a handful of values. The shuffle is nearly uniform, which the byte-ratio measurement
shows directly. That is why salting a composable aggregation measured 0.39x: it adds a stage to redo
work the planner already did.

**"What would you build next?"**
A threshold calibration mode. The numbers in the classifier come from Spark's own skew factor plus
one workload on one machine, and a team should derive them from a directory of their own healthy
logs instead. After that, a cluster run of the same matrix, and write-side skew, which shares the
diagnosis machinery and has completely different fixes.

## What it does not do

- **No cluster numbers.** Everything is `local[4]`. Stated in the README, in ADR-001, and here.
- **No live diagnosis.** Event logs only.
- **It does not rewrite your query.** It diagnoses and ranks recommendations; a human picks.
- **Straggler detection is under-exercised by real data**, for the reason above.
- **No write-side skew, no multi-column keys, no streaming.** All in the out-of-scope section with
  a trigger for revisiting each.
- **The severity sweep's absolute times fall as skew rises**, which is an artifact of the generator
  concentrating rows on fewer keys, not a Spark behaviour. The sweep is read down its columns, and
  the README says so.

## The four numbers to remember

- **15.43x time ratio and 41.71x bytes ratio** on the naive join: both lopsided, so it is key skew.
- **6.51x** for broadcast; **1.52x** for adaptive execution alone; **0.81x** for salting.
- **0 of 16** cells won by salting.
- **3.4x**: how much slower a warmup run is than the median of the runs after it, which is why the
  protocol exists.
