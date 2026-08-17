# ADR-001: measure in local mode, and be explicit about what a local measurement proves

- **Status:** accepted
- **Date:** 2026-08-17

## Context

Every number in this repository comes from `local[4]` on one machine: one JVM, four task slots, a
2-million-row fact table, no network shuffle. Real Spark jobs run on clusters with dozens of
executors, remote shuffle fetches, and datasets three orders of magnitude larger.

The temptation is to either (a) present the numbers as though they were cluster numbers, or (b)
refuse to publish them because they are not. Both are worse than the third option, which is to
publish them with a precise statement of what transfers.

## Decision

Measure in local mode. State, in the README and here, which conclusions survive the move to a
cluster and which do not.

**What transfers, because it is decided by the partitioner and the data rather than by the
hardware:**

- *The existence and shape of the skew.* Which partition a key lands in is `hash(key) % n`. Three
  keys holding 72% of the rows produce partitions holding 72% of the rows on any cluster of any
  size, and the ratio between the largest and the median partition is a property of the data.
- *The classification.* Whether the slow task also read more bytes is a per-task fact from the
  event log. The discriminator between key skew and a straggler does not know how many machines
  there were.
- *The ordering of the fixes, for this workload shape.* Broadcast removes a shuffle; salting adds
  replication; adaptive execution splits a partition. Those are structural changes to the plan,
  and their direction does not reverse because there are more executors.

**What does not transfer:**

- *Absolute times.* 292 ms for a broadcast join is a laptop fact and nothing else.
- *The magnitude of the speedups.* Broadcast's 6.51x will change on a cluster, and it will
  probably grow, because a real shuffle crosses the network while a local one does not.
- *Anything about network shuffle, remote fetch latency, or executor loss.* Local mode has no
  remote fetches, so `Fetch Wait Time` is near zero and the straggler-by-network case cannot be
  produced here at all. The classifier has a rule for it; that rule is tested on hand-built data
  and has never fired on a real log in this repository, and the README says so.
- *Straggler realism.* A real straggler is a busy host, a failing disk, or an executor mid-GC.
  Local mode has one JVM, so the stragglers observed here are scheduling and cache effects. The
  measured cases are real, but they are not the interesting production cases.

## Consequences

The strongest claim this repository can make is: *on a workload whose skew shape is realistic, with
a dimension table that fits in memory, on Spark 4.0.4 with a modern planner, the ordering of the
fixes is broadcast, then adaptive execution, then isolation, then salting — and salting lost to
doing nothing.* That is a useful, checkable, falsifiable claim, and it is deliberately narrower
than "salting is obsolete".

Costs accepted:

- A reader who wants cluster numbers does not get them, and the honest answer is to run the harness
  with `--master` pointed at one. That is one flag, and the reason the harness takes it.
- The `straggler` verdict is under-exercised by real data. It is covered by unit tests over
  hand-built distributions, which is weaker than a real log, and that gap is stated rather than
  papered over.
- The severity sweep's absolute times fall as skew *rises*, which is an artifact of how the
  generator moves rows onto fewer keys rather than a Spark behaviour. The sweep is therefore read
  down its columns (which fix wins at this severity) and not across its rows.

## Alternatives considered

**Run the matrix on a real cluster.** Better numbers, and it makes the repository unreproducible
for anyone reading it: a reviewer cannot check a claim that needs an EMR account. The design goal
is that `make compare` works on a laptop.

**Use a standard benchmark (TPC-DS).** Well-known shape, published results, and skew is not its
point: the interesting TPC-DS skew is mild and incidental. A generator with an explicit `hot_share`
makes severity a parameter, which is what the sweep needs.

**Simulate Spark's execution instead of running it.** Fast, deterministic, and it would measure the
simulator. The whole value here is that the metrics come from Spark's own event log.
