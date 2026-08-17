"""Classification tests: the part that has to be right or the tool is worse than nothing.

Two kinds of test here. Hand-built stages pin each rule against a distribution whose verdict is
obvious by construction, including the ones that must *not* fire. Then the committed real logs
check that the rules still classify actual Spark runs the way the README says they do — the
difference between a classifier that works on paper and one that works on a job.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from skewdoc.diagnose import (
    HEALTHY,
    KEY_SKEW,
    SPILL,
    STARVED,
    STRAGGLER,
    Thresholds,
    describe,
    diagnose,
    diagnose_stage,
    gini,
)
from skewdoc.eventlog import StageRecord, TaskRecord, parse

FIXTURES = Path(__file__).resolve().parents[1] / "data" / "fixtures"


def task(index: int, run_ms: int, read_bytes: int = 1_000_000, records: int = 1000,
         disk_spill: int = 0, gc_ms: int = 0, fetch_wait_ms: int = 0,
         executor: str = "1", input_bytes: int = 0) -> TaskRecord:
    return TaskRecord(
        stage_id=1, stage_attempt=0, task_id=index, index=index, executor_id=executor,
        host="host", launch_time_ms=0, finish_time_ms=run_ms, run_ms=run_ms,
        cpu_ms=float(run_ms), gc_ms=gc_ms, shuffle_read_bytes=read_bytes,
        shuffle_read_records=records, shuffle_write_bytes=0, shuffle_write_records=0,
        input_bytes=input_bytes, input_records=0, output_records=0, memory_spilled=0,
        disk_spilled=disk_spill, peak_memory=0, fetch_wait_ms=fetch_wait_ms, failed=False,
        speculative=False,
    )


def stage(tasks: list[TaskRecord], name: str = "collect at job.py:12") -> StageRecord:
    record = StageRecord(stage_id=1, attempt=0, name=name, num_tasks=len(tasks),
                         submission_time_ms=0, completion_time_ms=max(
                             (t.run_ms for t in tasks), default=0))
    record.tasks = list(tasks)
    return record


def test_a_balanced_stage_is_healthy():
    tasks = [task(i, 100 + i) for i in range(16)]
    result = diagnose_stage(stage(tasks))
    assert result.verdict == HEALTHY
    assert result.confidence == "high"


def test_time_and_bytes_moving_together_is_key_skew():
    tasks = [task(i, 100, read_bytes=1_000_000) for i in range(15)]
    tasks.append(task(15, 3_000, read_bytes=40_000_000))
    result = diagnose_stage(stage(tasks))
    assert result.verdict == KEY_SKEW
    assert result.confidence == "high"
    assert "the time follows the data" in result.reason
    assert any("broadcast" in item for item in result.recommendations)


def test_time_without_bytes_is_a_straggler_not_skew():
    """The expensive mistake this discriminator exists to prevent."""
    tasks = [task(i, 100, read_bytes=1_000_000) for i in range(15)]
    tasks.append(task(15, 3_000, read_bytes=1_050_000, executor="7"))
    result = diagnose_stage(stage(tasks))
    assert result.verdict == STRAGGLER
    assert "the data is spread evenly" in result.reason
    assert any("salting does nothing" in item for item in result.recommendations)
    assert result.slowest_executor == "7"


def test_a_gc_bound_straggler_says_so_first():
    tasks = [task(i, 100) for i in range(15)]
    tasks.append(task(15, 3_000, read_bytes=1_050_000, gc_ms=1_500))
    result = diagnose_stage(stage(tasks))
    assert result.verdict == STRAGGLER
    assert "GC" in result.recommendations[0]


def test_a_fetch_bound_straggler_points_upstream():
    tasks = [task(i, 100) for i in range(15)]
    tasks.append(task(15, 3_000, read_bytes=1_050_000, fetch_wait_ms=2_000))
    result = diagnose_stage(stage(tasks))
    assert "waiting on shuffle fetches" in result.recommendations[0]


def test_spill_without_imbalance_is_reported_as_spill():
    tasks = [task(i, 400, disk_spill=50_000_000) for i in range(12)]
    result = diagnose_stage(stage(tasks))
    assert result.verdict == SPILL
    assert result.spilling_tasks == 12
    assert any("partition count" in item for item in result.recommendations)


def test_too_few_tasks_is_starvation_rather_than_skew():
    tasks = [task(i, 2_000) for i in range(3)]
    result = diagnose_stage(stage(tasks))
    assert result.verdict == STARVED
    assert "serial rather than skewed" in result.reason


def test_a_trivial_stage_is_not_judged_at_all():
    """A 5 ms stage with a 10x ratio is 4 ms of noise, not a finding."""
    tasks = [task(i, 1) for i in range(8)] + [task(8, 10)]
    result = diagnose_stage(stage(tasks))
    assert result.verdict == HEALTHY
    assert "below the" in result.reason


def test_the_dominance_rule_scales_with_task_count():
    """One task holding 40% of a 5-task stage is arithmetic, not skew."""
    tasks = [task(i, 100) for i in range(4)] + [task(4, 260)]
    result = diagnose_stage(stage(tasks))
    assert result.verdict == HEALTHY, result.reason

    many = [task(i, 20) for i in range(63)] + [task(63, 700)]
    crowded = diagnose_stage(stage(many))
    assert crowded.slowest_task_share > 0.3
    assert crowded.verdict in (KEY_SKEW, STRAGGLER)


def test_thresholds_are_overridable():
    tasks = [task(i, 100, read_bytes=1_000_000) for i in range(15)]
    tasks.append(task(15, 350, read_bytes=4_000_000))
    assert diagnose_stage(stage(tasks)).verdict == HEALTHY
    strict = diagnose_stage(stage(tasks), Thresholds(time_ratio=3.0, bytes_ratio=2.0))
    assert strict.verdict == KEY_SKEW


def test_a_map_stage_is_judged_on_input_bytes_not_shuffle_bytes():
    """A map stage's shuffle read is zero for every task, so comparing to it proves nothing."""
    tasks = [task(i, 100, read_bytes=0, records=0, input_bytes=1_000_000) for i in range(15)]
    tasks.append(task(15, 3_000, read_bytes=0, records=0, input_bytes=40_000_000))
    result = diagnose_stage(stage(tasks))
    assert result.read_source == "input"
    assert result.verdict == KEY_SKEW, "input skew is still key skew"


def test_failed_and_speculative_tasks_are_excluded():
    tasks = [task(i, 100) for i in range(8)]
    failed = task(8, 5_000)
    object.__setattr__(failed, "failed", True)
    speculative = task(9, 5_000)
    object.__setattr__(speculative, "speculative", True)
    result = diagnose_stage(stage([*tasks, failed, speculative]))
    assert result.tasks == 8
    assert result.verdict == HEALTHY


def test_an_empty_stage_is_not_a_crash():
    result = diagnose_stage(stage([]))
    assert result.verdict == HEALTHY
    assert result.tasks == 0


def test_per_task_samples_are_kept_for_the_chart():
    tasks = [task(i, 100 + i) for i in range(9)]
    result = diagnose_stage(stage(tasks))
    assert result.task_times_ms == [float(100 + i) for i in range(9)]
    assert len(result.task_read_bytes) == 9


@pytest.mark.parametrize(
    ("values", "expected"),
    [([1, 1, 1, 1], 0.0), ([0, 0, 0, 4], 0.75), ([], 0.0), ([0, 0], 0.0)],
)
def test_gini_matches_hand_computation(values, expected):
    assert round(gini(values), 4) == expected


def test_describe_uses_a_real_task_for_the_percentile():
    """An interpolated p95 reports a duration no task took, which nobody can go and look at."""
    values = [10.0, 20.0, 30.0, 40.0, 1000.0]
    described = describe(values)
    assert described.p95 in values
    assert described.median == 30.0
    assert described.ratio_max_median == round(1000 / 30, 2)


def test_a_zero_median_does_not_produce_an_infinite_ratio():
    described = describe([0.0, 0.0, 0.0, 500.0])
    assert described.ratio_max_median == 0.0


def test_the_real_naive_join_is_diagnosed_as_key_skew():
    """The committed log from the README's naive run, classified by the shipped thresholds."""
    diagnosis = diagnose(parse(FIXTURES / "join-naive-noaqe"))
    assert diagnosis.verdict == KEY_SKEW
    worst = diagnosis.worst
    assert worst is not None
    assert worst.tasks == 64
    assert worst.time.ratio_max_median > 5
    assert worst.read_bytes.ratio_max_median > 10
    assert worst.read_source == "shuffle_read"
    assert not diagnosis.aqe_enabled


def test_the_real_broadcast_join_has_no_skewed_stage():
    diagnosis = diagnose(parse(FIXTURES / "join-broadcast-noaqe"))
    assert diagnosis.verdict == HEALTHY
    assert diagnosis.worst is None


def test_the_real_composable_aggregation_is_not_flagged():
    """Spark's map-side partial aggregation already folded the hot group down."""
    diagnosis = diagnose(parse(FIXTURES / "agg-naive-noaqe"))
    assert diagnosis.verdict != KEY_SKEW
