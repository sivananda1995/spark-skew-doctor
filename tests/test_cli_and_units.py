"""The command line, the report renderers, and the validation that keeps a bad run out.

Nothing here starts Spark. Two tests marked ``spark`` do, and they are the ones that prove
the event log the parser reads is the event log Spark actually writes; everything else works
from the committed logs, so the suite stays fast and runnable on a machine with no JVM.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from skewdoc.charts import stage_svg, task_time_svg
from skewdoc.cli import main
from skewdoc.diagnose import diagnose
from skewdoc.errors import WorkloadError
from skewdoc.eventlog import parse
from skewdoc.fixes import (
    AGG_STRATEGIES,
    COMPOSABLE,
    JOIN_STRATEGIES,
    for_agg,
    for_join,
    with_aqe,
)
from skewdoc.report import read_json, render_html, render_markdown
from skewdoc.workloads import WorkloadSpec

FIXTURES = Path(__file__).resolve().parents[1] / "data" / "fixtures"
SKEWED = FIXTURES / "join-naive-noaqe"
CLEAN = FIXTURES / "join-broadcast-noaqe"


# ------------------------------------------------------------------ validation

def test_a_workload_spec_refuses_impossible_values():
    with pytest.raises(WorkloadError, match="hot_share"):
        WorkloadSpec(hot_share=1.0).validate()
    with pytest.raises(WorkloadError, match="fact_rows"):
        WorkloadSpec(fact_rows=10).validate()
    with pytest.raises(WorkloadError, match="hot_keys"):
        WorkloadSpec(hot_keys=0).validate()
    with pytest.raises(WorkloadError, match="unknown workload"):
        WorkloadSpec(name="nope").validate()


def test_an_unknown_strategy_lists_the_known_ones():
    with pytest.raises(WorkloadError) as excinfo:
        for_join("magic")
    for name in JOIN_STRATEGIES:
        assert name in str(excinfo.value)


def test_isolation_refuses_to_guess_the_hot_keys():
    """Its whole cost is needing to know them, so pretending otherwise would hide the trade."""
    with pytest.raises(WorkloadError, match="hot keys"):
        for_join("isolated")


def test_aggregation_refuses_the_strategies_that_do_not_apply():
    with pytest.raises(WorkloadError) as excinfo:
        for_agg("broadcast")
    assert "no small side to" in str(excinfo.value)
    assert set(AGG_STRATEGIES) == {"naive", "aqe", "salted"}


def test_the_slug_distinguishes_the_aqe_setting():
    """Two cells that differ only in AQE must not share an event-log directory."""
    off = with_aqe(for_join("naive"), False)
    on = with_aqe(for_join("naive"), True)
    assert off.slug != on.slug
    assert off.slug.endswith("noaqe") and on.slug.endswith("aqe")
    assert for_join("salted", salt=32).slug == "salted-salt32-noaqe"


def test_only_composable_aggregations_can_be_computed_in_two_stages():
    assert {"sum", "count", "max", "min"} == COMPOSABLE
    from skewdoc.fixes import salted_aggregation

    with pytest.raises(WorkloadError, match="average of averages"):
        salted_aggregation(object(), salt=4, aggregation="avg")


def test_the_composability_rule_is_checked_before_pyspark_is_needed(monkeypatch):
    """The rule is a property of the aggregation, so it holds on a machine with no JVM.

    Pinned with pyspark made unimportable rather than by trusting the line order, because that
    is how this broke: the import sat one line above the validation, so every laptop with
    pyspark installed ran green while the JVM-free CI job failed on the import.
    """
    import builtins

    from skewdoc.fixes import salted_aggregation

    real_import = builtins.__import__

    def refuse_pyspark(name, *args, **kwargs):
        if name.startswith("pyspark"):
            raise ModuleNotFoundError("No module named 'pyspark'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", refuse_pyspark)
    with pytest.raises(WorkloadError, match="average of averages"):
        salted_aggregation(object(), salt=4, aggregation="avg")


# ------------------------------------------------------------------ rendering

def test_the_markdown_report_names_the_stage_and_the_fix():
    diagnosis = diagnose(parse(SKEWED))
    markdown = render_markdown(diagnosis)
    assert "Skew diagnosis: key skew" in markdown
    assert "time ratio" in markdown
    assert "What to do" in markdown
    assert "broadcast" in markdown


def test_the_markdown_report_says_when_there_is_nothing_to_do():
    markdown = render_markdown(diagnose(parse(CLEAN)))
    assert "Nothing here needs a skew fix" in markdown


def test_the_html_report_is_self_contained_and_carries_the_chart():
    diagnosis = diagnose(parse(SKEWED))
    html = render_html(diagnosis, "2026-08-17T00:00:00+00:00", version="test",
                       chart_svg=task_time_svg(diagnosis))
    assert "<!doctype html>" in html
    assert "KEY SKEW" in html
    assert "<svg" in html and "<rect" in html
    assert "http://" not in html and "src=" not in html, "no external assets"


def test_the_html_report_marks_a_strategy_that_changed_the_answer():
    diagnosis = diagnose(parse(SKEWED))
    matrix = {"runs": [{
        "strategy": "salted", "wall_ms": 100.0, "speedup_vs_control": 2.0,
        "worst_stage_verdict": "healthy", "worst_stage_time_ratio": 1.1,
        "worst_stage_bytes_ratio": 1.0, "total_task_ms": 500.0, "correct": False,
        "diagnosis": {"aqe_enabled": True},
    }]}
    html = render_html(diagnosis, "2026-08-17T00:00:00+00:00", matrix=matrix)
    assert "DIFFERENT ANSWER" in html


def test_the_badge_classes_cannot_collide_with_the_cell_classes():
    """Same bug as the sibling project: equal specificity, later rule wins, invisible badge."""
    import re

    html = render_html(diagnose(parse(SKEWED)), "2026-08-17T00:00:00+00:00")
    badges = set(re.findall(r"\.badge\.([a-z-]+)", html))
    cells = set(re.findall(r"class='(cell-[a-z]+)'", html))
    assert badges and cells
    assert badges & cells == set()


def test_the_chart_draws_one_bar_per_task():
    diagnosis = diagnose(parse(SKEWED))
    worst = diagnosis.worst
    svg = stage_svg(worst)
    # One bar per task in each of the two panels.
    assert svg.count("<rect") == worst.tasks * 2
    assert "one bar per task" in svg


def test_read_json_tolerates_a_missing_or_broken_file(tmp_path):
    assert read_json(tmp_path / "absent.json") is None
    broken = tmp_path / "broken.json"
    broken.write_text("{oops")
    assert read_json(broken) is None


# ------------------------------------------------------------------ cli

def test_diagnose_prints_a_table_and_exits_zero(capsys):
    assert main(["diagnose", str(SKEWED)]) == 0
    printed = capsys.readouterr().out
    assert "key_skew" in printed
    assert "worst:" in printed and "why:" in printed


def test_diagnose_can_act_as_a_ci_gate(capsys):
    assert main(["diagnose", str(SKEWED), "--fail-on-skew"]) == 1
    capsys.readouterr()
    assert main(["diagnose", str(CLEAN), "--fail-on-skew"]) == 0


def test_diagnose_emits_json_with_the_thresholds_it_used(capsys):
    assert main(["diagnose", str(SKEWED), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["verdict"] == "key_skew"
    assert payload["thresholds"]["time_ratio"] == 5.0
    assert payload["stages"], "the per-stage detail is the point of the json"


def test_diagnose_honours_overridden_thresholds(capsys):
    assert main(["diagnose", str(SKEWED), "--json", "--time-ratio", "999",
                 "--bytes-ratio", "999"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["verdict"] == "healthy", "nothing is skewed if nothing can be"


def test_diagnose_writes_both_reports(tmp_path, capsys):
    markdown = tmp_path / "out" / "report.md"
    html = tmp_path / "out" / "report.html"
    assert main(["diagnose", str(SKEWED), "--markdown", str(markdown),
                 "--html", str(html)]) == 0
    capsys.readouterr()
    assert "key skew" in markdown.read_text()
    assert "<svg" in html.read_text()


def test_a_missing_event_log_exits_three(capsys):
    assert main(["diagnose", "no/such/log"]) == 3


@pytest.mark.spark
def test_a_real_run_produces_a_log_this_parser_understands(tmp_path):
    """The one property no fixture can prove: the format written is the format read."""
    pytest.importorskip("pyspark")
    from skewdoc import fixes
    from skewdoc.experiment import run

    spec = WorkloadSpec(fact_rows=200_000, dim_rows=2_000, hot_keys=2, hot_share=0.7)
    result, _ = run(spec, fixes.for_join("naive"), log_root=str(tmp_path),
                    shuffle_partitions=16, repeats=1)
    assert result.rows == 200_000
    assert result.task_count > 0
    assert Path(result.event_log).exists()
    application = parse(result.event_log)
    assert application.task_count == result.task_count


@pytest.mark.spark
def test_a_strategy_that_changed_the_answer_would_be_caught(tmp_path):
    """The correctness check is the reason a comparison can be trusted, so it is tested."""
    pytest.importorskip("pyspark")
    from skewdoc import fixes
    from skewdoc.experiment import run

    spec = WorkloadSpec(fact_rows=200_000, dim_rows=2_000, hot_keys=2, hot_share=0.7)
    control, fingerprint = run(spec, fixes.for_join("naive"), log_root=str(tmp_path),
                               shuffle_partitions=16, repeats=1)
    salted, _ = run(spec, fixes.for_join("salted", salt=8), log_root=str(tmp_path),
                    shuffle_partitions=16, control=fingerprint, repeats=1)
    assert salted.correct is True, "salting must not change the answer"

    wrong = fingerprint.__class__(rows=fingerprint.rows + 1, checksum=fingerprint.checksum)
    assert not fingerprint.matches(wrong)
    assert control.rows == fingerprint.rows
