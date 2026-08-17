"""Logging behaviour, and the command paths that need Spark to reach.

The logging tests exist because a log call must never be able to take a pipeline down, and
because task metrics are aggregates but hot keys are business data: those belong in a report
under the job's access control, not in a log line.
"""

from __future__ import annotations

import json
import logging

import pytest

from skewdoc.charts import task_time_svg
from skewdoc.cli import main
from skewdoc.diagnose import ApplicationDiagnosis, Thresholds
from skewdoc.logging_setup import JsonFormatter, TextFormatter, configure, get_logger


def _record(**extra) -> logging.LogRecord:
    record = logging.LogRecord("skewdoc.test", logging.INFO, __file__, 1,
                               "diagnosed stage", (), None)
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_the_json_formatter_emits_one_object_with_the_extras():
    payload = json.loads(JsonFormatter().format(_record(stage=3, verdict="key_skew")))
    assert payload["event"] == "diagnosed stage"
    assert payload["stage"] == 3
    assert payload["verdict"] == "key_skew"
    assert payload["level"] == "INFO"


def test_the_json_formatter_never_raises_on_an_unserialisable_extra():
    payload = json.loads(JsonFormatter().format(_record(frame=object())))
    assert payload["frame"].startswith("<unserialisable object")


def test_the_json_formatter_records_an_exception_as_one_line():
    try:
        raise ValueError("stage 7 has no tasks")
    except ValueError:
        import sys
        record = _record()
        record.exc_info = sys.exc_info()
    payload = json.loads(JsonFormatter().format(record))
    assert "stage 7 has no tasks" in payload["error"]


def test_the_text_formatter_sorts_extras_for_stable_output():
    line = TextFormatter().format(_record(zebra=1, alpha=2))
    assert line.index("alpha=2") < line.index("zebra=1")
    assert "diagnosed stage" in line


def test_configure_replaces_handlers_rather_than_stacking_them():
    configure("INFO", "json")
    configure("ERROR", "text")
    logger = logging.getLogger("skewdoc")
    assert len(logger.handlers) == 1
    assert logger.level == logging.ERROR
    assert not logger.propagate


def test_get_logger_namespaces_everything_under_skewdoc():
    assert get_logger("diagnose").name == "skewdoc.diagnose"
    assert get_logger("skewdoc.cli").name == "skewdoc.cli"


def test_the_chart_of_an_empty_diagnosis_is_empty():
    diagnosis = ApplicationDiagnosis(
        app_id="a", app_name="n", source="s", stages=[], thresholds=Thresholds(),
        aqe_enabled=True, aqe_skew_join_enabled=True, shuffle_partitions="8",
    )
    assert task_time_svg(diagnosis) == ""


@pytest.mark.spark
def test_the_run_command_reports_a_median_and_its_samples(capsys):
    pytest.importorskip("pyspark")
    code = main(["run", "--workload", "join_skew", "--strategy", "naive",
                 "--fact-rows", "200000", "--dim-rows", "2000", "--hot-keys", "2",
                 "--shuffle-partitions", "16", "--repeats", "2", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert code in (0, 1)
    assert len(payload["wall_ms_samples"]) == 2
    assert payload["warmup_ms"] > 0
    assert payload["rows"] == 200_000


@pytest.mark.spark
def test_the_compare_command_ranks_strategies_and_checks_the_answers(tmp_path, capsys):
    pytest.importorskip("pyspark")
    out = tmp_path / "matrix.json"
    code = main(["compare", "--workload", "agg_skew", "--aqe", "on",
                 "--fact-rows", "200000", "--dim-rows", "2000", "--hot-keys", "2",
                 "--shuffle-partitions", "16", "--repeats", "1", "--out", str(out)])
    printed = capsys.readouterr().out
    assert code == 0, printed
    assert "speedup" in printed
    payload = json.loads(out.read_text())
    assert payload["all_correct"] is True
    assert len(payload["runs"]) == 2
    assert payload["repeats"] == 1


@pytest.mark.spark
def test_the_keys_command_measures_the_distribution_it_was_asked_for(capsys):
    pytest.importorskip("pyspark")
    code = main(["keys", "--fact-rows", "200000", "--dim-rows", "2000", "--hot-keys", "2",
                 "--hot-share", "0.6", "--top", "4", "--json"])
    distribution = json.loads(capsys.readouterr().out)
    assert code == 0
    assert len(distribution) == 4
    hot = sum(row["share"] for row in distribution[:2])
    assert 0.55 < hot < 0.65, "the generator must produce the share it was asked for"
