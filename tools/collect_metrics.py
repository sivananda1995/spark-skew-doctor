"""Re-measure every number this repository publishes, into one machine-readable registry.

The values come from three places, and each is recorded with the command that produced it:

  * the **committed event logs** under `data/fixtures/`, re-parsed and re-diagnosed here, so
    every claim about what the diagnosis says is derived rather than remembered;
  * the **comparison JSON** under `docs/experiments/` and `benchmark/results/`, written by the
    Spark runs, because re-running twenty Spark jobs inside the receipts step would make
    `make verify` take twenty minutes;
  * the **test suite**, for the test count and coverage.

That split is why the receipts job in CI needs no JVM: it checks that the documents agree
with the artifacts, and the Spark job checks that the artifacts can be regenerated.

Run from the repository root: python tools/collect_metrics.py
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from skewdoc.diagnose import diagnose  # noqa: E402
from skewdoc.eventlog import parse  # noqa: E402
from skewdoc.logging_setup import configure  # noqa: E402

CHECKED_DOCUMENTS = [
    "README.md",
    "docs/adr/ADR-001-local-mode-and-what-it-proves.md",
    "docs/adr/ADR-002-classification-thresholds.md",
    "docs/adr/ADR-003-measurement-protocol.md",
]

# Anchor phrases: templates with one {} where the measured value belongs. The checker puts the
# value in and requires the whole phrase, pinning a claim to the sentence that makes it.
ANCHORS: dict[str, list[str]] = {
    "naive_time_ratio": ["ran {}x the median"],
    "naive_bytes_ratio": ["read {}x the median bytes"],
    "naive_slowest_task_share": ["{} of the stage's task time"],
    "naive_tasks": ["one task per {} shuffle partitions"],
    "naive_verdict": ["diagnosed as {}"],
    "broadcast_verdict": ["broadcast run is {}"],
    "agg_naive_verdict": ["composable aggregation is {}"],
    "join_naive_ms": ["naive join takes {} ms"],
    "join_broadcast_speedup": ["broadcast is {}x"],
    "join_aqe_speedup": ["adaptive execution alone is {}x"],
    "join_salted_speedup": ["salting is {}x"],
    "join_isolated_speedup": ["isolating the hot keys is {}x"],
    "agg_salted_speedup": ["salting a composable aggregation is {}x"],
    "agg_wide_salted_speedup": ["salting it is {}x"],
    "agg_wide_aqe_speedup": ["adaptive execution gives {}x"],
    "cells_where_salting_won": ["salting won {} of them"],
    "cells_measured": ["{} strategy cells"],
    "severity_levels": ["{} severities"],
    "warmup_over_median": ["{}x slower than the median"],
    "test_count": ["badge/tests-{}-", "{} tests and", "{} tests, and"],
    "line_coverage_pct": ["badge/coverage-{}25-", "{} line coverage"],
}


def entry(value, display, source: str, note: str = "") -> dict:
    return {
        "value": value,
        "display": display if isinstance(display, list) else [display],
        "source": source,
        "note": note,
    }


def attach_anchors(metrics: dict) -> None:
    unknown = sorted(set(ANCHORS) - set(metrics))
    if unknown:
        raise SystemExit(
            "ANCHORS names metrics that this run did not measure: "
            f"{', '.join(unknown)}. Remove them or fix the metric name."
        )
    for name, record in metrics.items():
        record["anchors"] = ANCHORS.get(name)


def collect_from_fixtures(metrics: dict) -> None:
    """Re-diagnose the committed logs, so every claim about a verdict is derived here."""
    naive = diagnose(parse("data/fixtures/join-naive-noaqe"))
    worst = naive.worst
    metrics["naive_verdict"] = entry(
        naive.verdict, naive.verdict.replace("_", " "),
        "skewdoc diagnose data/fixtures/join-naive-noaqe")
    metrics["naive_time_ratio"] = entry(
        worst.time.ratio_max_median, str(worst.time.ratio_max_median),
        "skewdoc diagnose data/fixtures/join-naive-noaqe")
    metrics["naive_bytes_ratio"] = entry(
        worst.read_bytes.ratio_max_median, str(worst.read_bytes.ratio_max_median),
        "skewdoc diagnose data/fixtures/join-naive-noaqe")
    metrics["naive_slowest_task_share"] = entry(
        worst.slowest_task_share, f"{worst.slowest_task_share:.0%}",
        "skewdoc diagnose data/fixtures/join-naive-noaqe")
    metrics["naive_tasks"] = entry(
        worst.tasks, str(worst.tasks),
        "skewdoc diagnose data/fixtures/join-naive-noaqe")
    metrics["broadcast_verdict"] = entry(
        diagnose(parse("data/fixtures/join-broadcast-noaqe")).verdict,
        diagnose(parse("data/fixtures/join-broadcast-noaqe")).verdict.replace("_", " "),
        "skewdoc diagnose data/fixtures/join-broadcast-noaqe")
    metrics["agg_naive_verdict"] = entry(
        diagnose(parse("data/fixtures/agg-naive-noaqe")).verdict,
        diagnose(parse("data/fixtures/agg-naive-noaqe")).verdict.replace("_", " "),
        "skewdoc diagnose data/fixtures/agg-naive-noaqe")


def _cell(matrix: dict, strategy: str, aqe: bool) -> dict:
    for run in matrix["runs"]:
        if run["strategy"] == strategy and run["diagnosis"]["aqe_enabled"] is aqe:
            return run
    raise SystemExit(f"no {strategy} cell with aqe={aqe} in the matrix")


def collect_from_matrices(metrics: dict) -> None:
    join = json.loads(Path("docs/experiments/join_matrix.json").read_text())
    agg = json.loads(Path("docs/experiments/agg_matrix.json").read_text())
    wide = json.loads(Path("docs/experiments/agg_wide_matrix.json").read_text())
    severity = json.loads(Path("benchmark/results/severity.json").read_text())
    source = "skewdoc compare --workload join_skew"

    naive_off = _cell(join, "naive", False)
    metrics["join_naive_ms"] = entry(
        naive_off["wall_ms"], f"{naive_off['wall_ms']:.0f}", source,
        "median of repeated runs after a discarded warmup")
    for strategy, key in (("broadcast", "join_broadcast_speedup"),
                          ("salted", "join_salted_speedup"),
                          ("isolated", "join_isolated_speedup")):
        cell = _cell(join, strategy, False)
        metrics[key] = entry(
            cell["speedup_vs_control"], str(cell["speedup_vs_control"]), source)
    aqe_cell = _cell(join, "naive", True)
    metrics["join_aqe_speedup"] = entry(
        aqe_cell["speedup_vs_control"], str(aqe_cell["speedup_vs_control"]), source)

    agg_salted = _cell(agg, "salted", False)
    metrics["agg_salted_speedup"] = entry(
        agg_salted["speedup_vs_control"], str(agg_salted["speedup_vs_control"]),
        "skewdoc compare --workload agg_skew")
    wide_salted = _cell(wide, "salted", False)
    metrics["agg_wide_salted_speedup"] = entry(
        wide_salted["speedup_vs_control"], str(wide_salted["speedup_vs_control"]),
        "skewdoc compare --workload agg_wide")
    wide_aqe = _cell(wide, "naive", True)
    metrics["agg_wide_aqe_speedup"] = entry(
        wide_aqe["speedup_vs_control"], str(wide_aqe["speedup_vs_control"]),
        "skewdoc compare --workload agg_wide")

    # How many cells were measured in total, and in how many of them salting was the fastest
    # option. This is the headline claim, so it is counted rather than asserted.
    cells = []
    for matrix in (join, agg, wide):
        for run in matrix["runs"]:
            cells.append((matrix["workload"], run["diagnosis"]["aqe_enabled"],
                          run["strategy"], run["wall_ms"]))
    groups: dict[tuple[str, bool], list[tuple[str, float]]] = {}
    for workload, aqe, strategy, wall in cells:
        groups.setdefault((workload, aqe), []).append((strategy, wall))
    salt_wins = sum(
        1 for members in groups.values()
        if min(members, key=lambda item: item[1])[0] == "salted"
    )
    metrics["cells_measured"] = entry(len(cells), str(len(cells)),
                                      "the three comparison JSON files")
    metrics["cells_where_salting_won"] = entry(
        salt_wins, str(salt_wins), "the three comparison JSON files",
        "groups are (workload, AQE setting); salting wins one by being its fastest strategy")
    metrics["severity_levels"] = entry(
        len(severity["summary"]), str(len(severity["summary"])),
        "python benchmark/bench_severity.py")

    # The warmup effect, as a ratio, from the run that shows it most clearly.
    worst_warmup = max(
        (run for matrix in (join, agg, wide) for run in matrix["runs"]),
        key=lambda run: run["warmup_ms"] / max(run["wall_ms"], 1),
    )
    metrics["warmup_over_median"] = entry(
        round(worst_warmup["warmup_ms"] / worst_warmup["wall_ms"], 1),
        str(round(worst_warmup["warmup_ms"] / worst_warmup["wall_ms"], 1)),
        "the warmup_ms and wall_ms fields of the comparison JSON",
        "the first run of a query against the median of the runs after it")


def collect_test_health(metrics: dict, skip: bool) -> None:
    if skip:
        registry = json.loads(Path("docs/metrics.json").read_text())["metrics"]
        metrics["test_count"] = registry["test_count"]
        metrics["line_coverage_pct"] = registry["line_coverage_pct"]
        return
    junit = Path("build/junit.xml")
    junit.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "--cov=skewdoc", "--cov-report=xml",
         f"--junitxml={junit}"],
        check=True, capture_output=True,
    )
    root_xml = ET.parse("coverage.xml").getroot()
    coverage = float(root_xml.attrib["line-rate"])
    root = ET.parse(junit).getroot()
    suites = root.findall("testsuite") or [root]
    tests = sum(int(suite.attrib.get("tests", 0)) for suite in suites)
    metrics["test_count"] = entry(tests, str(tests), "pytest --junitxml")
    metrics["line_coverage_pct"] = entry(
        round(coverage * 100, 1),
        [f"{coverage * 100:.0f}%", f"{coverage * 100:.1f}%"],
        "pytest --cov=skewdoc --cov-report=xml")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="docs/metrics.json")
    parser.add_argument("--skip-tests", action="store_true",
                        help="reuse the committed test count and coverage instead of "
                             "re-running the suite; used by the receipts job, which has no JVM")
    args = parser.parse_args()

    configure("ERROR", "text")
    metrics: dict = {}
    collect_from_fixtures(metrics)
    collect_from_matrices(metrics)
    collect_test_health(metrics, args.skip_tests)
    attach_anchors(metrics)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "environment": {"python": platform.python_version(), "platform": platform.platform()},
        "how_to_use": (
            "Every value here was produced by the command in its source field. "
            "tools/check_numbers.py asserts that each one still appears in the documents "
            "listed under checked_documents, inside its anchor phrase, so a stale claim "
            "fails CI."
        ),
        "checked_documents": CHECKED_DOCUMENTS,
        "metrics": metrics,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote {out} with {len(metrics)} measured values")


if __name__ == "__main__":
    main()
