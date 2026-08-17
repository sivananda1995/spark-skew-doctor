"""Renderings: a markdown summary for a pull request, and one self-contained HTML page.

Both are built only from measured structures. Nothing here computes a fact about a Spark job; if
a number appears, the diagnosis or the experiment produced it and this module formatted it.

The design rule for the diagnosis report is that a reader should be able to act without opening
the History Server: name the stage, the ratio, the executor that ran the slowest task, and the
recommendation. When the verdict is *not* key skew it says so loudly, because the expensive
mistake is reshaping data to fix a slow machine.
"""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

from .diagnose import HEALTHY, KEY_SKEW, SPILL, STARVED, STRAGGLER, ApplicationDiagnosis

VERDICT_LABEL = {
    KEY_SKEW: "key skew",
    STRAGGLER: "straggler, not skew",
    SPILL: "spill",
    STARVED: "too few tasks",
    HEALTHY: "healthy",
}
VERDICT_CLASS = {
    KEY_SKEW: "cell-bad",
    STRAGGLER: "cell-warn",
    SPILL: "cell-warn",
    STARVED: "cell-warn",
    HEALTHY: "cell-ok",
}


def read_json(path: str | Path) -> dict[str, Any] | None:
    target = Path(path)
    if not target.exists():
        return None
    try:
        return json.loads(target.read_text())
    except json.JSONDecodeError:
        return None


def _ms(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value / 1000:.2f} s" if value >= 1000 else f"{value:.0f} ms"


def _mb(value: float) -> str:
    return f"{value / 1e6:.1f} MB"


def render_markdown(diagnosis: ApplicationDiagnosis, top: int = 6) -> str:
    lines = [
        f"## Skew diagnosis: {VERDICT_LABEL.get(diagnosis.verdict, diagnosis.verdict)}",
        "",
        f"`{diagnosis.app_name}` ({diagnosis.app_id}), "
        f"{len(diagnosis.stages)} stage(s) with tasks. "
        f"AQE {'on' if diagnosis.aqe_enabled else 'off'}, "
        f"skew join {'on' if diagnosis.aqe_skew_join_enabled else 'off'}, "
        f"shuffle partitions {diagnosis.shuffle_partitions}.",
        "",
        "| stage | tasks | verdict | slowest | median | time ratio | bytes ratio | gini |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    ordered = sorted(diagnosis.stages, key=lambda s: -s.time.total)[:top]
    for stage in ordered:
        lines.append(
            f"| {stage.stage_id}.{stage.attempt} | {stage.tasks} | "
            f"{VERDICT_LABEL.get(stage.verdict, stage.verdict)} | "
            f"{_ms(stage.time.maximum)} | {_ms(stage.time.median)} | "
            f"{stage.time.ratio_max_median}x | {stage.read_bytes.ratio_max_median}x | "
            f"{stage.time.gini} |"
        )
    lines.append("")

    worst = diagnosis.worst
    if worst is None:
        lines += ["No stage crossed a threshold. Nothing here needs a skew fix.", ""]
        return "\n".join(lines)

    lines += [
        f"### Stage {worst.stage_id}.{worst.attempt}: "
        f"{VERDICT_LABEL.get(worst.verdict, worst.verdict)} "
        f"({worst.confidence} confidence)",
        "",
        worst.reason + ".",
        "",
        f"- slowest task {_ms(worst.time.maximum)} on executor "
        f"{worst.slowest_executor or '?'}, median {_ms(worst.time.median)}, "
        f"p95 {_ms(worst.time.p95)}",
        f"- {worst.read_source.replace('_', ' ')}: max {_mb(worst.read_bytes.maximum)}, "
        f"median {_mb(worst.read_bytes.median)}, "
        f"{worst.read_bytes.ratio_max_median}x ratio",
        f"- that one task is {worst.slowest_task_share:.0%} of the stage's task time",
    ]
    if worst.spilling_tasks:
        lines.append(
            f"- {worst.spilling_tasks} task(s) spilled, {_mb(worst.spilled_bytes)} to disk"
        )
    if worst.empty_task_share:
        lines.append(f"- {worst.empty_task_share:.0%} of tasks read no records at all")
    lines.append("")
    if worst.recommendations:
        lines += ["**What to do**", ""]
        lines += [f"1. {item}" for item in worst.recommendations]
        lines.append("")
    return "\n".join(lines)


def _matrix_rows(matrix: dict[str, Any] | None) -> str:
    if not matrix:
        return "<p class='hint'>No strategy comparison has been run for this workload.</p>"
    body = []
    for run in matrix.get("runs", []):
        aqe = run.get("diagnosis", {}).get("aqe_enabled")
        verdict = run.get("worst_stage_verdict", "")
        correct = run.get("correct")
        body.append(
            "<tr>"
            f"<td class='mono'>{html.escape(str(run.get('strategy', '')))}</td>"
            f"<td>{'on' if aqe else 'off'}</td>"
            f"<td class='num'>{_ms(run.get('wall_ms'))}</td>"
            f"<td class='num'>{run.get('speedup_vs_control', 0)}x</td>"
            f"<td class='{VERDICT_CLASS.get(verdict, '')}'>"
            f"{html.escape(VERDICT_LABEL.get(verdict, verdict))}</td>"
            f"<td class='num'>{run.get('worst_stage_time_ratio', 0)}x</td>"
            f"<td class='num'>{run.get('worst_stage_bytes_ratio', 0)}x</td>"
            f"<td class='num'>{_ms(run.get('total_task_ms'))}</td>"
            f"<td class='{'cell-ok' if correct else 'cell-bad'}'>"
            f"{'same answer' if correct else 'DIFFERENT ANSWER'}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>strategy</th><th>AQE</th><th>median wall</th><th>speedup</th>"
        "<th>worst stage</th><th>time ratio</th><th>bytes ratio</th><th>total task time</th>"
        "<th>correctness</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def _stage_rows(diagnosis: ApplicationDiagnosis) -> str:
    body = []
    for stage in sorted(diagnosis.stages, key=lambda s: -s.time.total):
        body.append(
            "<tr>"
            f"<td class='mono'>{stage.stage_id}.{stage.attempt}</td>"
            f"<td class='small'>{html.escape(stage.name[:44])}</td>"
            f"<td class='num'>{stage.tasks}</td>"
            f"<td class='{VERDICT_CLASS.get(stage.verdict, '')}'>"
            f"{html.escape(VERDICT_LABEL.get(stage.verdict, stage.verdict))}</td>"
            f"<td class='num'>{_ms(stage.time.maximum)}</td>"
            f"<td class='num'>{_ms(stage.time.median)}</td>"
            f"<td class='num'>{stage.time.ratio_max_median}x</td>"
            f"<td class='num'>{stage.read_bytes.ratio_max_median}x</td>"
            f"<td class='num'>{stage.time.gini}</td>"
            f"<td class='num'>{_mb(stage.spilled_bytes)}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>stage</th><th>name</th><th>tasks</th><th>verdict</th>"
        "<th>slowest task</th><th>median task</th><th>time ratio</th><th>bytes ratio</th>"
        "<th>gini</th><th>spilled</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def render_html(
    diagnosis: ApplicationDiagnosis,
    generated_at: str,
    matrix: dict[str, Any] | None = None,
    version: str = "",
    chart_svg: str = "",
) -> str:
    worst = diagnosis.worst
    verdict = diagnosis.verdict
    badge = "bad" if verdict == KEY_SKEW else ("ok" if verdict == HEALTHY else "warn")
    recommendations = (
        "".join(f"<li>{html.escape(item)}</li>" for item in worst.recommendations)
        if worst and worst.recommendations
        else "<li>Nothing: no stage crossed a threshold.</li>"
    )
    headline = (
        html.escape(worst.reason) + "."
        if worst
        else "Every stage's task distribution is inside the thresholds."
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>skewdoc report {html.escape(verdict)}</title>
<style>
  :root {{
    --surface:#fcfcfb; --panel:#fff; --line:#e4e3df; --ink:#0b0b0b; --ink2:#52514e;
    --muted:#7c7a75; --blue:#2a78d6; --red:#e34948; --amber:#eda100; --green:#008300;
  }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; padding:32px 36px 44px; background:var(--surface); color:var(--ink);
    font:14px/1.5 -apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }}
  h1 {{ font-size:20px; margin:0 0 2px; letter-spacing:-0.01em; }}
  h2 {{ font-size:15px; margin:30px 0 8px; }}
  .sub {{ color:var(--ink2); font-size:12.5px; margin:0 0 20px; }}
  .badge {{ display:inline-block; padding:3px 10px; border-radius:4px; font-weight:650;
    font-size:12px; letter-spacing:0.04em; margin-left:10px; vertical-align:3px; }}
  .badge.ok {{ background:var(--green); color:#fff; }}
  .badge.bad {{ background:var(--red); color:#fff; }}
  .badge.warn {{ background:var(--amber); color:#241a00; }}
  .tiles {{ display:flex; gap:12px; flex-wrap:wrap; margin:18px 0 6px; }}
  .tile {{ background:var(--panel); border:1px solid var(--line); border-radius:6px;
    padding:12px 14px; min-width:150px; }}
  .tile .label {{ color:var(--muted); font-size:11.5px; text-transform:uppercase;
    letter-spacing:0.05em; }}
  .tile .value {{ font-size:22px; font-weight:600; margin-top:4px;
    font-variant-numeric:tabular-nums; }}
  table {{ border-collapse:collapse; width:100%; background:var(--panel);
    border:1px solid var(--line); border-radius:6px; overflow:hidden; margin-bottom:6px; }}
  th,td {{ text-align:left; padding:7px 11px; border-bottom:1px solid var(--line);
    font-size:12.5px; }}
  th {{ background:#f6f5f2; color:var(--ink2); font-weight:600; font-size:11.5px;
    text-transform:uppercase; letter-spacing:0.04em; }}
  tr:last-child td {{ border-bottom:none; }}
  td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  td.mono,.mono {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }}
  .small {{ font-size:11.5px; color:var(--ink2); }}
  /* Cell helper names are prefixed so they cannot collide with a badge modifier: equal
     specificity plus a later declaration would silently repaint the badge. */
  .cell-ok {{ color:var(--green); font-weight:600; }}
  .cell-bad {{ color:var(--red); font-weight:700; }}
  .cell-warn {{ color:var(--amber); font-weight:600; }}
  .hint {{ color:var(--muted); font-size:12.5px; margin:0 0 10px; max-width:92ch; }}
  ol {{ margin:6px 0 0 20px; padding:0; }}
  li {{ margin-bottom:5px; max-width:92ch; }}
  .chart {{ background:var(--panel); border:1px solid var(--line); border-radius:6px;
    padding:10px; margin-bottom:8px; }}
  footer {{ color:var(--muted); font-size:11.5px; margin-top:28px; }}
</style></head>
<body>
  <h1>Spark skew diagnosis<span class="badge {badge}">
    {html.escape(VERDICT_LABEL.get(verdict, verdict).upper())}</span></h1>
  <p class="sub">{html.escape(diagnosis.app_name)} &middot; {html.escape(diagnosis.app_id)}
    &nbsp;&middot;&nbsp; AQE {'on' if diagnosis.aqe_enabled else 'off'},
    skew join {'on' if diagnosis.aqe_skew_join_enabled else 'off'},
    shuffle partitions {html.escape(str(diagnosis.shuffle_partitions))}
    &nbsp;&middot;&nbsp; skewdoc {html.escape(version)}
    &nbsp;&middot;&nbsp; {html.escape(generated_at)}</p>
  <p class="hint">{headline}</p>
  <div class="tiles">
    <div class="tile"><div class="label">stages with tasks</div>
      <div class="value">{len(diagnosis.stages)}</div></div>
    <div class="tile"><div class="label">slowest task</div>
      <div class="value">{_ms(worst.time.maximum) if worst else '-'}</div></div>
    <div class="tile"><div class="label">median task</div>
      <div class="value">{_ms(worst.time.median) if worst else '-'}</div></div>
    <div class="tile"><div class="label">time ratio</div>
      <div class="value">{worst.time.ratio_max_median if worst else 0}x</div></div>
    <div class="tile"><div class="label">bytes ratio</div>
      <div class="value">{worst.read_bytes.ratio_max_median if worst else 0}x</div></div>
    <div class="tile"><div class="label">slowest task's share</div>
      <div class="value">{f'{worst.slowest_task_share:.0%}' if worst else '-'}</div></div>
  </div>
  <h2>What to do</h2>
  <ol>{recommendations}</ol>
  {f'<h2>Task time distribution</h2><div class="chart">{chart_svg}</div>' if chart_svg else ''}
  <h2>Every stage that ran tasks</h2>
  <p class="hint">A high time ratio with a low bytes ratio is not key skew: the data was spread
    evenly and the task was slow anyway, which is a host or a GC pause rather than a partition.
    Reshaping the data cannot fix it.</p>
  {_stage_rows(diagnosis)}
  <h2>Strategy comparison</h2>
  <p class="hint">Each row is the median of repeated runs after a discarded warmup, with the
    answer checked against the control's row count and checksum. A strategy that changes the
    answer is marked, not ranked.</p>
  {_matrix_rows(matrix)}
  <footer>Every number here came from the Spark event log named in the run's JSON, parsed by
    skewdoc. Thresholds are printed in that JSON too, so a verdict can be re-derived.</footer>
</body></html>
"""
