"""One chart, drawn as inline SVG with no dependency on anything.

The single most useful picture of a skewed stage is its per-task duration, sorted, with the
median marked: a healthy stage is a flat line with a small tail, key skew is a flat line with
one bar ten times the height of the rest, and a straggler looks the same in *time* while its
bytes bar stays flat — which is why both are drawn together. Every bar is one real task, taken
from the event log; nothing is reconstructed from quantiles.

Hand-written SVG rather than matplotlib, for two reasons. The report is one self-contained
HTML file, so an inline `<svg>` needs no image file and no base64 blob; and the chart then
works in a build that installed no plotting library, which keeps the diagnosis path
dependency-free.
"""

from __future__ import annotations

import html

from .diagnose import ApplicationDiagnosis, StageDiagnosis

WIDTH = 900
HEIGHT = 260
PAD_LEFT = 54
PAD_RIGHT = 12
PAD_TOP = 22
PAD_BOTTOM = 30


def _bars(values: list[float], colour: str, top: float, x0: int, plot_width: int,
          plot_height: int, baseline: int) -> str:
    if not values:
        return ""
    step = plot_width / len(values)
    bar_width = max(1.0, step * 0.82)
    out = []
    for index, value in enumerate(sorted(values)):
        height = 0.0 if top <= 0 else (value / top) * plot_height
        x = x0 + index * step
        out.append(
            f"<rect x='{x:.1f}' y='{baseline - height:.1f}' width='{bar_width:.1f}' "
            f"height='{height:.1f}' fill='{colour}' />"
        )
    return "".join(out)


def stage_svg(stage: StageDiagnosis) -> str:
    """Sorted per-task duration and per-task bytes for one stage, side by side."""
    plot_width = (WIDTH - PAD_LEFT - PAD_RIGHT - 40) / 2
    plot_height = HEIGHT - PAD_TOP - PAD_BOTTOM
    baseline = PAD_TOP + plot_height
    time_top = max(stage.time.maximum, 1.0)
    bytes_top = max(stage.read_bytes.maximum, 1.0)
    median_y = baseline - (stage.time.median / time_top) * plot_height

    left = _bars(stage.task_times_ms, "#2a78d6", time_top, PAD_LEFT, int(plot_width),
                 int(plot_height), baseline)
    right_x = int(PAD_LEFT + plot_width + 40)
    right = _bars(stage.task_read_bytes, "#7c7a75", bytes_top, right_x, int(plot_width),
                  int(plot_height), baseline)

    return f"""<svg viewBox="0 0 {WIDTH} {HEIGHT}" width="100%" role="img"
  aria-label="Per-task duration and bytes read for stage {stage.stage_id}, sorted">
  <text x="{PAD_LEFT}" y="14" font-size="11" fill="#52514e">task duration (ms), sorted —
    median {stage.time.median:.0f}, max {stage.time.maximum:.0f}
    ({stage.time.ratio_max_median}x)</text>
  <text x="{right_x}" y="14" font-size="11" fill="#52514e">{
      html.escape(stage.read_source.replace('_', ' '))} bytes, sorted —
    {stage.read_bytes.ratio_max_median}x</text>
  <line x1="{PAD_LEFT}" y1="{baseline}" x2="{PAD_LEFT + plot_width:.0f}" y2="{baseline}"
    stroke="#e4e3df" />
  <line x1="{right_x}" y1="{baseline}" x2="{right_x + plot_width:.0f}" y2="{baseline}"
    stroke="#e4e3df" />
  <line x1="{PAD_LEFT}" y1="{median_y:.1f}" x2="{PAD_LEFT + plot_width:.0f}"
    y2="{median_y:.1f}" stroke="#e34948" stroke-dasharray="4 3" />
  {left}{right}
  <text x="{PAD_LEFT}" y="{HEIGHT - 8}" font-size="10.5" fill="#7c7a75">one bar per task
    ({stage.tasks} tasks), sorted; the dashed line is the median duration</text>
</svg>"""


def task_time_svg(diagnosis: ApplicationDiagnosis) -> str:
    """The chart for the stage worth looking at, or the busiest one if all are healthy."""
    if not diagnosis.stages:
        return ""
    stage = diagnosis.worst or max(diagnosis.stages, key=lambda s: s.time.total)
    return stage_svg(stage)
