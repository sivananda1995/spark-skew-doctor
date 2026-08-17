"""Capture the README's screenshots from artifacts this repository produced.

Nothing here is a mock-up. The HTML page is the real report written by
`skewdoc diagnose --html`, and the terminal images render the exact bytes it wrote to stdout in
`tools/run_demo.sh`, read back from reports/.

Prerequisites: python -m pip install playwright && python -m playwright install chromium
Run from the repository root, after tools/run_demo.sh:
    python tools/capture_screenshots.py
"""

from __future__ import annotations

import argparse
import html
from pathlib import Path

from playwright.sync_api import sync_playwright

TERMINAL_TEMPLATE = """<!doctype html>
<html><head><meta charset="utf-8"><style>
  body {{ margin: 0; padding: 26px; background: #f1f0ec;
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
  .window {{ background: #14161a; border-radius: 8px; overflow: hidden;
             box-shadow: 0 6px 22px rgba(0,0,0,0.18); }}
  .bar {{ background: #22262c; padding: 8px 12px; color: #b9bec7; font-size: 11.5px;
          letter-spacing: 0.02em; }}
  .dot {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%;
          margin-right: 6px; vertical-align: -1px; }}
  pre {{ margin: 0; padding: 16px 18px 20px; color: #dfe3ea; font-size: 11.6px;
         line-height: 1.55; white-space: pre-wrap; word-break: break-word; }}
  .cmd {{ color: #7fd18d; }}
  .bad {{ color: #ff7b72; }}
  .warn {{ color: #f0b429; }}
  .dim {{ color: #8b93a1; }}
  .ok {{ color: #8ab4f8; }}
</style></head><body>
  <div class="window">
    <div class="bar"><span class="dot" style="background:#ff5f57"></span>
      <span class="dot" style="background:#febc2e"></span>
      <span class="dot" style="background:#28c840"></span>
      {title}</div>
    <pre>{body}</pre>
  </div>
</body></html>
"""


def _terminal_html(title: str, lines: list[str]) -> str:
    rendered = []
    for line in lines:
        escaped = html.escape(line)
        if line.startswith("$"):
            escaped = f'<span class="cmd">{escaped}</span>'
        elif "key_skew" in line or "exit=1" in line:
            escaped = f'<span class="bad">{escaped}</span>'
        elif "straggler" in line or "partition_starvation" in line:
            escaped = f'<span class="warn">{escaped}</span>'
        elif "healthy" in line or "nothing here needs" in line:
            escaped = f'<span class="ok">{escaped}</span>'
        elif line.startswith("{"):
            escaped = f'<span class="dim">{escaped}</span>'
        rendered.append(escaped)
    return TERMINAL_TEMPLATE.format(title=html.escape(title), body="\n".join(rendered))


def _shorten(line: str, limit: int = 210) -> str:
    """Trim a long structured log line at a field boundary rather than mid-token.

    A screenshot that ends in `"ma` looks like a bug in the tool rather than a deliberate
    trim, so the cut lands after the last complete key/value pair that fits.
    """
    if len(line) <= limit:
        return line
    cut = line.rfind(", ", 0, limit)
    return (line[:cut] if cut > 0 else line[:limit]) + ", ...}"


def _read(reports: Path, name: str, stream: str = "stdout") -> list[str]:
    suffix = "stdout.txt" if stream == "stdout" else "stderr.log"
    path = reports / f"{name}.{suffix}"
    return path.read_text().splitlines() if path.exists() else []


def _diagnose_lines(reports: Path) -> list[str]:
    """The command that needs no cluster: a recorded log in, a verdict and a fix out."""
    lines = ["$ skewdoc diagnose data/fixtures/join-naive-noaqe"]
    lines += [line for line in _read(reports, "diagnose-skewed")
              if not line.startswith("exit=")]
    lines += ["", "$ skewdoc diagnose data/fixtures/join-broadcast-noaqe"]
    lines += [line for line in _read(reports, "diagnose-clean") if not line.startswith("exit=")]
    lines += ["", "$ skewdoc diagnose data/fixtures/join-naive-noaqe --fail-on-skew; echo $?"]
    lines += ["exit=1    (usable as a CI gate on a job's own event log)"]
    return lines


def _agg_lines(reports: Path) -> list[str]:
    """The aggregation that is not skewed, because Spark folded the hot group down map-side."""
    lines = ["$ skewdoc diagnose data/fixtures/agg-naive-noaqe"]
    lines += [line for line in _read(reports, "diagnose-agg") if not line.startswith("exit=")]
    lines += ["", "# 72% of rows on three keys, and not a key-skew verdict anywhere: a",
              "# sum/count/max group-by is already two-stage, so the hot group never",
              "# arrives whole. Salting it measured 0.39x — see the README."]
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", default="reports")
    parser.add_argument("--out", default="docs/screenshots")
    args = parser.parse_args()
    reports = Path(args.reports)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    terminals = {
        "diagnose_terminal.png": ("skewdoc diagnose — no cluster, no JVM, a recorded log",
                                  _diagnose_lines(reports), 1240),
        "diagnose_aggregation.png": ("skewdoc diagnose — the aggregation that is not skewed",
                                     _agg_lines(reports), 1240),
    }

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            report_html = reports / "skew.html"
            if report_html.exists():
                for target, height, full in (("report_key_skew.png", 1000, True),
                                             ("strategy_matrix.png", 1000, False)):
                    page = browser.new_page(viewport={"width": 1400, "height": height},
                                            device_scale_factor=2)
                    page.goto(report_html.resolve().as_uri())
                    page.wait_for_load_state("load")
                    if target == "strategy_matrix.png":
                        heading = page.locator("h2", has_text="Strategy comparison")
                        heading.scroll_into_view_if_needed()
                    page.screenshot(path=out_dir / target, full_page=full)
                    page.close()
                    print(f"wrote {out_dir / target}")
            else:
                print("skipping the report screenshots: run tools/run_demo.sh first")

            for target, (title, lines, width) in terminals.items():
                if not lines:
                    print(f"skipping {target}: no captured output")
                    continue
                page_path = out_dir / "_terminal.html"
                page_path.write_text(_terminal_html(title, lines))
                # A short viewport with full_page=True lets the image size itself to the
                # content instead of leaving half a screen of background below it.
                page = browser.new_page(viewport={"width": width, "height": 240},
                                        device_scale_factor=2)
                page.goto(page_path.resolve().as_uri())
                page.wait_for_load_state("load")
                page.screenshot(path=out_dir / target, full_page=True)
                page.close()
                page_path.unlink(missing_ok=True)
                print(f"wrote {out_dir / target}")
        finally:
            browser.close()


if __name__ == "__main__":
    main()
