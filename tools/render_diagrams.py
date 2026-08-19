"""Render this repository's architecture diagram to a committed SVG.

## Why the SVG is committed rather than rendered by GitHub

GitHub renders fenced ```mermaid blocks itself, and when that works it is the nicest option:
the source is the picture. It does not always work. The diagram in this README parses and
renders with mermaid 10 and 11 locally, and GitHub still showed "Unable to render rich
display: Cannot read properties of undefined (reading 'render')", which is a failure inside
their lazily loaded renderer rather than a syntax error in the diagram. There is nothing to
fix in the source, and no way to fix it from here.

So the picture is generated once, committed, and embedded as an image. That renders
identically on GitHub, in an editor's markdown preview, in a PDF export, and offline, which
is the same standard every other artefact in this repository is held to. The source stays in
the README underneath, in a plain fence so nothing tries to render it, and this script
regenerates the image when it changes.

## Running it

Needs Node with the mermaid package and Playwright with Chromium, neither of which is a
dependency of the package itself:

    npm install mermaid@11
    python tools/render_diagrams.py

It reads every fence marked `mermaid-source` in README.md, so adding a diagram means adding
a fence and re-running this.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "diagrams"

#: Fences this script renders. `mermaid-source` rather than `mermaid`, so GitHub leaves the
#: block alone and shows it as text next to the image built from it.
FENCE = re.compile(r"```mermaid-source(?:[ \t]+name=([\w-]+))?\n(.*?)```", re.DOTALL)


def candidates() -> list[pathlib.Path]:
    return [ROOT / "node_modules" / "mermaid" / "dist" / "mermaid.min.js",
            pathlib.Path("/tmp/mermaid-check/node_modules/mermaid/dist/mermaid.min.js")]


def library() -> pathlib.Path:
    for path in candidates():
        if path.exists():
            return path
    raise SystemExit("mermaid is not installed. Run: npm install mermaid@11")


SVG_ROOT = re.compile(r'<svg([^>]*?)>')
VIEWBOX = re.compile(r'viewBox="([-\d.]+) ([-\d.]+) ([\d.]+) ([\d.]+)"')


def sized(svg: str) -> str:
    """Give the root element intrinsic dimensions taken from its own viewBox.

    Mermaid emits `width="100%"` with a `max-width` style, which is right for a diagram
    injected into a live page and wrong for one embedded with `<img src>`: with no height
    and a percentage width, a browser has no aspect ratio to work from and picks a default.
    Explicit width and height from the viewBox give the image an intrinsic size, and the
    viewBox stays so it still scales to its container.

    A background rectangle is also painted in, and that decision was made by looking at the
    result rather than by reasoning about it. A transparent background sounds like the
    theme-neutral choice, and it is not: the node fills are light with dark text either way,
    so on GitHub's dark theme the subgraph titles and the edge labels, which have no fill
    behind them, end up dark grey on near black. One light background reads correctly under
    both schemes.
    """
    box = VIEWBOX.search(svg)
    if not box:
        return svg
    left, top = float(box.group(1)), float(box.group(2))
    width, height = float(box.group(3)), float(box.group(4))
    backdrop = (f'<rect x="{left}" y="{top}" width="{width}" height="{height}" '
                f'fill="#fbfbf9" stroke="none"/>')

    def rewrite(match: re.Match[str]) -> str:
        attributes = match.group(1)
        attributes = re.sub(r'\s*width="[^"]*"', "", attributes)
        attributes = re.sub(r'\s*height="[^"]*"', "", attributes)
        attributes = re.sub(r'\s*style="[^"]*"', "", attributes)
        return f'<svg width="{width:.0f}" height="{height:.0f}"{attributes}>{backdrop}'

    return SVG_ROOT.sub(rewrite, svg, count=1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readme", default="README.md")
    parser.add_argument("--theme", default="neutral",
                        help="mermaid theme; neutral reads on both colour schemes")
    args = parser.parse_args(argv)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("playwright is required: pip install playwright && "
              "playwright install chromium", file=sys.stderr)
        return 2

    readme = (ROOT / args.readme).read_text()
    jobs = []
    for index, match in enumerate(FENCE.finditer(readme), start=1):
        name = match.group(1) or ("architecture" if index == 1 else f"diagram-{index}")
        jobs.append({"name": name, "code": match.group(2)})
    if not jobs:
        print(f"no ```mermaid-source fences in {args.readme}; nothing to render")
        return 0

    # The library is attached with add_script_tag rather than inlined into a scratch page.
    # Inlining three megabytes of JavaScript into a file:// document worked for two of the
    # four repositories in this series and timed out for the other two, which is the kind of
    # flake that costs an hour to attribute. A script tag is also how a browser is meant to
    # be handed a library.
    OUT.mkdir(parents=True, exist_ok=True)
    render_js = """
      window.__render = async (jobs, theme) => {
        // htmlLabels must be false. With it on, mermaid puts HTML inside a
        // <foreignObject>, including unclosed <br> tags, so the file is not
        // well-formed XML. It still displays when injected into a live page and
        // fails to load at all as <img src>, which is how it is embedded here:
        // naturalWidth 0, a broken-image icon, and nothing in any console to say
        // why. With htmlLabels off the labels are <text> and <tspan> instead,
        // and the file is valid SVG.
        mermaid.initialize({startOnLoad: false, theme: theme, securityLevel: 'strict',
                            flowchart: {useMaxWidth: true, htmlLabels: false}});
        const out = [];
        for (const j of jobs) {
          try {
            const {svg} = await mermaid.render('g-' + j.name, j.code);
            out.push({name: j.name, svg: svg, error: null});
          } catch (e) {
            out.push({name: j.name, svg: null, error: String((e && e.message) || e)});
          }
        }
        return out;
      };
    """
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_content("<!doctype html><html><body></body></html>")
        page.add_script_tag(path=str(library()))
        page.add_script_tag(content=render_js)
        page.wait_for_function("typeof window.__render === 'function'", timeout=60_000)
        results = page.evaluate("([jobs, theme]) => window.__render(jobs, theme)",
                                [jobs, args.theme])
        browser.close()

    failed = [entry for entry in results if entry["error"]]
    for entry in results:
        if entry["error"]:
            print(f"FAILED {entry['name']}: {entry['error']}", file=sys.stderr)
            continue
        target = OUT / f"{entry['name']}.svg"
        target.write_text(sized(entry["svg"]))
        print(f"wrote {target.relative_to(ROOT)} ({len(entry['svg']) / 1024:.1f} KB)")

    (OUT / "manifest.json").write_text(json.dumps(
        {"theme": args.theme, "source": args.readme,
         "diagrams": [entry["name"] for entry in results if not entry["error"]],
         "note": ("rendered from the mermaid-source fences in the README by "
                  "tools/render_diagrams.py; committed so the picture does not depend on a "
                  "renderer this repository does not control")},
        indent=2) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
