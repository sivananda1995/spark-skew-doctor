"""Fail if a document quotes a number that the code no longer produces.

Shared with the rag-regression-gate project, where the design is recorded in its ADR-007:
values are pinned to the phrase that claims them, because searching a long document for "24"
proves nothing, and every file that quotes a measurement is checked rather than only the README.

Reads docs/metrics.json, which tools/collect_metrics.py regenerates by running the real
pipeline, benchmarks, and test suite, then checks that every document listed in that
registry still contains each current value. A number that has moved shows up as a missing
string, which is the signal that the prose and the code have diverged.

This is deliberately a check rather than a template renderer. Templating the README would
keep it consistent and unreadable; a check keeps the prose hand-written and the numbers
honest, and it fails loudly the moment those two goals conflict.

Two things were learned the hard way and are enforced here.

**Presence is not enough.** "Is the string '11' somewhere in the README" passes on any long
document, so the first version of this check silently approved a sentence that said 10 where
the code measured 11, and another that said 159 tests where the suite had 170. A metric may
therefore declare *anchors*: phrase templates such as ``"{} queries named in the blame
table"``. The value is substituted in and the whole phrase must appear verbatim, so the
check reads "the sentence about the blame table says 11". A metric whose display string is
short enough to be ambiguous and which declares no anchor is itself a failure, because that
is exactly the shape that gave false assurance.

**Some claims are inequalities.** A metric may carry a ``floor``, in which case the documents
quote the floor and the check asserts that the measurement still clears it. Throughput is the
case that needs this: an exact rate moves between runs, so a check pinned to it fails at random
and gets ignored, while "over 40,000 events a second" is stable and still falsifiable.

**The README is not the only place numbers live.** A config comment quoted a recall figure
from before a bug fix, and a module docstring quoted a held-out score that had moved by four
points; neither was checked, so both rotted. The registry lists every file that writes a
measured value down, and all of them are searched.

Run from the repository root: python tools/check_numbers.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Below this length, a display string is common enough in ordinary prose that finding it
# somewhere in a document is not evidence of anything. "0.8762" is specific; "11" is not.
ANCHOR_REQUIRED_BELOW = 6


def load_documents(paths: list[str]) -> dict[str, str]:
    missing = [p for p in paths if not Path(p).exists()]
    if missing:
        raise SystemExit(f"checked_documents lists files that do not exist: {missing}")
    return {p: Path(p).read_text() for p in paths}


def find_anywhere(documents: dict[str, str], needle: str) -> str | None:
    for name, text in documents.items():
        if needle in text:
            return name
    return None


def check(payload: dict, documents: dict[str, str]) -> tuple[list[str], int, int]:
    """Return (problems, metrics checked, anchor phrases checked)."""
    problems: list[str] = []
    checked = phrases = 0

    for name, record in payload["metrics"].items():
        variants = [str(v) for v in record["display"]]
        anchors = record.get("anchors")
        source = record["source"]
        checked += 1

        floor = record.get("floor")
        if floor is not None and record["value"] < floor:
            # The document claims an inequality and the measurement no longer supports it. This
            # is the one case where a number can be wrong without having changed in the prose.
            problems.append(
                f"{name}: the documents claim at least {floor:,} but this build measured "
                f"{record['value']:,} (measured by: {source})"
            )

        if not anchors:
            if min(len(v) for v in variants) < ANCHOR_REQUIRED_BELOW:
                problems.append(
                    f"{name} = {record['value']!r} has no anchor and its display string is "
                    f"shorter than {ANCHOR_REQUIRED_BELOW} characters, so searching for it "
                    "would prove nothing. Add an anchor phrase to ANCHORS in "
                    "tools/collect_metrics.py."
                )
                continue
            if not any(find_anywhere(documents, v) for v in variants):
                problems.append(
                    f"{name}: no checked document contains {' or '.join(map(repr, variants))} "
                    f"(current value {record['value']!r}, measured by: {source})"
                )
            continue

        for template in anchors:
            phrases += 1
            if template.count("{}") != 1:
                problems.append(
                    f"{name}: the anchor {template!r} must contain exactly one {{}} "
                    "placeholder, so the measured value has one place to go"
                )
                continue
            wanted = [template.format(v) for v in variants]
            if any(find_anywhere(documents, phrase) for phrase in wanted):
                continue
            # Report what the document says where the phrase should have been, because
            # "expected 11" is far less useful than "this line says 10".
            skeleton = template.replace("{}", "")
            near = _nearby(documents, skeleton)
            problems.append(
                f"{name}: expected one of {', '.join(map(repr, wanted))} in a checked "
                f"document (current value {record['value']!r}, measured by: {source})"
                + (f"\n      found instead: {near}" if near else "")
            )

    return problems, checked, phrases


def _nearby(documents: dict[str, str], skeleton: str) -> str:
    """Find the line that looks like it was meant to carry the value."""
    probe = max(skeleton.split(), key=len, default="")
    if len(probe) < 4:
        return ""
    for name, text in documents.items():
        for number, line in enumerate(text.splitlines(), start=1):
            if probe in line:
                return f"{name}:{number}: {line.strip()[:150]}"
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", default="docs/metrics.json")
    parser.add_argument("--documents", nargs="*", default=None,
                        help="override the document list in the registry")
    args = parser.parse_args()

    metrics_path = Path(args.metrics)
    if not metrics_path.exists():
        print(f"{metrics_path} is missing. Run: python tools/collect_metrics.py")
        return 2
    payload = json.loads(metrics_path.read_text())
    paths = args.documents or payload.get("checked_documents") or ["README.md"]
    documents = load_documents(paths)

    problems, checked, phrases = check(payload, documents)
    print(
        f"checked {checked} measured values from {metrics_path} against "
        f"{len(documents)} document(s): {', '.join(documents)}"
    )
    print(f"{phrases} of them are pinned to an exact phrase rather than searched for loose")

    if not problems:
        print("every number in these documents matches a value this build measured")
        return 0

    print(f"\n{len(problems)} problem(s):\n")
    for problem in problems:
        print(f"  - {problem}")
    print(
        "\nEither the prose needs updating, or the change that moved these numbers needs "
        "explaining."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
