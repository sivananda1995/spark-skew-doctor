"""Print a comparison JSON as the table the README shows. Used by the demo recording.

Exists so the video's command is something a reader would actually type, rather than a
one-line Python program with escaped quotes in it.

    python tools/show_matrix.py docs/experiments/join_matrix.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("matrix", nargs="?", default="docs/experiments/join_matrix.json")
    args = parser.parse_args()
    payload = json.loads(Path(args.matrix).read_text())

    print(f"{payload['workload']}: median of {payload['repeats']} runs after a warmup, "
          f"{payload['spec']['fact_rows']:,} rows, "
          f"{payload['spec']['hot_keys']} hot keys at {payload['spec']['hot_share']:.0%}")
    print(f"{'strategy':<12} {'aqe':<5} {'median':>8} {'speedup':>8}  {'verdict':<22} answer")
    for run in payload["runs"]:
        aqe = "on" if run["diagnosis"]["aqe_enabled"] else "off"
        print(f"{run['strategy']:<12} {aqe:<5} {run['wall_ms']:>7.0f}m "
              f"{run['speedup_vs_control']:>8}  {run['worst_stage_verdict']:<22} "
              f"{'same' if run['correct'] else 'DIFFERENT'}")


if __name__ == "__main__":
    main()
