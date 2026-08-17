"""Where does each fix start to earn its complexity? Sweep the severity and find out.

The strategy matrix answers "which fix is best on this workload". It does not answer the
question somebody actually has, which is "should *I* write the salting code". That depends on
how skewed
the data is and on whether adaptive execution is available, so both are swept here:

  * ``hot_share`` from mild to extreme, holding everything else fixed;
  * naive with adaptive execution off, which is the Spark 2.x situation and still the situation
    inside any pipeline where AQE has been disabled to stabilise plans;
  * naive with adaptive execution on, the modern default;
  * salting with adaptive execution off, the hand-written fix in the world where it is the only
    option available.

The output is the table that tells someone what to do rather than what happened.

Run: python benchmark/bench_severity.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from skewdoc import fixes  # noqa: E402
from skewdoc.experiment import run  # noqa: E402
from skewdoc.logging_setup import configure  # noqa: E402
from skewdoc.workloads import WorkloadSpec  # noqa: E402

CASES = (
    ("naive", False),
    ("naive", True),
    ("salted", False),
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shares", type=float, nargs="*", default=[0.2, 0.5, 0.8, 0.95])
    parser.add_argument("--fact-rows", type=int, default=1_500_000)
    parser.add_argument("--dim-rows", type=int, default=20_000)
    parser.add_argument("--hot-keys", type=int, default=1)
    parser.add_argument("--salt", type=int, default=16)
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    parser.add_argument("--out", default="benchmark/results/severity.json")
    args = parser.parse_args()

    configure("ERROR", "text")
    rows: list[dict] = []
    for share in args.shares:
        spec = WorkloadSpec(
            name="join_skew", fact_rows=args.fact_rows, dim_rows=args.dim_rows,
            hot_keys=args.hot_keys, hot_share=share,
        )
        control = None
        for name, aqe in CASES:
            strategy = fixes.with_aqe(fixes.for_join(name, salt=args.salt), aqe)
            result, fingerprint = run(
                spec, strategy, log_root="build/eventlogs/severity",
                shuffle_partitions=args.shuffle_partitions, control=control,
            )
            if control is None:
                control = fingerprint
                result.correct = True
            rows.append({
                "hot_share": share,
                "strategy": name,
                "aqe": aqe,
                "wall_ms": result.wall_ms,
                "verdict": result.worst_stage_verdict,
                "time_ratio": result.worst_stage_time_ratio,
                "bytes_ratio": result.worst_stage_bytes_ratio,
                "total_task_ms": result.total_task_ms,
                "correct": result.correct,
            })
            print(f"share={share:<5} {name:<7} aqe={str(aqe):<5} "
                  f"{result.wall_ms:>8.0f} ms  {result.worst_stage_verdict:<20} "
                  f"tr={result.worst_stage_time_ratio:<7} correct={result.correct}")

    # For each severity, what the two fixes bought against the do-nothing baseline.
    summary = []
    for share in args.shares:
        cell = {r["strategy"] + ("+aqe" if r["aqe"] else ""): r for r in rows
                if r["hot_share"] == share}
        baseline = cell["naive"]["wall_ms"]
        summary.append({
            "hot_share": share,
            "naive_ms": baseline,
            "naive_verdict": cell["naive"]["verdict"],
            "naive_time_ratio": cell["naive"]["time_ratio"],
            "aqe_speedup": round(baseline / cell["naive+aqe"]["wall_ms"], 2),
            "salt_speedup": round(baseline / cell["salted"]["wall_ms"], 2),
            "aqe_beats_salt": cell["naive+aqe"]["wall_ms"] < cell["salted"]["wall_ms"],
        })

    payload = {
        "fact_rows": args.fact_rows,
        "dim_rows": args.dim_rows,
        "hot_keys": args.hot_keys,
        "salt": args.salt,
        "shuffle_partitions": args.shuffle_partitions,
        "rows": rows,
        "summary": summary,
        "all_correct": all(r["correct"] for r in rows),
        "salt_ever_beats_aqe": any(not s["aqe_beats_salt"] for s in summary),
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2) + "\n")

    print(f"\n{'hot share':>10} {'naive ms':>9} {'naive verdict':<22} {'AQE':>6} {'salt':>6} "
          "who wins")
    for record in summary:
        winner = "AQE" if record["aqe_beats_salt"] else "salting"
        print(f"{record['hot_share']:>10} {record['naive_ms']:>9.0f} "
              f"{record['naive_verdict']:<22} {record['aqe_speedup']:>5}x "
              f"{record['salt_speedup']:>5}x  {winner}")
    print(f"\nsalting beat AQE at any severity: {payload['salt_ever_beats_aqe']}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
