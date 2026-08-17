"""Command line entry point.

The important property of this interface: **`diagnose` needs neither Spark nor a JVM.** It
reads a recorded event log, which is what somebody has at 9am when last night's job took four
hours. Every other command runs Spark and therefore needs the `spark` extra installed.

Exit codes are the contract with CI:

  0  no stage crossed a threshold, or a comparison completed with every strategy correct
  1  a stage is skewed (or a strategy returned a different answer from the control)
  2  usage or configuration error
  3  the event log or workload is missing or unreadable
  4  Spark or a JVM is unavailable and the command needed one
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import __version__, fixes
from .diagnose import HEALTHY, Thresholds, diagnose
from .errors import (
    ConfigError,
    EventLogError,
    SkewdocError,
    SparkMissingError,
    WorkloadError,
)
from .eventlog import parse
from .logging_setup import configure, get_logger
from .report import read_json, render_html, render_markdown
from .workloads import WORKLOADS, WorkloadSpec

log = get_logger("skewdoc.cli")

EXIT_OK = 0
EXIT_SKEWED = 1
EXIT_USAGE = 2
EXIT_MISSING = 3
EXIT_NO_SPARK = 4


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="skewdoc",
        description="Diagnose Spark data skew from an event log, then prove the fix.",
    )
    parser.add_argument("--version", action="version", version=f"skewdoc {__version__}")
    parser.add_argument("--log-level", default="ERROR")
    parser.add_argument("--log-format", default="text", choices=("text", "json"))
    sub = parser.add_subparsers(dest="command", required=True)

    p_diag = sub.add_parser(
        "diagnose",
        help="read a Spark event log and say whether, and how, a stage is skewed (no Spark "
             "required)",
    )
    p_diag.add_argument("event_log")
    p_diag.add_argument("--json", action="store_true")
    p_diag.add_argument("--markdown", default=None)
    p_diag.add_argument("--html", default=None)
    p_diag.add_argument("--matrix", default=None,
                        help="a comparison JSON to include in the html report")
    p_diag.add_argument("--time-ratio", type=float, default=None)
    p_diag.add_argument("--bytes-ratio", type=float, default=None)
    p_diag.add_argument("--min-tasks", type=int, default=None)
    p_diag.add_argument("--fail-on-skew", action="store_true",
                        help="exit 1 when a stage is skewed, for use as a CI gate")

    p_run = sub.add_parser("run", help="run one workload under one strategy and diagnose it")
    p_run.add_argument("--workload", default="join_skew", choices=WORKLOADS)
    p_run.add_argument("--strategy", default="naive")
    p_run.add_argument("--aqe", action="store_true", help="enable adaptive query execution")
    p_run.add_argument("--salt", type=int, default=16)
    p_run.add_argument("--fact-rows", type=int, default=2_000_000)
    p_run.add_argument("--dim-rows", type=int, default=20_000)
    p_run.add_argument("--hot-keys", type=int, default=3)
    p_run.add_argument("--hot-share", type=float, default=0.72)
    p_run.add_argument("--shuffle-partitions", type=int, default=64)
    p_run.add_argument("--repeats", type=int, default=2)
    p_run.add_argument("--master", default="local[4]")
    p_run.add_argument("--json", action="store_true")

    p_cmp = sub.add_parser("compare", help="run every strategy for a workload and rank them")
    p_cmp.add_argument("--workload", default="join_skew", choices=WORKLOADS)
    p_cmp.add_argument("--aqe", choices=("off", "on", "both"), default="both")
    p_cmp.add_argument("--salt", type=int, default=16)
    p_cmp.add_argument("--fact-rows", type=int, default=2_000_000)
    p_cmp.add_argument("--dim-rows", type=int, default=20_000)
    p_cmp.add_argument("--hot-keys", type=int, default=3)
    p_cmp.add_argument("--hot-share", type=float, default=0.72)
    p_cmp.add_argument("--shuffle-partitions", type=int, default=64)
    p_cmp.add_argument("--repeats", type=int, default=2)
    p_cmp.add_argument("--master", default="local[4]")
    p_cmp.add_argument("--out", default=None)

    p_keys = sub.add_parser("keys", help="show the workload's key distribution, measured")
    p_keys.add_argument("--workload", default="join_skew", choices=WORKLOADS)
    p_keys.add_argument("--fact-rows", type=int, default=2_000_000)
    p_keys.add_argument("--dim-rows", type=int, default=20_000)
    p_keys.add_argument("--hot-keys", type=int, default=3)
    p_keys.add_argument("--hot-share", type=float, default=0.72)
    p_keys.add_argument("--top", type=int, default=10)
    p_keys.add_argument("--json", action="store_true")
    return parser


def _thresholds(args) -> Thresholds:
    thresholds = Thresholds()
    if getattr(args, "time_ratio", None) is not None:
        thresholds.time_ratio = args.time_ratio
    if getattr(args, "bytes_ratio", None) is not None:
        thresholds.bytes_ratio = args.bytes_ratio
    if getattr(args, "min_tasks", None) is not None:
        thresholds.min_tasks = args.min_tasks
    return thresholds


def _spec(args) -> WorkloadSpec:
    return WorkloadSpec(
        name=args.workload,
        fact_rows=args.fact_rows,
        dim_rows=args.dim_rows,
        hot_keys=args.hot_keys,
        hot_share=args.hot_share,
    )


def _strategy(workload: str, name: str, salt: int, aqe: bool) -> fixes.StrategyConf:
    if workload == "join_skew":
        hot = tuple(range(3)) if name == fixes.ISOLATED else ()
        base = fixes.for_join(name, salt=salt, hot_keys=hot)
    else:
        base = fixes.for_agg(name, salt=salt)
    return fixes.with_aqe(base, aqe)


def _print_diagnosis(diagnosis) -> None:
    print(f"{'stage':>7} {'tasks':>6} {'verdict':<22} {'slowest':>9} {'median':>9} "
          f"{'t-ratio':>8} {'b-ratio':>8} {'read':<13} gini")
    for stage in sorted(diagnosis.stages, key=lambda s: (s.stage_id, s.attempt)):
        print(f"{stage.stage_id:>5}.{stage.attempt} {stage.tasks:>6} {stage.verdict:<22} "
              f"{stage.time.maximum:>8.0f}m {stage.time.median:>8.0f}m "
              f"{stage.time.ratio_max_median:>8} {stage.read_bytes.ratio_max_median:>8} "
              f"{stage.read_source:<13} {stage.time.gini}")
    worst = diagnosis.worst
    print()
    if worst is None:
        print("no stage crossed a threshold: nothing here needs a skew fix")
        return
    print(f"worst: {worst.headline}")
    print(f"why:   {worst.reason}")
    for index, item in enumerate(worst.recommendations, start=1):
        print(f"  {index}. {item}")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure(args.log_level, args.log_format)

    try:
        if args.command == "diagnose":
            application = parse(args.event_log)
            diagnosis = diagnose(application, _thresholds(args))
            if args.json:
                print(json.dumps(diagnosis.as_dict(), indent=2))
            else:
                _print_diagnosis(diagnosis)
            generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            if args.markdown:
                target = Path(args.markdown)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(render_markdown(diagnosis))
                print(f"wrote {target}")
            if args.html:
                target = Path(args.html)
                target.parent.mkdir(parents=True, exist_ok=True)
                chart = ""
                try:
                    from .charts import task_time_svg

                    chart = task_time_svg(diagnosis)
                except ImportError:
                    chart = ""
                target.write_text(render_html(
                    diagnosis, generated_at, read_json(args.matrix) if args.matrix else None,
                    version=__version__, chart_svg=chart,
                ))
                print(f"wrote {target}")
            if args.fail_on_skew and diagnosis.verdict != HEALTHY:
                return EXIT_SKEWED
            return EXIT_OK

        if args.command == "run":
            from .experiment import run as run_cell

            result, _ = run_cell(
                _spec(args),
                _strategy(args.workload, args.strategy, args.salt, args.aqe),
                shuffle_partitions=args.shuffle_partitions, repeats=args.repeats,
                master=args.master,
            )
            if args.json:
                print(json.dumps(result.as_dict(), indent=2))
            else:
                print(f"{result.workload} / {result.strategy_summary}")
                samples = [round(v) for v in result.wall_ms_samples]
                print(f"  median of {len(samples)} runs: {result.wall_ms:.0f} ms "
                      f"(warmup {result.warmup_ms:.0f} ms, samples {samples})")
                print(f"  rows {result.rows:,}  spark {result.spark_version}  "
                      f"event log {result.event_log}")
                print(f"  worst stage: {result.worst_stage_verdict}, "
                      f"time ratio {result.worst_stage_time_ratio}x, "
                      f"bytes ratio {result.worst_stage_bytes_ratio}x")
            return EXIT_SKEWED if result.worst_stage_verdict != HEALTHY else EXIT_OK

        if args.command == "compare":
            from .experiment import run_matrix

            names = (
                [fixes.NAIVE, fixes.SALTED, fixes.BROADCAST, fixes.ISOLATED]
                if args.workload == "join_skew" else [fixes.NAIVE, fixes.SALTED]
            )
            flags = {"off": [False], "on": [True], "both": [False, True]}[args.aqe]
            strategies = [
                _strategy(args.workload, name, args.salt, aqe)
                for aqe in flags for name in names
            ]
            payload = run_matrix(
                _spec(args), strategies, shuffle_partitions=args.shuffle_partitions,
                repeats=args.repeats, master=args.master,
            )
            if args.out:
                target = Path(args.out)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(json.dumps(payload, indent=2) + "\n")
            print(f"{'strategy':<12} {'aqe':<5} {'median':>9} {'speedup':>8} "
                  f"{'verdict':<22} {'t-ratio':>8} answer")
            for record in payload["runs"]:
                aqe_on = record["diagnosis"]["aqe_enabled"]
                print(f"{record['strategy']:<12} {'on' if aqe_on else 'off':<5} "
                      f"{record['wall_ms']:>8.0f}m {record['speedup_vs_control']:>8} "
                      f"{record['worst_stage_verdict']:<22} "
                      f"{record['worst_stage_time_ratio']:>8} "
                      f"{'same' if record['correct'] else 'DIFFERENT'}")
            if args.out:
                print(f"\nwrote {args.out}")
            return EXIT_OK if payload["all_correct"] else EXIT_SKEWED

        if args.command == "keys":
            from .session import spark_session
            from .workloads import key_distribution

            spec = _spec(args)
            with spark_session("skewdoc-keys", "build/eventlogs/keys",
                               shuffle_partitions=32, aqe=True) as spark:
                distribution = key_distribution(spark, spec, top=args.top)
            if args.json:
                print(json.dumps(distribution, indent=2))
            else:
                print(f"{'key':>10} {'rows':>12} {'share':>8}")
                for row in distribution:
                    print(f"{row['key']:>10} {row['rows']:>12,} {row['share']:>7.2%}")
            return EXIT_OK

    except SparkMissingError as exc:
        log.error("spark unavailable", extra={"error": str(exc)})
        return EXIT_NO_SPARK
    except (ConfigError, WorkloadError) as exc:
        log.error("input rejected", extra={"error": str(exc)})
        return EXIT_USAGE
    except (EventLogError, FileNotFoundError) as exc:
        log.error("missing or unreadable input", extra={"error": str(exc)})
        return EXIT_MISSING
    except SkewdocError as exc:
        log.error("run failed", extra={"error": str(exc)})
        return EXIT_SKEWED

    return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
