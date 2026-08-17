#!/usr/bin/env bash
# Capture the real output of every command the README shows. Nothing here is transcribed by hand:
# reports/ is what the screenshots are taken from.
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p reports build

run() {
  local name="$1"; shift
  echo "--- $name: $*"
  "$@" > "reports/$name.stdout.txt" 2> "reports/$name.stderr.log"
  local code=$?
  echo "exit=$code" >> "reports/$name.stdout.txt"
  echo "    exit $code"
  return 0
}

# Diagnosis only: no Spark, no JVM, just the committed event logs.
run diagnose-skewed    skewdoc diagnose data/fixtures/join-naive-noaqe
run diagnose-clean     skewdoc diagnose data/fixtures/join-broadcast-noaqe
run diagnose-agg       skewdoc diagnose data/fixtures/agg-naive-noaqe
run diagnose-gate      skewdoc diagnose data/fixtures/join-naive-noaqe --fail-on-skew
run diagnose-json      skewdoc diagnose data/fixtures/join-broadcast-noaqe --json

skewdoc diagnose data/fixtures/join-naive-noaqe \
  --html reports/skew.html --markdown reports/skew.md \
  --matrix docs/experiments/join_matrix.json > reports/report.stdout.txt 2>&1
echo "reports written to reports/"
