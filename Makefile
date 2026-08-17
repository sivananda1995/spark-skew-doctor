# Every target regenerates something this repository claims. Nothing post-processes a number.
.PHONY: help install lint test test-fast verify diagnose compare severity demo shots video \
        receipts fixtures all clean

help:
	@grep -E '^[a-z-]+:.*?##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | column -t -s "$$(printf '\t')"

install: ## install with the dev and spark extras
	pip install -e ".[dev,spark]"

lint: ## ruff
	ruff check .

test-fast: ## everything that does not need a JVM: the parser, the classifier, the reports
	python -m pytest -m "not spark" --cov=skewdoc --cov-report=term -q

test: ## the whole suite, including the tests that run real Spark jobs
	python -m pytest --cov=skewdoc --cov-report=term --cov-report=xml -q

diagnose: ## diagnose the committed event log from the naive run
	skewdoc diagnose data/fixtures/join-naive-noaqe

compare: ## run every join strategy, both AQE settings, and rank them
	skewdoc compare --workload join_skew --out docs/experiments/join_matrix.json

compare-agg: ## the same for the two aggregation workloads
	skewdoc compare --workload agg_skew --out docs/experiments/agg_matrix.json
	skewdoc compare --workload agg_wide --out docs/experiments/agg_wide_matrix.json

severity: ## sweep how skewed the data has to be before each fix pays
	python benchmark/bench_severity.py

demo: ## reproduce the scenarios the readme shows, capturing real output
	bash tools/run_demo.sh

shots: ## regenerate readme screenshots from real reports
	python tools/capture_screenshots.py

video: ## record the demo video from a real terminal session
	python tools/record_demo.py

receipts: ## re-measure every published number and check every document that quotes one
	python tools/collect_metrics.py
	python tools/check_numbers.py

fixtures: ## refresh the committed event logs from a fresh set of runs
	skewdoc run --workload join_skew --strategy naive --repeats 1
	skewdoc run --workload join_skew --strategy broadcast --repeats 1
	skewdoc run --workload agg_skew --strategy naive --repeats 1
	rm -rf data/fixtures/join-naive-noaqe data/fixtures/join-broadcast-noaqe \
		data/fixtures/agg-naive-noaqe
	cp -r build/eventlogs/join_skew-naive-noaqe data/fixtures/join-naive-noaqe
	cp -r build/eventlogs/join_skew-broadcast-noaqe data/fixtures/join-broadcast-noaqe
	cp -r build/eventlogs/agg_skew-naive-noaqe data/fixtures/agg-naive-noaqe

verify: lint test receipts ## lint, the full suite, and every number re-measured

all: compare compare-agg severity demo shots video receipts ## everything, in order

clean:
	rm -rf build reports .pytest_cache .ruff_cache .coverage coverage.xml htmlcov
