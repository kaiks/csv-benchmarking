# CSV Benchmarking

Ruby-focused benchmark materials for comparing CSV processing approaches across
simple row-oriented tasks and one combined workload.

The benchmark compares:

- Ruby stdlib `CSV`
- `smarter_csv` 1.16.3
- `polars-df` 0.25.1
- `polars-df` with `low_memory: true`
- DuckDB 1.5.2.0

The generated CSV rows contain:

- `first_name`
- `last_name`
- `date_of_birth`
- `account_balance`

Roughly 10% of DOB values are intentionally invalid, split between empty values
and `mm-dd-yy` formatting.

## Quick Start

Install dependencies:

```bash
bundle install
```

Generate datasets:

```bash
bundle exec ruby scripts/generate_csv.rb --preset 1k
bundle exec ruby scripts/generate_csv.rb --preset 10k
bundle exec ruby scripts/generate_csv.rb --preset 1m
```

Build the benchmark Docker image:

```bash
docker build -t csv-benchmarking:latest .
```

Run a small validated benchmark:

```bash
bundle exec ruby scripts/run_benchmark.rb --no-build \
  --tasks all \
  --file data/people_1k.csv:1 \
  --file data/people_10k.csv:1 \
  --file data/people_1m.csv:1
```

Build charts and the presentation:

```bash
ruby scripts/build_charts.rb \
  --summary benchmarks/results/all_implementations_all_task_validation.summary.json

ruby scripts/build_presentation.rb \
  --summary benchmarks/results/all_implementations_all_task_validation.summary.json
```

The benchmark implementation details live in [benchmarks/README.md](benchmarks/README.md).
