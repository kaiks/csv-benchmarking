# CSV Benchmarking

The benchmark runner executes implementation scripts in Docker and records one
JSON object per run. Each run captures wall time, peak container memory, and any
memory checkpoints emitted by the implementation.

## Running

Generate data:

```bash
bundle exec ruby scripts/generate_csv.rb --preset 1k
bundle exec ruby scripts/generate_csv.rb --preset 10k
bundle exec ruby scripts/generate_csv.rb --preset 1m
```

Run the default profile:

```bash
bundle exec ruby scripts/run_benchmark.rb
```

The default profile runs:

| File | Iterations per task |
| --- | ---: |
| `data/people_1k.csv` | 1000 |
| `data/people_10k.csv` | 100 |
| `data/people_1m.csv` | 100 |

With the five default implementations and seven tasks, that is 42,000 measured
Docker runs before any warmups.

The default task list is:

- `read`
- `count_high_balances`
- `count_duplicate_rows`
- `total_account_balance`
- `count_invalid_dob_rows`
- `invalid_dob_indexes`
- `all`

For quick checks:

```bash
bundle exec ruby scripts/run_benchmark.rb \
  --file data/people_1k.csv:1 \
  --tasks read,all
```

Use `--implementation NAME_OR_PATH` to run one implementation. Repeat the flag
to run a subset:

```bash
bundle exec ruby scripts/run_benchmark.rb \
  --implementation ruby_csv_default \
  --implementation polars \
  --file data/people_1k.csv:3
```

Default implementations are declared in `implementations/manifest.json`:

- `ruby_csv_default`
- `smarter_csv`
- `polars`
- `polars_low_memory`
- `duckdb`

Each implementation is launched as an independent process. If the manifest entry
has a `gemfile`, the runner invokes only that bundle:

```text
env BUNDLE_GEMFILE=implementations/gemfiles/polars.gemfile bundle exec ruby implementations/polars.rb ...
```

This keeps the load path and required dependencies implementation-specific. The
Docker image may have all dependencies installed, but each measured Ruby process
only activates its own bundle.

Correctness checking is enabled by default. The runner computes expected values
with `ruby_csv_default`, writes them to `*.expected.json`, and stores `correct`
on every measured record. Disable this with `--no-verify`.

Each task is measured independently. For example, `count_invalid_dob_rows` and
`invalid_dob_indexes` are separate Docker process executions, and `all` is also
separate execution rather than a summary assembled from the individual tasks.

## Implementation Contract

An implementation is a Ruby script that accepts:

```bash
ruby path/to/implementation.rb --file data/people_1m.csv --task all
```

Supported tasks:

- `read`
- `count_high_balances`
- `count_duplicate_rows`
- `total_account_balance`
- `count_invalid_dob_rows`
- `invalid_dob_indexes`
- `all`

The script must print one JSON object to stdout. The runner stores that JSON
under the `result` field for the benchmark record.

To let the runner record post-file-read memory, emit this exact line to stderr
after the file has been read and before printing the JSON result:

```text
__CSV_BENCHMARK_CHECKPOINT__ post_file_read
```

The runner records that checkpoint from Docker cgroup memory, so memory used by
native extensions and child tooling is included in the container total.

`invalid_dob_indexes` uses zero-based data row indexes. The header row is not
counted.

The default implementation materializes the full invalid-index array so its
memory behavior matches the task, but it prints a compact digest instead of
serializing millions of indexes into the benchmark log. The digest includes the
count, first ten indexes, last ten indexes, and an order-sensitive checksum.

## Charts

Build SVG charts from an existing benchmark summary:

```bash
ruby scripts/build_charts.rb \
  --summary benchmarks/results/all_implementations_all_task_validation.summary.json \
  --output-dir benchmarks/charts
```

The chart builder only reads stored results. It does not run benchmarks.

## Presentation

Build the HTML presentation from the stored summary:

```bash
ruby scripts/build_presentation.rb \
  --summary benchmarks/results/all_implementations_all_task_validation.summary.json \
  --output presentations/csv_benchmarking_talk.html \
  --repo-url https://github.com/kaiks/csv-benchmarking
```

Export it to PDF with Chromium:

```bash
chromium --headless --disable-gpu --no-sandbox \
  --allow-file-access-from-files \
  --print-to-pdf=presentations/csv_benchmarking_talk.pdf \
  --no-pdf-header-footer \
  file://$PWD/presentations/csv_benchmarking_talk.html
```
