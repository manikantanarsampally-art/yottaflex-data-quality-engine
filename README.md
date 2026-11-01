# Configurable Data Quality Engine

A local, reproducible data engineering pipeline that ingests order CSVs, validates data quality, quarantines invalid rows, publishes accepted rows to SQLite, and records run-level and rule-level metrics.

## Features

- Config-driven required-field, schema, uniqueness, referential-integrity, range, freshness, and custom business-rule checks.
- Explicit quarantine/reject CSV and persisted reject records with source row, source file, original record, and failed rule names.
- Run observability: run ID, timestamps, status, counts, duration, throughput, and failure reason.
- Idempotent target publishing using `order_id` as the target primary key and SQLite UPSERT.
- Reproducible synthetic-data generator (fixed random seed).
- Automated tests for happy path, empty input, duplicates, nulls, malformed values, unexpected schema, stale data, domain edge cases, and reruns.

## Architecture

```text
orders.csv + customers.csv
          |
          v
      CSV ingestion
          |
          v
 schema + configurable data-quality rules
          |
          +------------------------+
          |                        |
     valid records            invalid records
          |                        |
          v                        v
 transform + derive total    quarantine/rejects
          |                        |
          v                        v
 SQLite orders_target      SQLite dq_rejects
          |
          +---- dq_runs + dq_rule_results
          |
          v
 accepted/rejected CSVs + JSON run report
```

## Requirements

- Python 3.10+
- pip

## Setup

Run these commands from the repository root:

```bash
python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows PowerShell:
# .venv\Scripts\Activate.ps1

python -m pip install -r requirements.txt
```

## Run the starter sample

```bash
python -m src.quality_engine --config config/rules.yaml
```

Each run prints a JSON report and writes accepted/rejected CSV files and a JSON report under `data/output/`. SQLite run and rule results are stored in `data/output/quality_engine.db`.

To keep a repeatable run ID for demonstrations:

```bash
python -m src.quality_engine --config config/rules.yaml --run-id demo-run-001
```

A run ID must be unique per execution because run history is retained. Reusing a run ID is not supported and will produce a primary-key conflict; use a new run ID to represent a retry. Target publishing remains idempotent by `order_id`.

## Generate data and benchmark 100,000 rows

Generate a dataset for manual inspection:

```bash
python -m src.quality_engine --generate 100000 --seed 42
```

The generated data is written to `data/generated/`. To run it manually, update `source_path` and `customers_path` in `config/rules.yaml` to point to `data/generated/orders.csv` and `data/generated/customers.csv`, then run the pipeline. Generated data contains occasional missing customer IDs, invalid reference keys, and out-of-range quantities.

For an isolated benchmark that leaves `data/input/` and `data/output/` untouched, run:

```bash
python scripts/benchmark_100k.py --rows 100000 --seed 42
```

The script prints the actual measured counts, reconciliation difference, rule-failure metrics, duration, and throughput for that run. Do not quote benchmark values until the script has been executed in the target environment; runtime varies by machine.

## Run tests

```bash
pytest -q
```

## Inspect results with SQL

```sql
-- Run-level reconciliation
SELECT run_id, input_count, accepted_count, rejected_count,
       input_count - accepted_count - rejected_count AS reconciliation_difference
FROM dq_runs;

-- Rule failures by severity
SELECT severity, SUM(failed_count) AS failed_rows_across_rules
FROM dq_rule_results
GROUP BY severity
ORDER BY severity;

-- Latest run summary
SELECT * FROM dq_runs ORDER BY started_at DESC;

-- Inspect quarantined rows and their rule reasons
SELECT source_row_number, source_file, source_record, failed_rules
FROM dq_rejects
ORDER BY rejected_at DESC;
```

Note: failed counts are rule-level counts, so the same record may be counted under multiple rules. The run-level rejected count is the number of distinct rejected records.

## Operational metrics and reconciliation

Each run report contains `input_count`, `accepted_count`, `rejected_count`, and `reconciliation_difference`. The pipeline fails if `input_count - accepted_count - rejected_count` is not zero. It also reports `total_rule_failures` and `rule_failure_metrics` (duplicate keys, invalid references, range, missing-value, malformed-timestamp, and malformed-numeric failures). Rule failures overlap by design: one row can fail several rules, so these counts can exceed `rejected_count` and must not be summed to estimate rejected rows.

## Data dictionary

### Source `orders.csv`
| Column | Type | Required | Meaning |
|---|---|---:|---|
| `order_id` | string | yes | Stable order business key |
| `customer_id` | string | yes | Reference to `customers.customer_id` |
| `order_timestamp` | ISO-8601 timestamp | yes | Time the order was recorded |
| `quantity` | integer | yes | Number of units; configured range 1–10,000 |
| `unit_price` | decimal | yes | Per-unit price; configured range 0–1,000,000 |

### Source `customers.csv`
| Column | Type | Required | Meaning |
|---|---|---:|---|
| `customer_id` | string | yes | Stable customer key |
| `customer_name` | string | yes | Display name |

### Target `orders_target`
Adds derived `order_total = quantity * unit_price`, `source_file`, `last_run_id`, and `loaded_at`.

### Operational tables
- `dq_runs`: one record per pipeline run, status, timestamps, row counts, duration, and failure reason.
- `dq_rule_results`: one record per rule per run, severity, checked count, failed count, status, and details.
- `dq_rejects`: rejected source records and all applicable failed rule names.

## Rules and assumptions

- The dataset grain is one row per order. `order_id` is the primary key.
- `customer_id` must exist in the customer reference dataset.
- Missing required columns and unexpected columns cause a clear schema error by default. Set `allow_unexpected_columns: true` in YAML only when additive columns are intentionally allowed; row-level missing values are quarantined.
- Duplicate order IDs are quarantined (including the first occurrence) to avoid arbitrarily selecting a winner.
- Freshness threshold defaults to 168 hours (7 days) and can be changed in YAML.
- The supported custom expression in this starter implementation is `order_total > 0`. This deliberately avoids executing arbitrary Python from configuration. Extend via a registry of named, reviewed functions for additional rules.
- Invalid records remain traceable in the reject table and reject CSV.
- The pipeline uses SQLite and local files; it is designed as a reviewable vertical slice, not a distributed production framework.

## Idempotency, retry, and backfill

- Accepted rows are UPSERTed by `order_id`, so rerunning a source does not create duplicate target keys.
- Each execution should use a fresh `run_id` to preserve run history.
- A failed run records the failure reason in `dq_runs` and re-raises the exception so schedulers can detect failure.
- For backfills, use the same source data with a new run ID and document the backfill window. This implementation updates existing target orders by key; it does not maintain full SCD/event history.

## Performance

The JSON report includes elapsed seconds, rows per second, reconciliation difference, total overlapping rule failures, and categorized rule-level failure counts. Run `python scripts/benchmark_100k.py --rows 100000 --seed 42` to measure performance in an isolated temporary directory. One reasonable next optimization is vectorizing row-level rule checks and batching SQLite writes in transactions; for larger distributed workloads, port the validation transforms to Spark.

## Design and trade-offs

- Pandas and SQLite keep setup simple and reproducible.
- YAML separates rule parameters from implementation.
- SQLite is durable locally and easy to inspect, but not intended for concurrent multi-node processing.
- A required-column schema change fails fast; malformed field values are quarantined.
- Duplicate keys are all rejected in the batch rather than choosing a winner.
- The custom-rule interface is intentionally constrained to prevent unsafe arbitrary expression execution.

## Known limitations / next steps

- Add explicit schema versioning and configurable allowed-column policies.
- Add a source-file checksum and persisted batch ledger for stronger replay detection.
- Expand custom rules through a named function registry.
- Add incremental watermarking and late-arriving-event policy.
- Consider Spark/Databricks for distributed processing at higher volumes.
