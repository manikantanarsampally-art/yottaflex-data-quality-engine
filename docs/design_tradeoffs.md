# Design and Trade-off Note

## Main decisions
1. **Pandas + SQLite:** minimize setup and make the full solution runnable on a laptop.
2. **YAML configuration:** required columns, keys, foreign keys, ranges, freshness, and custom checks are declared separately from the pipeline.
3. **Quarantine over silent cleanup:** records that fail any rule are preserved with source row, source record, and failed rule names.
4. **Fail fast on schema drift:** missing required columns stop the run; malformed values and rule violations become row-level rejects.
5. **UPSERT by order_id:** reruns update existing target keys rather than inserting duplicates.
6. **Separate run-level and rule-level metrics:** run-level counts reconcile distinct input records, while rule failures can overlap.

## Limitations
- This is a single-process local pipeline, not a distributed orchestration platform.
- SQLite serializes writes and is not suited for a multi-writer production deployment.
- Custom rules use a deliberately small allowlist rather than evaluating arbitrary expressions.
- The target keeps the latest accepted representation of an order; complete event history/SCD Type 2 is not implemented.

## Possible improvements
- Batch inserts with `executemany` and vectorize rule evaluation.
- Add a batch ledger keyed by source checksum and source path.
- Add schema versioning and an allowed/forbidden-column policy.
- Add retry orchestration, metrics export, and a Spark implementation for large distributed datasets.
