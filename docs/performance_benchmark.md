# Synthetic 100,000-row benchmark

Executed with the fixed connection-cleanup implementation using:

```bash
python scripts/benchmark_100k.py --rows 100000 --seed 42
```

Measured result from one run in the packaging environment (your computer may differ):

| Metric | Measured result |
|---|---:|
| Input records | 100,000 |
| Accepted records | 99,699 |
| Rejected records | 301 |
| Reconciliation difference | 0 |
| Duplicate-key failures | 0 |
| Invalid-reference failures | 100 |
| Range failures | 100 |
| Missing-value failures | 101 |
| Malformed timestamp failures | 0 |
| Malformed numeric failures | 0 |
| Total overlapping rule failures | 401 |
| Duration | 1.6025 seconds |
| Throughput | 62,403.53 rows/second |

These are measured results from this particular seeded run, not a guarantee of performance on another machine. Rule-level failure counts overlap: one row can fail more than one rule, so total rule failures can exceed rejected rows. Reconciliation is based on distinct rows: 100,000 = 99,699 + 301.

The benchmark writes its generated data, database, and reports to a temporary directory. SQLite connections are explicitly closed, so Windows can clean up the temporary directory after the run. The benchmark does not overwrite sample input files or persist benchmark artifacts in the repository.
