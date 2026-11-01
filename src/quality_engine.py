"""Configurable data-quality pipeline for order records."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import logging
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

LOG = logging.getLogger("quality_engine")


@contextmanager
def db_connection(db_path: str | Path):
    """Open a SQLite connection that commits/rolls back and always closes.

    sqlite3.Connection used as a context manager handles transactions but does
    not close the connection. Explicit closure is important on Windows, where
    an open connection prevents temporary benchmark directories from deleting
    their database files.
    """
    conn = sqlite3.connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat(timespec="seconds")


def load_config(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a YAML mapping.")
    return config


def read_csv_safely(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file does not exist: {path}")
    # Keep values as strings initially so malformed values can be quarantined, not silently coerced.
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def init_db(db_path: str | Path) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with db_connection(db_path) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS dq_runs (
            run_id TEXT PRIMARY KEY,
            dataset_name TEXT NOT NULL,
            source_file TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL,
            input_count INTEGER NOT NULL DEFAULT 0,
            accepted_count INTEGER NOT NULL DEFAULT 0,
            rejected_count INTEGER NOT NULL DEFAULT 0,
            duration_seconds REAL,
            failure_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS dq_rule_results (
            result_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            dataset_name TEXT NOT NULL,
            rule_name TEXT NOT NULL,
            severity TEXT NOT NULL,
            checked_count INTEGER NOT NULL,
            failed_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            details TEXT,
            FOREIGN KEY(run_id) REFERENCES dq_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS orders_target (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_timestamp TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            order_total REAL NOT NULL,
            source_file TEXT NOT NULL,
            last_run_id TEXT NOT NULL,
            loaded_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dq_rejects (
            reject_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            dataset_name TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            source_record TEXT NOT NULL,
            failed_rules TEXT NOT NULL,
            rejected_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES dq_runs(run_id)
        );
        """)


def _record_rule(results: list[dict[str, Any]], dataset: str, name: str,
                 severity: str, checked: int, failed_rows: list[int],
                 details: str = "") -> None:
    results.append({
        "dataset_name": dataset, "rule_name": name, "severity": severity,
        "checked_count": int(checked), "failed_count": len(failed_rows),
        "status": "PASS" if not failed_rows else "FAIL",
        "failed_rows": sorted(set(failed_rows)), "details": details
    })


def validate_orders(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    config: dict[str, Any],
    now: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    """Validate and transform records. Source row numbers are 2-based CSV line numbers."""
    dataset = config.get("dataset_name", "orders")
    source = config.get("source_path", "unknown")
    frame = orders.copy().reset_index(drop=True)
    frame["_source_row_number"] = range(2, len(frame) + 2)
    frame["_source_file"] = str(source)
    frame["_source_record"] = [
        json.dumps({k: str(v) for k, v in row.items()}, sort_keys=True)
        for row in orders.to_dict(orient="records")
    ]
    failures: dict[int, list[str]] = {int(i): [] for i in frame.index}
    rule_results: list[dict[str, Any]] = []
    n = len(frame)

    required = list(config.get("required_columns", []))
    missing_cols = [col for col in required if col not in frame.columns]
    unexpected = [c for c in orders.columns if c not in required]
    if missing_cols or (unexpected and not config.get("allow_unexpected_columns", False)):
        # Fail fast on schema drift rather than silently ignoring or misrouting columns.
        problems = []
        if missing_cols:
            problems.append(f"missing required columns {missing_cols}")
        if unexpected and not config.get("allow_unexpected_columns", False):
            problems.append(f"unexpected columns {unexpected}")
        raise ValueError("Unexpected schema: " + "; ".join(problems))

    _record_rule(rule_results, dataset, "schema_required_columns", "critical", n, [],
                 f"Required columns present; unexpected columns: {unexpected or 'none'}")

    # Completeness: each required field must contain a non-empty value.
    for col in required:
        bad = [int(i) for i, value in frame[col].items() if str(value).strip() == ""]
        for i in bad:
            failures[i].append(f"completeness_{col}")
        _record_rule(rule_results, dataset, f"completeness_{col}", "high", n, bad,
                     f"{col} must not be null or empty")

    # Parse and type-convert deterministically; malformed values are rejected.
    parsed_ts = pd.to_datetime(frame["order_timestamp"], format="ISO8601", errors="coerce", utc=True)
    bad_ts = [int(i) for i, ts in parsed_ts.items() if pd.isna(ts)]
    for i in bad_ts:
        failures[i].append("valid_order_timestamp")
    _record_rule(rule_results, dataset, "valid_order_timestamp", "high", n, bad_ts,
                 "order_timestamp must be a valid ISO-compatible timestamp")

    quantity_num = pd.to_numeric(frame["quantity"], errors="coerce")
    bad_quantity_type = [int(i) for i, val in quantity_num.items()
                         if pd.isna(val) or float(val) % 1 != 0]
    for i in bad_quantity_type:
        failures[i].append("valid_integer_quantity")
    _record_rule(rule_results, dataset, "valid_integer_quantity", "high", n, bad_quantity_type,
                 "quantity must be an integer")

    price_num = pd.to_numeric(frame["unit_price"], errors="coerce")
    bad_price_type = [int(i) for i, val in price_num.items() if pd.isna(val)]
    for i in bad_price_type:
        failures[i].append("valid_numeric_unit_price")
    _record_rule(rule_results, dataset, "valid_numeric_unit_price", "high", n, bad_price_type,
                 "unit_price must be numeric")

    # Uniqueness: every duplicate occurrence after the first is rejected.
    pk = config.get("primary_key", "order_id")
    dup_mask = frame[pk].duplicated(keep=False) | frame[pk].eq("")
    duplicate_rows = [int(i) for i in frame.index if bool(dup_mask.iloc[i])]
    for i in duplicate_rows:
        failures[i].append(f"uniqueness_{pk}")
    _record_rule(rule_results, dataset, f"uniqueness_{pk}", "critical", n, duplicate_rows,
                 f"{pk} must be unique; duplicate key occurrences are quarantined")

    # Referential integrity.
    for fk in config.get("foreign_keys", []):
        col, ref_col = fk["column"], fk["reference_column"]
        known = set(customers[ref_col].astype(str)) if ref_col in customers.columns else set()
        bad = [int(i) for i, value in frame[col].items()
               if str(value).strip() and str(value) not in known]
        for i in bad:
            failures[i].append(f"referential_integrity_{col}")
        _record_rule(rule_results, dataset, f"referential_integrity_{col}", "critical", n, bad,
                     f"{col} must exist in {fk['reference_dataset']}.{ref_col}")

    # Range checks.
    for rule in config.get("range_rules", []):
        col = rule["column"]
        numeric = quantity_num if col == "quantity" else price_num if col == "unit_price" else pd.to_numeric(frame[col], errors="coerce")
        bad = [int(i) for i, val in numeric.items()
               if pd.isna(val) or (rule.get("min") is not None and val < rule["min"])
               or (rule.get("max") is not None and val > rule["max"])]
        for i in bad:
            failures[i].append(f"range_{col}")
        _record_rule(rule_results, dataset, f"range_{col}", "high", n, bad,
                     f"{col} must be between {rule.get('min')} and {rule.get('max')}")

    # Freshness: timestamps must parse and be no older than configured maximum age.
    now = now or utc_now()
    max_age = float(config.get("freshness_max_age_hours", 168))
    freshness_bad = []
    for i, ts in parsed_ts.items():
        if pd.notna(ts) and (now - ts.to_pydatetime()).total_seconds() > max_age * 3600:
            freshness_bad.append(int(i))
            failures[int(i)].append("freshness_order_timestamp")
    _record_rule(rule_results, dataset, "freshness_order_timestamp", "medium", n, freshness_bad,
                 f"Timestamp must be no older than {max_age} hours")

    # Transform after parsing; custom business rules operate on derived order_total.
    frame["_parsed_timestamp"] = parsed_ts
    frame["_quantity_num"] = quantity_num
    frame["_unit_price_num"] = price_num
    frame["_order_total"] = quantity_num * price_num
    for rule in config.get("custom_rules", []):
        # Deliberately allow only the supported expression in this starter project.
        if rule.get("expression") == "order_total > 0":
            bad = [int(i) for i, val in frame["_order_total"].items() if pd.isna(val) or val <= 0]
        else:
            raise ValueError(f"Unsupported custom rule expression: {rule.get('expression')}")
        for i in bad:
            failures[i].append(rule["name"])
        _record_rule(rule_results, dataset, rule["name"], rule.get("severity", "medium"),
                     n, bad, rule.get("description", ""))

    rejected_indices = {i for i, reasons in failures.items() if reasons}
    accepted_indices = [i for i in frame.index if i not in rejected_indices]
    accepted = pd.DataFrame({
        "order_id": frame.loc[accepted_indices, "order_id"].astype(str),
        "customer_id": frame.loc[accepted_indices, "customer_id"].astype(str),
        "order_timestamp": frame.loc[accepted_indices, "_parsed_timestamp"].map(
            lambda x: x.isoformat().replace("+00:00", "Z")),
        "quantity": frame.loc[accepted_indices, "_quantity_num"].astype(int),
        "unit_price": frame.loc[accepted_indices, "_unit_price_num"].astype(float),
        "order_total": frame.loc[accepted_indices, "_order_total"].astype(float),
        "source_file": frame.loc[accepted_indices, "_source_file"].astype(str),
    }).reset_index(drop=True)

    reject_rows = []
    for i in sorted(rejected_indices):
        reject_rows.append({
            "source_row_number": int(frame.loc[i, "_source_row_number"]),
            "source_file": str(frame.loc[i, "_source_file"]),
            "source_record": str(frame.loc[i, "_source_record"]),
            "failed_rules": sorted(failures[i]),
        })
    rejects = pd.DataFrame(reject_rows, columns=[
        "source_row_number", "source_file", "source_record", "failed_rules"
    ])
    return accepted, rejects, rule_results


def summarize_rule_metrics(rules: list[dict[str, Any]]) -> dict[str, int]:
    """Return overlapping rule-level failure counts by operational category.

    These counts are intentionally not expected to sum to rejected records: a row
    can fail multiple rules.
    """
    metrics = {
        "duplicate_key_failures": 0,
        "invalid_reference_failures": 0,
        "range_failures": 0,
        "missing_value_failures": 0,
        "malformed_timestamp_failures": 0,
        "malformed_numeric_failures": 0,
    }
    for rule in rules:
        name = str(rule.get("rule_name", ""))
        key = None
        if name.startswith("uniqueness_"):
            key = "duplicate_key_failures"
        elif name.startswith("referential_integrity_"):
            key = "invalid_reference_failures"
        elif name.startswith("range_"):
            key = "range_failures"
        elif name.startswith("completeness_"):
            key = "missing_value_failures"
        elif name == "valid_order_timestamp":
            key = "malformed_timestamp_failures"
        elif name in {"valid_integer_quantity", "valid_numeric_unit_price"}:
            key = "malformed_numeric_failures"
        if key:
            metrics[key] += int(rule.get("failed_count", 0))
    return metrics


def run_pipeline(config_path: str | Path, run_id: str | None = None) -> dict[str, Any]:
    config = load_config(config_path)
    run_id = run_id or str(uuid.uuid4())
    dataset = config.get("dataset_name", "orders")
    source_path = str(config["source_path"])
    db_path = config["database_path"]
    output_dir = Path(config.get("output_dir", "data/output"))
    output_dir.mkdir(parents=True, exist_ok=True)
    init_db(db_path)
    # Never overwrite a prior run's audit trail. Retries must use a fresh run ID.
    with db_connection(db_path) as conn:
        existing = conn.execute("SELECT status FROM dq_runs WHERE run_id = ?", (run_id,)).fetchone()
    if existing:
        raise ValueError(f"run_id '{run_id}' already exists with status {existing[0]}; use a new run ID")
    started = iso_now()
    start_clock = time.perf_counter()
    LOG.info("Starting run_id=%s dataset=%s source=%s", run_id, dataset, source_path)
    input_count = accepted_count = rejected_count = 0
    try:
        orders = read_csv_safely(source_path)
        customers = read_csv_safely(config["customers_path"])
        input_count = len(orders)
        accepted, rejects, rules = validate_orders(orders, customers, config)
        accepted_count, rejected_count = len(accepted), len(rejects)
        reconciliation_difference = input_count - accepted_count - rejected_count
        if reconciliation_difference != 0:
            raise RuntimeError(
                "Reconciliation failed: input_count - accepted_count - rejected_count "
                f"= {reconciliation_difference} (expected 0)"
            )
        ended = iso_now()
        duration = time.perf_counter() - start_clock
        with db_connection(db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("""INSERT INTO dq_runs
                (run_id,dataset_name,source_file,started_at,ended_at,status,input_count,
                 accepted_count,rejected_count,duration_seconds,failure_reason)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (run_id, dataset, source_path, started, ended, "SUCCESS", input_count,
                 accepted_count, rejected_count, duration, None))
            for rule in rules:
                conn.execute("""INSERT INTO dq_rule_results
                    (result_id,run_id,dataset_name,rule_name,severity,checked_count,
                     failed_count,status,details) VALUES (?,?,?,?,?,?,?,?,?)""",
                    (str(uuid.uuid4()), run_id, rule["dataset_name"], rule["rule_name"],
                     rule["severity"], rule["checked_count"], rule["failed_count"],
                     rule["status"], rule["details"]))
            for _, row in rejects.iterrows():
                conn.execute("""INSERT INTO dq_rejects
                    (reject_id,run_id,dataset_name,source_file,source_row_number,
                     source_record,failed_rules,rejected_at) VALUES (?,?,?,?,?,?,?,?)""",
                    (str(uuid.uuid4()), run_id, dataset, row["source_file"],
                     int(row["source_row_number"]), row["source_record"],
                     json.dumps(row["failed_rules"]), ended))
            # Idempotent target publishing: stable order_id primary key + upsert.
            for _, row in accepted.iterrows():
                conn.execute("""INSERT INTO orders_target
                    (order_id,customer_id,order_timestamp,quantity,unit_price,order_total,
                     source_file,last_run_id,loaded_at)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(order_id) DO UPDATE SET
                      customer_id=excluded.customer_id,
                      order_timestamp=excluded.order_timestamp,
                      quantity=excluded.quantity,
                      unit_price=excluded.unit_price,
                      order_total=excluded.order_total,
                      source_file=excluded.source_file,
                      last_run_id=excluded.last_run_id,
                      loaded_at=excluded.loaded_at""",
                    (row["order_id"], row["customer_id"], row["order_timestamp"],
                     int(row["quantity"]), float(row["unit_price"]), float(row["order_total"]),
                     row["source_file"], run_id, ended))
        accepted.to_csv(output_dir / f"accepted_{run_id}.csv", index=False)
        rejects.to_csv(output_dir / f"rejects_{run_id}.csv", index=False)
        report = {
            "run_id": run_id, "dataset_name": dataset, "status": "SUCCESS",
            "input_count": input_count, "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "reconciliation_difference": input_count - accepted_count - rejected_count,
            "rule_failure_metrics": summarize_rule_metrics(rules),
            "total_rule_failures": sum(int(rule["failed_count"]) for rule in rules),
            "duration_seconds": round(duration, 4),
            "rows_per_second": round(input_count / duration, 2) if duration else None,
            "accepted_output": str(output_dir / f"accepted_{run_id}.csv"),
            "reject_output": str(output_dir / f"rejects_{run_id}.csv"),
            "database_path": db_path,
        }
        (output_dir / f"report_{run_id}.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        LOG.info("Completed run_id=%s input=%s accepted=%s rejected=%s duration=%.4fs",
                 run_id, input_count, accepted_count, rejected_count, duration)
        return report
    except Exception as exc:
        ended = iso_now()
        duration = time.perf_counter() - start_clock
        with db_connection(db_path) as conn:
            conn.execute("""INSERT OR REPLACE INTO dq_runs
                (run_id,dataset_name,source_file,started_at,ended_at,status,input_count,
                 accepted_count,rejected_count,duration_seconds,failure_reason)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (run_id, dataset, source_path, started, ended, "FAILED", input_count,
                 accepted_count, rejected_count, duration, str(exc)))
        LOG.exception("Pipeline failed run_id=%s", run_id)
        raise


def generate_data(output_dir: str | Path, rows: int = 100_000, seed: int = 42) -> None:
    """Generate reproducible synthetic customers and orders with occasional bad records."""
    import random
    from datetime import timedelta
    rng = random.Random(seed)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    customers = [{"customer_id": f"C{i:06d}", "customer_name": f"Customer {i}"}
                 for i in range(1, max(101, rows // 10) + 1)]
    now = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)
    orders = []
    for i in range(1, rows + 1):
        customer_id = f"C{rng.randint(1, len(customers)):06d}"
        timestamp = now - timedelta(hours=rng.randint(0, 120))
        quantity, price = rng.randint(1, 8), round(rng.uniform(1, 500), 2)
        if i % 997 == 0:
            customer_id = "C999999"  # invalid reference
        if i % 991 == 0:
            quantity = 0  # invalid range
        if i % 983 == 0:
            customer_id = ""  # missing required value
        orders.append({
            "order_id": f"O{i:09d}", "customer_id": customer_id,
            "order_timestamp": timestamp.isoformat().replace("+00:00", "Z"),
            "quantity": str(quantity), "unit_price": str(price)
        })
    pd.DataFrame(customers).to_csv(output_dir / "customers.csv", index=False)
    pd.DataFrame(orders).to_csv(output_dir / "orders.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Configurable data quality pipeline")
    parser.add_argument("--config", default="config/rules.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--generate", type=int, default=None, help="Generate N synthetic orders")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.generate is not None:
        generate_data("data/generated", rows=args.generate, seed=args.seed)
        print(f"Generated data under data/generated (rows={args.generate}, seed={args.seed})")
        return
    print(json.dumps(run_pipeline(args.config, args.run_id), indent=2))


if __name__ == "__main__":
    main()
