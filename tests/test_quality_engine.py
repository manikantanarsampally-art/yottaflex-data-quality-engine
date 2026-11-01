from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import pytest
from src.quality_engine import validate_orders, load_config, run_pipeline, generate_data, summarize_rule_metrics

CONFIG = load_config("config/rules.yaml")
NOW = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)


def sample_orders():
    return pd.DataFrame([
        {"order_id":"A1","customer_id":"C001","order_timestamp":"2026-10-31T10:00:00Z","quantity":"2","unit_price":"10"},
        {"order_id":"A2","customer_id":"C999","order_timestamp":"2026-10-31T10:00:00Z","quantity":"1","unit_price":"10"},
        {"order_id":"A3","customer_id":"C001","order_timestamp":"2026-10-31T10:00:00Z","quantity":"0","unit_price":"10"},
        {"order_id":"A4","customer_id":"","order_timestamp":"2026-10-31T10:00:00Z","quantity":"1","unit_price":"10"},
    ])


def sample_customers():
    return pd.DataFrame([{"customer_id":"C001","customer_name":"Asha"}])


def test_happy_path_and_domain_edge_cases():
    accepted, rejects, results = validate_orders(sample_orders(), sample_customers(), CONFIG, now=NOW)
    assert accepted["order_id"].tolist() == ["A1"]
    assert set(rejects["source_row_number"].tolist()) == {3, 4, 5}
    assert any(r["rule_name"] == "referential_integrity_customer_id" and r["failed_count"] == 1 for r in results)
    assert any(r["rule_name"] == "range_quantity" and r["failed_count"] == 1 for r in results)


def test_empty_input():
    empty = pd.DataFrame(columns=CONFIG["required_columns"])
    accepted, rejects, results = validate_orders(empty, sample_customers(), CONFIG, now=NOW)
    assert len(accepted) == 0
    assert len(rejects) == 0
    assert all(r["checked_count"] == 0 for r in results)


def test_duplicate_key_is_rejected():
    orders = pd.DataFrame([
        {"order_id":"D1","customer_id":"C001","order_timestamp":"2026-10-31T10:00:00Z","quantity":"1","unit_price":"10"},
        {"order_id":"D1","customer_id":"C001","order_timestamp":"2026-10-31T10:00:00Z","quantity":"1","unit_price":"10"},
    ])
    accepted, rejects, results = validate_orders(orders, sample_customers(), CONFIG, now=NOW)
    assert len(accepted) == 0
    assert len(rejects) == 2


def test_malformed_timestamp_and_numeric_value_are_rejected():
    orders = pd.DataFrame([
        {"order_id":"M1","customer_id":"C001","order_timestamp":"yesterday-ish","quantity":"one","unit_price":"10"},
    ])
    accepted, rejects, results = validate_orders(orders, sample_customers(), CONFIG, now=NOW)
    assert len(accepted) == 0
    assert "valid_order_timestamp" in rejects.iloc[0]["failed_rules"]
    assert "valid_integer_quantity" in rejects.iloc[0]["failed_rules"]


def test_missing_required_column_fails_schema():
    orders = pd.DataFrame([{"order_id":"A1","customer_id":"C001"}])
    with pytest.raises(ValueError, match="Unexpected schema"):
        validate_orders(orders, sample_customers(), CONFIG, now=NOW)


def test_stale_timestamp_is_rejected():
    orders = pd.DataFrame([
        {"order_id":"OLD1","customer_id":"C001","order_timestamp":"2026-10-01T10:00:00Z","quantity":"1","unit_price":"10"},
    ])
    accepted, rejects, results = validate_orders(orders, sample_customers(), CONFIG, now=NOW)
    assert len(accepted) == 0
    assert "freshness_order_timestamp" in rejects.iloc[0]["failed_rules"]


def test_reproducible_generator(tmp_path):
    first, second = tmp_path / "first", tmp_path / "second"
    generate_data(first, rows=20, seed=7)
    generate_data(second, rows=20, seed=7)
    assert (first / "orders.csv").read_bytes() == (second / "orders.csv").read_bytes()
    assert (first / "customers.csv").read_bytes() == (second / "customers.csv").read_bytes()


def test_pipeline_rerun_is_safe(tmp_path):
    import shutil, yaml, sqlite3
    project = Path(".")
    cfg = load_config(project / "config/rules.yaml")
    # Use a temp project copy of sample inputs and database/output paths.
    source = tmp_path / "orders.csv"
    customers = tmp_path / "customers.csv"
    shutil.copy("data/input/orders.csv", source)
    shutil.copy("data/input/customers.csv", customers)
    cfg["source_path"] = str(source)
    cfg["customers_path"] = str(customers)
    cfg["database_path"] = str(tmp_path / "out.db")
    cfg["output_dir"] = str(tmp_path / "output")
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    run_pipeline(cfg_path, run_id="fixed-run-1")
    run_pipeline(cfg_path, run_id="fixed-run-2")
    with sqlite3.connect(cfg["database_path"]) as conn:
        target_count = conn.execute("SELECT COUNT(*) FROM orders_target").fetchone()[0]
        run_count = conn.execute("SELECT COUNT(*) FROM dq_runs").fetchone()[0]
    assert target_count == 1
    assert run_count == 2


def test_rule_failure_counts_can_exceed_rejected_records_but_reconcile_rows():
    orders = pd.DataFrame([
        {"order_id":"X1", "customer_id":"MISSING", "order_timestamp":"not-a-date", "quantity":"0", "unit_price":"bad"},
        {"order_id":"X1", "customer_id":"C001", "order_timestamp":"2026-10-31T10:00:00Z", "quantity":"1", "unit_price":"10"},
        {"order_id":"X3", "customer_id":"C001", "order_timestamp":"2026-10-31T10:00:00Z", "quantity":"1", "unit_price":"10"},
    ])
    accepted, rejects, rules = validate_orders(orders, sample_customers(), CONFIG, now=NOW)
    assert len(accepted) + len(rejects) == len(orders)
    assert len(accepted) == 1
    assert len(rejects) == 2
    assert sum(rule["failed_count"] for rule in rules) > len(rejects)
    metrics = summarize_rule_metrics(rules)
    assert metrics["duplicate_key_failures"] == 2
    assert metrics["invalid_reference_failures"] == 1
    assert metrics["range_failures"] == 2
    assert metrics["malformed_timestamp_failures"] == 1
    assert metrics["malformed_numeric_failures"] == 1


def test_pipeline_report_has_reconciliation_and_operational_metrics(tmp_path):
    import shutil, yaml
    cfg = load_config(Path("config/rules.yaml"))
    source, customers = tmp_path / "orders.csv", tmp_path / "customers.csv"
    shutil.copy("data/input/orders.csv", source)
    shutil.copy("data/input/customers.csv", customers)
    cfg.update({"source_path": str(source), "customers_path": str(customers),
                "database_path": str(tmp_path / "out.db"), "output_dir": str(tmp_path / "output")})
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    report = run_pipeline(cfg_path, run_id="metrics-run")
    assert report["reconciliation_difference"] == 0
    assert report["input_count"] == report["accepted_count"] + report["rejected_count"]
    assert "duplicate_key_failures" in report["rule_failure_metrics"]
    assert report["total_rule_failures"] >= report["rejected_count"]


def test_reusing_run_id_does_not_overwrite_a_successful_run(tmp_path):
    import shutil, yaml, sqlite3
    cfg = load_config(Path("config/rules.yaml"))
    source, customers = tmp_path / "orders.csv", tmp_path / "customers.csv"
    shutil.copy("data/input/orders.csv", source)
    shutil.copy("data/input/customers.csv", customers)
    cfg.update({"source_path": str(source), "customers_path": str(customers),
                "database_path": str(tmp_path / "out.db"), "output_dir": str(tmp_path / "output")})
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    first = run_pipeline(cfg_path, run_id="protected-run")
    with pytest.raises(ValueError, match="already exists"):
        run_pipeline(cfg_path, run_id="protected-run")
    with sqlite3.connect(cfg["database_path"]) as conn:
        row = conn.execute("SELECT status, input_count FROM dq_runs WHERE run_id='protected-run'").fetchone()
    assert row == ("SUCCESS", first["input_count"])


def test_empty_batch_pipeline_reconciles(tmp_path):
    import yaml, sqlite3
    source = tmp_path / "empty.csv"
    pd.DataFrame(columns=CONFIG["required_columns"]).to_csv(source, index=False)
    customers = tmp_path / "customers.csv"
    sample_customers().to_csv(customers, index=False)
    cfg = dict(CONFIG)
    cfg.update({"source_path": str(source), "customers_path": str(customers),
                "database_path": str(tmp_path / "out.db"), "output_dir": str(tmp_path / "output")})
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    report = run_pipeline(cfg_path, run_id="empty-run")
    assert report["input_count"] == report["accepted_count"] == report["rejected_count"] == 0
    assert report["reconciliation_difference"] == 0


def test_unexpected_column_fails_schema_by_default():
    orders = sample_orders().assign(unreviewed_column="surprise")
    with pytest.raises(ValueError, match="unexpected columns.*unreviewed_column"):
        validate_orders(orders, sample_customers(), CONFIG, now=NOW)
