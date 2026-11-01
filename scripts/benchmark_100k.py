"""Run a reproducible 100k-row pipeline benchmark in an isolated temp directory.

Usage (from repository root): python scripts/benchmark_100k.py --rows 100000 --seed 42
The script prints the measured report and does not alter data/input or data/output.
"""
from __future__ import annotations

import argparse
import json
import tempfile
import sys
from pathlib import Path

# Make imports work when launched as `python scripts/benchmark_100k.py`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import yaml

from src.quality_engine import generate_data, run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark the data-quality engine")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.rows < 0:
        parser.error("--rows must be >= 0")

    with tempfile.TemporaryDirectory(prefix="dq-benchmark-") as temp:
        work = Path(temp)
        generated = work / "generated"
        generate_data(generated, rows=args.rows, seed=args.seed)
        config = yaml.safe_load(Path("config/rules.yaml").read_text(encoding="utf-8"))
        config.update({
            "source_path": str(generated / "orders.csv"),
            "customers_path": str(generated / "customers.csv"),
            "database_path": str(work / "quality_engine.db"),
            "output_dir": str(work / "output"),
        })
        config_path = work / "rules.yaml"
        config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
        report = run_pipeline(config_path, run_id=f"benchmark-{args.rows}-{args.seed}")
        report["benchmark_rows_requested"] = args.rows
        report["benchmark_seed"] = args.seed
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
