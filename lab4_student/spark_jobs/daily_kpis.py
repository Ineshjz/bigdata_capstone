#!/usr/bin/env python3
"""
Standalone Spark job for Lab 4 (CLI / Track X spark-submit).

Usage (inside airflow-worker container):
  spark-submit /opt/airflow/spark_jobs/daily_kpis.py --date 2026-06-01
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow imports from /opt/airflow
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab4_starter.spark_kpis import run_daily_cli  # noqa: E402

if __name__ == "__main__":
    run_daily_cli()
