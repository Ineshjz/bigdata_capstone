"""Path helpers for Lab 4 medallion folders."""
from __future__ import annotations

from pathlib import Path

DATA_ROOT = Path("/opt/airflow/data")
INCOMING = DATA_ROOT / "incoming"
RAW = DATA_ROOT / "raw"
CURATED = DATA_ROOT / "curated"
REPORTS = DATA_ROOT / "reports"
REFERENCE = DATA_ROOT / "reference"


def incoming_csv(logical_date: str) -> Path:
    return INCOMING / f"transactions_{logical_date}.csv"


def raw_parquet(logical_date: str) -> Path:
    return RAW / f"dt={logical_date}" / "transactions.parquet"


def curated_kpis(logical_date: str) -> Path:
    return CURATED / f"dt={logical_date}" / "kpis_by_category_country.parquet"


def report_json(logical_date: str) -> Path:
    return REPORTS / f"dashboard_{logical_date}.json"


def reference_targets() -> Path:
    return REFERENCE / "category_targets.csv"
