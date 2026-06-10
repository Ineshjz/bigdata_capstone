"""Bronze → silver ingest (DuckDB). Keeps DAG files thin."""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

from include.paths import incoming_csv, raw_parquet


def ingest_day(logical_date: str, *, min_rows: int = 10) -> dict:
    """Read daily CSV and write idempotent silver Parquet for one logical date."""
    csv_path = incoming_csv(logical_date)
    pq_path = raw_parquet(logical_date)

    if not csv_path.exists():
        raise FileNotFoundError(f"Missing vendor file: {csv_path}")

    pq_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        if pq_path.exists():
            pq_path.unlink()
        con.execute(
            f"""
            COPY (SELECT * FROM read_csv('{csv_path}', header=True))
            TO '{pq_path}' (FORMAT PARQUET)
            """
        )
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{pq_path}')").fetchone()[0]
        amount_sum = con.execute(
            f"SELECT COALESCE(SUM(amount_eur), 0) FROM read_parquet('{pq_path}')"
        ).fetchone()[0]
    finally:
        con.close()

    if row_count < min_rows:
        raise RuntimeError(f"Ingest failed: only {row_count} rows for {logical_date}")

    return {
        "logical_date": logical_date,
        "raw_path": str(pq_path),
        "row_count": int(row_count),
        "amount_sum": float(amount_sum),
    }


def validate_silver(logical_date: str, *, min_rows: int = 10, min_revenue: float = 0.01) -> dict:
    """Raise if silver layer looks corrupt (required capstone task; Track Q demo with --corrupt)."""
    pq_path = raw_parquet(logical_date)
    if not os.path.exists(pq_path):
        raise FileNotFoundError(f"Silver file missing: {pq_path}")

    con = duckdb.connect()
    try:
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{pq_path}')").fetchone()[0]
        amount_sum = con.execute(
            f"SELECT COALESCE(SUM(amount_eur), 0) FROM read_parquet('{pq_path}')"
        ).fetchone()[0]
    finally:
        con.close()

    if row_count < min_rows:
        raise RuntimeError(f"Validation failed: row_count={row_count}")
    if amount_sum < min_revenue:
        raise RuntimeError(f"Validation failed: amount_sum={amount_sum} (corrupt day?)")

    return {"row_count": int(row_count), "amount_sum": float(amount_sum)}
