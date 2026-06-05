"""
Lab 4 STARTER DAG - smoke test only. Do not submit as your final project.

Spark task only checks PySpark/Java (spark.range(1).count()) via lab4_starter - not a real KPI job.
Copy to dags/team_<yourname>.py and implement your capstone (see lab4.tex).

Quick check (host): python scripts/smoke_test.py
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

from include.ingest import ingest_day
from include.paths import report_json
from lab4_starter.spark_kpis import run_daily

DEFAULT_ARGS = {
    "owner": "lab4",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="lab4_starter",
    description="Smoke pipeline: vendor → ingest → PySpark config check → publish",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "starter"],
) as dag:

    wait_csv = FileSensor(
        task_id="wait_for_vendor_csv",
        filepath="/opt/airflow/data/incoming/transactions_{{ ds }}.csv",
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
    )

    @task
    def ingest(ds: str) -> dict:
        return ingest_day(ds)

    @task
    def verify_spark(ds: str) -> dict:
        return run_daily(ds)

    @task
    def publish(ds: str) -> dict:
        path = report_json(ds)
        if not path.exists():
            raise FileNotFoundError(f"Report not found: {path}")
        return {"report_path": str(path), "status": "ready"}

    ingested = ingest()
    spark_ok = verify_spark()
    published = publish()

    wait_csv >> ingested >> spark_ok >> published
