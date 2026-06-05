"""
Lab 4 Capstone DAG — team_redouanes
Retail KPI pipeline: vendor CSV -> Silver (DuckDB) -> validate -> Gold (PySpark) -> publish
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from include.ingest import ingest_day, validate_silver
from include.paths import report_json
from include.team_redouanes_spark import run_daily


def on_failure_callback(context):
    ti = context["task_instance"]
    print(
        f"[ALERT] Task {ti.task_id} failed for ds={context['ds']} "
        f"on try {ti.try_number}. Check logs for details."
    )


DEFAULT_ARGS = {
    "owner": "team_redouanes",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="team_redouanes",
    description="Capstone retail KPI pipeline — team redouanes",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone"],
) as dag:
    # ------------------------------------------------------------------
    # Task 1 — Attente du fichier CSV
    # ------------------------------------------------------------------
    wait_csv = FileSensor(
        task_id="wait_for_vendor_csv",
        filepath="/opt/airflow/data/incoming/transactions_{{ ds }}.csv",
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
        soft_fail=True,
    )

    # ------------------------------------------------------------------
    # Groupe 1 — Quality Gates (ingest + validate)
    # ------------------------------------------------------------------
    with TaskGroup("quality_gates") as quality_gates:

        @task(retries=0)
        def validate(ds: str) -> dict:
            """Raise if Silver layer is corrupt."""
            result = validate_silver(ds, min_rows=10, min_revenue=0.01)
            if result["row_count"] > 10000:
                raise RuntimeError(
                    f"Validation failed: suspiciously high row count "
                    f"({result['row_count']}) for {ds}."
                )
            return result

        @task
        def ingest(ds: str) -> dict:
            """Read daily CSV and write idempotent Silver Parquet via DuckDB."""
            return ingest_day(ds)

        ingested = ingest()
        validated = validate()
        ingested >> validated

    # ------------------------------------------------------------------
    # Groupe 2 — Processing (Spark + publish)
    # ------------------------------------------------------------------
    with TaskGroup("processing") as processing:

        @task
        def run_spark(ds: str) -> dict:
            """Run the 3 Spark transforms; write curated Parquet and dashboard JSON."""
            return run_daily(ds)

        @task
        def publish(ds: str) -> dict:
            """Confirm the dashboard JSON was produced."""
            path = report_json(ds)
            if not path.exists():
                raise FileNotFoundError(f"Report not found: {path}")
            return {"report_path": str(path), "status": "ready"}

        sparked = run_spark()
        published = publish()
        sparked >> published

    # ------------------------------------------------------------------
    # Task 6 — Notification d'échec (déclenchée si une task upstream échoue)
    # ------------------------------------------------------------------
    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def notify_on_failure(ds: str) -> None:
        """Write a failure marker file and log an alert when any upstream task fails."""
        from pathlib import Path

        marker = Path(f"/opt/airflow/data/reports/FAILED_{ds}.txt")
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(f"Pipeline failed for ds={ds}")
        print(f"[ALERT] Pipeline failure marker written: {marker}")

    notified = notify_on_failure()

    # ------------------------------------------------------------------
    # Dépendances
    # ------------------------------------------------------------------
    wait_csv >> quality_gates >> processing
    [quality_gates, processing] >> notified
