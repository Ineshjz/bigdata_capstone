#!/usr/bin/env python3
"""Lab 4 smoke test helper (run on the host from lab4_student/).

Seeds vendor data, unpauses lab4_starter, triggers 2026-06-01, waits for the dashboard JSON.
Requires: docker compose up -d, image built (docker compose build airflow-init).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATE = "2026-06-01"


def _trigger_argv(dag_id: str, logical_date: str) -> list[str]:
    """CLI flags for manual trigger (image pins Airflow 2.10.x)."""
    exec_ts = f"{logical_date}T00:00:00+00:00"
    return [
        "airflow",
        "dags",
        "trigger",
        dag_id,
        "-e",
        exec_ts,
    ]


def run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(cmd))
    return subprocess.run(cmd, cwd=ROOT, check=check, text=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Lab 4 smoke test (vendor + lab4_starter)")
    parser.add_argument("--date", default=DEFAULT_DATE, help="Logical date YYYY-MM-DD")
    parser.add_argument("--seed-only", action="store_true", help="Only run vendor_drop, do not trigger DAG")
    parser.add_argument("--wait-seconds", type=int, default=600, help="Max wait for dashboard JSON")
    args = parser.parse_args()

    py = sys.executable
    run([py, "scripts/vendor_drop.py", "--seed-pack", "--volume", "small"])
    run([py, "scripts/vendor_drop.py", "--reference"])

    if args.seed_only:
        print(f"Data ready for {args.date}. Trigger lab4_starter in the UI or re-run without --seed-only.")
        return 0

    run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "dags",
            "unpause",
            "lab4_starter",
        ]
    )
    run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "airflow-scheduler",
            *_trigger_argv("lab4_starter", args.date),
        ]
    )

    report = ROOT / "data" / "reports" / f"dashboard_{args.date}.json"
    print(f"Waiting for {report} (max {args.wait_seconds}s). Check http://localhost:8080 if this times out.")
    deadline = time.time() + args.wait_seconds
    while time.time() < deadline:
        if report.exists():
            text = report.read_text(encoding="utf-8")
            if "smoke_ok" not in text:
                print("Warning: report exists but status is not smoke_ok - rebuild the image?", file=sys.stderr)
            print("Smoke test OK:", report)
            return 0
        time.sleep(5)

    print(f"Timeout: {report} not found. Inspect the Airflow UI (lab4_starter, ds={args.date}).", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
