# Team: Redouane Ndiaye & Ines [Lastname]

**DAG id:** `team_redouanes`  
**Git repo:** `https://github.com/...` — **also on your Moodle slides** (title or architecture)  
**Spark module:** `include/team_redouanes_spark.py`  
**Course:** Big Data Processing - Lab 4 Capstone

---

## 1. Business problem

A retail partner drops one CSV file per day containing store transactions. The operations team needs a daily KPI dashboard showing revenue and transaction counts by category and country. Without this pipeline, the dashboard cannot be refreshed and business decisions are made on stale data. If the pipeline receives corrupt data, the `validate` task blocks execution before any Spark computation begins, ensuring no incorrect KPIs are ever published to the dashboard.

---

## 2. Architecture

| Layer | Path | Tool |
|-------|------|------|
| Bronze | `data/incoming/` | `vendor_drop.py` |
| Silver | `data/raw/dt=<ds>/` | DuckDB (`ingest_day`) |
| Gold | `data/curated/dt=<ds>/` | `team_redouanes_spark.py` |
| Serve | `data/reports/` | JSON dashboard |

### Airflow (5 tasks)

| task_id | Role |
|---------|------|
| `wait_for_vendor_csv` | FileSensor — waits for the vendor CSV to appear in `data/incoming/` before starting the pipeline. Polls every 30s, times out after 10 min (`soft_fail=True`). |
| `ingest` | Reads the Bronze CSV and writes an idempotent Silver Parquet partition via DuckDB. Deletes the existing file before rewriting. |
| `validate` | Verifies the Silver layer is not corrupt: checks minimum row count (≥10), maximum row count (≤10,000), and strictly positive total revenue. `retries=0` — a data quality failure is deterministic. |
| `run_spark` | Calls `run_daily(ds)` from `team_redouanes_spark.py`. Runs the 3 Spark transforms and writes the Gold Parquet and the dashboard JSON. |
| `publish` | Verifies the dashboard JSON was produced by Spark. Raises a `FileNotFoundError` if the file is missing. Serves as the explicit end-of-pipeline confirmation. |

**Dependency graph:**

```
wait_for_vendor_csv → ingest → validate → run_spark → publish
```

---

## 3. Spark transformations (≥3 - your code)

File: `include/team_redouanes_spark.py`

| # | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | Reads Silver Parquet with an explicit schema (avoids type inference errors) and filters out rows where `amount_eur ≤ 0`. Raises a `RuntimeError` if no valid rows remain — this surfaces a visible failure when testing with `--corrupt`. |
| 2 | `transform_2` | Enriches each row with a `revenue_band` column ("small" < 20€, "medium" 20–100€, "large" > 100€). Optionally joins `category_targets.csv` using a broadcast join to avoid shuffling the large DataFrame. |
| 3 | `transform_3` | Aggregates KPIs by `category` and `country`: `total_revenue_eur`, `transaction_count`, and `avg_amount_eur`. This produces the Gold layer written to `data/curated/dt=<ds>/`. |

---

## 4. Idempotence

Re-running the DAG for the same `ds` produces identical results without duplicating data. At the Silver layer, `ingest_day()` deletes the existing Parquet file before rewriting it (`pq_path.unlink()`). At the Gold layer, `run_daily()` removes the entire `data/curated/dt=<ds>/` directory before writing the new Parquet (`shutil.rmtree()`), and overwrites the JSON report file unconditionally. Re-running `2026-06-01` twice yields the same dashboard JSON both times.

---

## 5. Backfill

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill team_redouanes -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

The `--reset-dagruns` flag forces re-execution even for dates already processed, which validates idempotence across the full date range. All 4 runs completed successfully with 0 failures (20 task instances total).

---

## 6. Failure demo

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
```

The `--corrupt` flag sets all `amount_eur` values to 0. The `validate` task detects that `amount_sum = 0.0`, raises a `RuntimeError`, and turns red in the Airflow UI. The `run_spark` and `publish` tasks are never reached, preventing corrupt KPIs from being written to the Gold layer. The `validate` task has `retries=0` so the failure is immediate and visible without waiting for retry delays.

---

## 7. Exploration tracks

| Track | Done? | Description |
|-------|-------|-------------|
| R Reliability | ✅ | Added `soft_fail=True` on the FileSensor (late vendor file → skipped, not failed). Added `on_failure_callback` in `DEFAULT_ARGS` to log structured alerts (task_id, ds, try_number) on any failure. `validate` has `retries=0` since data quality failures are deterministic. |
| S Spark depth | ✅ | Applied `F.broadcast()` on the reference DataFrame in `transform_2` to avoid a costly shuffle when joining against the small `category_targets.csv`. Set `spark.sql.shuffle.partitions=4` (vs. default 200) to match `local[*]` core count on a laptop and eliminate unnecessary overhead for ~200-row partitions. |
| O Orchestration | | |
| Q Data quality | ✅ | Added upper-bound row count validation (≤10,000) in `validate` to guard against accidental duplicate ingestion, in addition to the existing lower-bound and revenue checks from `validate_silver()`. |
| P Custom | | |
| X SparkSubmit | | |

---

## 8. Demo script & backup

**Happy path (~3 min):**
1. Show the Airflow UI with `team_redouanes` DAG graph — green run for `2026-06-01`.
2. Open `data/reports/dashboard_2026-06-01.json` and show the KPIs (`grand_total_eur`, `total_transactions`).
3. Show `data/curated/dt=2026-06-01/` exists with the Gold Parquet.

**Failure demo (~1 min):**
1. Run `python scripts/vendor_drop.py --date 2026-06-03 --corrupt`.
2. Trigger the DAG for `2026-06-03` — show `validate` turning red immediately.
3. Confirm `run_spark` and `publish` never execute.

**Idempotence demo (~1 min):**
1. Trigger `2026-06-01` a second time.
2. Show the output JSON is identical — no duplicates.

**Backup (if Docker fails on June 10):** the `demo_backup/` folder contains screenshots of the green graph, the red `validate` task, and the content of `dashboard_2026-06-01.json`.

---

## 9. Production next steps

In production, the `local[*]` SparkSession would be replaced by a `SparkSubmitOperator` pointing to a real cluster (YARN or Kubernetes), allowing the Airflow worker to simply monitor job status rather than executing Spark in-process. The `soft_fail=True` on the FileSensor would be complemented by an SLA miss callback to alert the on-call team when vendor files arrive late. The row count bounds in `validate` would be calibrated against historical data rather than fixed thresholds. Finally, the pipeline would be extended to support dynamic task mapping to process multiple vendor feeds in parallel.