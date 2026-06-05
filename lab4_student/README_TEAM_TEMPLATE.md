# Team: <First Last> & <First Last>

**DAG id:** `team_<shortname>`  
**Git repo:** `https://github.com/...` - **also on your Moodle slides** (title or architecture)  
**Spark module:** `include/team_<shortname>_spark.py`  
**Course:** Big Data Processing - Lab 4 Capstone

---

## 1. Business problem

<Who needs the dashboard? What breaks if the pipeline fails?>

**Defense tip:** for each section below, be ready to say **what you built** and **why** (not only that it runs).

**Submit by June 9, 23:59:** push capstone to **your pair's Git repo**; upload **slides on Moodle** with the **same URL** visible on the slides (title slide). Public repo, or private with instructor read access.

---

## 2. Architecture

<!-- Diagram: incoming → raw/dt= → curated/dt= → reports -->

| Layer | Path | Tool |
|-------|------|------|
| Bronze | `data/incoming/` | `vendor_drop.py` |
| Silver | `data/raw/dt=` | DuckDB (`ingest_day`) |
| Gold | `data/curated/dt=` | **Your** `team_<shortname>_spark.py` |
| Serve | `data/reports/` | JSON dashboard |

### Airflow (5 tasks)

| task_id | Role |
|---------|------|
| `task_1` | `role_1` |
| `task_2` | `role_2` |
| `task_3` | `role_3` |
| `task_4` | `role_4` |
| `task_5` | `role_5` |

**Dependency graph:**

```
e.g. `task_1` → `task_2` → `task_3` → `task_4` → `task_5`
```

---

## 3. Spark transformations (≥3 - your code)

File: `include/team_<shortname>_spark.py`

| # | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | `description_1` |
| 2 | `transform_2` | `description_2` |
| 3 | `transform_3` | `description_3` |

---

## 4. Idempotence

<Re-run same `ds`: what gets overwritten under `raw/dt=`, `curated/dt=`, `dashboard_*.json`?>

---

## 5. Backfill

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill team_<shortname> -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

---

## 6. Failure demo

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
```

<Which task fails? What appears in the Airflow UI?>

---

## 7. Exploration tracks

| Track | Done? | Describe your implementation |
|-------|-------|----------|
| R Reliability | | |
| S Spark depth | | |
| O Orchestration | | |
| Q Data quality | | |
| P Custom | | |
| X SparkSubmit | | |

---

## 8. Demo script & backup

---

## 9. Production next steps

