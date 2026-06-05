"""
Capstone Spark module — team redouanes
read silver -> enrich -> aggregate KPIs -> write curated Parquet + dashboard JSON
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from include.paths import curated_kpis, raw_parquet, reference_targets, report_json

# ---------------------------------------------------------------------------
# Schéma explicite du Silver (évite les inférences incorrectes de Spark)
# ---------------------------------------------------------------------------
SILVER_SCHEMA = StructType(
    [
        StructField("tx_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("country", StringType(), True),
        StructField("amount_eur", DoubleType(), True),
        StructField("ts", TimestampType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Transform 1 — Lecture avec schéma explicite + filtre des lignes invalides
# ---------------------------------------------------------------------------
def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """Read silver Parquet with explicit schema; drop rows with null or zero amount."""
    path = str(raw_parquet(logical_date))
    df = spark.read.schema(SILVER_SCHEMA).parquet(path)

    # Filtre : on rejette les montants nuls ou négatifs (cas --corrupt)
    df_clean = df.filter(F.col("amount_eur") > 0)

    row_count = df_clean.count()
    if row_count == 0:
        raise RuntimeError(
            f"transform_1: no valid rows for {logical_date} — "
            "all amounts are zero or negative (corrupt day?)"
        )
    return df_clean


# ---------------------------------------------------------------------------
# Transform 2 — Enrichissement : revenue_band + jointure optionnelle référentiel
# ---------------------------------------------------------------------------
def transform_2(
    spark: SparkSession,
    df: DataFrame,
    logical_date: str,
    *,
    with_reference: bool = False,
) -> DataFrame:
    """Enrich with revenue_band column; optionally join category targets."""
    df_enriched = df.withColumn(
        "revenue_band",
        F.when(F.col("amount_eur") < 20, F.lit("small"))
        .when(F.col("amount_eur") < 100, F.lit("medium"))
        .otherwise(F.lit("large")),
    )

    if with_reference:
        ref_path = str(reference_targets())
        if Path(ref_path).exists():
            ref_df = spark.read.option("header", True).csv(ref_path)
            df_enriched = df_enriched.join(
                ref_df.select("category", "target_revenue"),
                on="category",
                how="left",
            )

    return df_enriched


# ---------------------------------------------------------------------------
# Transform 3 — Agrégation des KPIs par category et country
# ---------------------------------------------------------------------------
def transform_3(df: DataFrame) -> DataFrame:
    """Aggregate total revenue, transaction count, and average amount by category & country."""
    return (
        df.groupBy("category", "country")
        .agg(
            F.round(F.sum("amount_eur"), 2).alias("total_revenue_eur"),
            F.count("tx_id").alias("transaction_count"),
            F.round(F.avg("amount_eur"), 2).alias("avg_amount_eur"),
        )
        .orderBy("category", "country")
    )


# ---------------------------------------------------------------------------
# run_daily — Appelé depuis la task Airflow
# ---------------------------------------------------------------------------
def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """Orchestrate the 3 transforms; write Gold Parquet + dashboard JSON. Idempotent."""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(f"lab4_kpis_{logical_date}")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # --- 3 transforms enchaînées ---
        df_silver = transform_1(spark, logical_date)
        df_enriched = transform_2(
            spark, df_silver, logical_date, with_reference=with_reference
        )
        df_kpis = transform_3(df_enriched)

        # --- Écriture Gold Parquet (idempotence : on supprime d'abord) ---
        out_parquet = curated_kpis(logical_date)
        if out_parquet.parent.exists():
            shutil.rmtree(out_parquet.parent)
        out_parquet.parent.mkdir(parents=True, exist_ok=True)
        df_kpis.write.parquet(str(out_parquet))

        # --- Collecte des totaux pour le JSON ---
        totals = df_kpis.agg(
            F.round(F.sum("total_revenue_eur"), 2).alias("grand_total_eur"),
            F.sum("transaction_count").alias("total_transactions"),
        ).collect()[0]

        # --- Écriture JSON de rapport (idempotence : réécriture simple) ---
        report = {
            "logical_date": logical_date,
            "status": "ok",
            "grand_total_eur": float(totals["grand_total_eur"]),
            "total_transactions": int(totals["total_transactions"]),
            "curated_path": str(out_parquet),
        }
        out_json = report_json(logical_date)
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(report, indent=2))

        return report

    finally:
        spark.stop()
