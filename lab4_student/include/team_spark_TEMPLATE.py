"""
Copy to include/team_<yourname>_spark.py and implement three Spark transformations.

Spec: read silver with schema → enrich → aggregate → write curated Parquet + dashboard JSON.
The smoke-test Spark code is baked into the Docker image (not shipped in the student kit).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """Transformation 1 - e.g. read silver with explicit schema + filter."""
    raise NotImplementedError("Implement transformation 1")


def transform_2(spark: SparkSession, df: DataFrame, logical_date: str) -> DataFrame:
    """Transformation 2 - e.g. enrich, join reference data, derive columns."""
    raise NotImplementedError("Implement transformation 2")


def transform_3(df: DataFrame) -> DataFrame:
    """Transformation 3 - e.g. aggregate KPIs by category and country."""
    raise NotImplementedError("Implement transformation 3")


def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """Called from your Airflow @task. Wire transform_1 → transform_2 → transform_3, then write outputs."""
    raise NotImplementedError("Implement run_daily")
