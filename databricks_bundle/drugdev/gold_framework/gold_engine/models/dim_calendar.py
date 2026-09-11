# src/gold/models/dim_calendar.py
from datetime import date as _date
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class DimCalendarModel(GoldModel):
    """Date spine dimension from 2015-01-01 to 2030-12-31."""
    name         = "dim_calendar"
    write_mode   = "overwrite"
    dependencies = []
    tags         = {"source": "computed"}
    class_path   = "gold.models.dim_calendar.DimCalendarModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        START  = _date(2015,  1,  1)
        END    = _date(2030, 12, 31)
        n_days = (END - START).days + 1

        start_col = F.to_date(F.lit(START.isoformat()))

        return (
            spark.range(0, n_days)
                .withColumn("d", F.date_add(start_col, F.col("id").cast("int")))
                .select(
                    F.col("d").cast("timestamp").alias("date_actual"),
                    F.col("d").cast("date").alias("date"),
                    F.year(F.col("d")).alias("year"),
                    F.quarter(F.col("d")).alias("quarter_num"),
                    F.month(F.col("d")).alias("month_num"),
                    F.dayofmonth(F.col("d")).alias("day_num"),
                    (F.dayofweek(F.col("d")) - 1).alias("dow_num"),
                    F.date_format(F.col("d"), "EEEE").alias("day"),
                    F.when(F.dayofweek(F.col("d")).isin(1, 7), True)
                     .otherwise(False).alias("is_weekend"),
                    F.last_day(F.col("d").cast("date")).alias("month_end_date"),
                )
        )
