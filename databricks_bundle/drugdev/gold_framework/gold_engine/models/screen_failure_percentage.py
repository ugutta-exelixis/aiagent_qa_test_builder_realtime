# src/gold/models/screen_failure_percentage.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class ScreenFailurePercentageModel(GoldModel):
    name         = "screen_failure_percentage"
    write_mode   = "overwrite"
    dependencies = ["dim_participant", "fct_participant_visit", "dim_study_site"]
    class_path   = "gold.models.screen_failure_percentage.ScreenFailurePercentageModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        dp = spark.table(f"{gold}.dim_participant").alias("dp")
        fpv = spark.table(f"{gold}.fct_participant_visit").alias("fpv")
        dss = spark.table(f"{gold}.dim_study_site").alias("dss")

        return (
            dp.join(
                fpv,
                (F.col("dp.study_id") == F.col("fpv.study_id"))
                & (F.col("dp.participant_id_sk") == F.col("fpv.participant_id_sk")),
                "left",
            )
            .join(
                dss,
                (F.col("fpv.study_id") == F.col("dss.study_id"))
                & (F.col("fpv.study_site_id_sk") == F.col("dss.study_site_id_sk")),
                "left",
            )
            .groupBy(
                F.col("dp.study_id").alias("study_id"),
                F.col("dss.main_site_name").alias("site_name"),
            )
            .agg(
                F.countDistinct(
                    F.when(
                        F.col("dp.participant_status_master").isin("Screen Failure", "Screen Failed"),
                        F.col("dp.participant_number"),
                    )
                ).alias("screen_failed_participants"),
                F.countDistinct(
                    F.when(
                        F.col("dp.participant_status_master").isin(
                            "Screen Failed",
                            "Screen Failure",
                            "In Follow-up",
                            "Off Study",
                            "On Treatment",
                            "In Follow Up",
                            "Randomized",
                        ),
                        F.col("dp.participant_number"),
                    )
                ).alias("total_screened"),
            )
            .select(
                "study_id",
                "site_name",
                "screen_failed_participants",
                "total_screened",
            )
        )