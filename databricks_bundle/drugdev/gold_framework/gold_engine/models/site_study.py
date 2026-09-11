# src/gold/models/site_study.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class SiteStudyModel(GoldModel):
    name         = "site_study"
    write_mode   = "overwrite"
    dependencies = ["dim_study", "dim_study_site"]
    class_path   = "gold.models.site_study.SiteStudyModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        ds = spark.table(f"{gold}.dim_study")
        dss = spark.table(f"{gold}.dim_study_site")

        study_status = (
            ds.select(
                F.col("study_id"),
                F.when(F.col("study_state").isin("Open", "Open ", "Planning", "Active"), F.lit("Active"))
                .when(F.col("study_state").isin("Closing", "Completed", "Terminated"), F.lit("Completed"))
                .otherwise(F.lit(None).cast("string")).alias("study_status"),
            )
        )

        site_info = (
            dss.alias("dss")
            .join(study_status.alias("ss"), F.col("dss.study_id") == F.col("ss.study_id"), "left")
            .filter(
                F.col("dss.study_site_status").isin(
                    "Enrollment Open",
                    "Enrollment on-Hold",
                    "Site Closed",
                    "Active Enrolling",
                    "Enrollment Closed",
                    "Approved",
                    "Submitted",
                    "on Hold",
                    "On Hold",
                    "Close Out Ready",
                    "selected",
                    "Selected",
                    "Activated",
                    "Closed",
                    "initiated",
                    "Initiated",
                    "Qualified",
                    "Enrollment On-Hold",
                )
            )
            .filter(F.col("dss.site_type") == F.lit("Main"))
            .select(
                F.col("dss.study_site_id_sk"),
                F.col("dss.study_id_sk"),
                F.col("dss.study_country_id"),
                F.col("dss.site_number"),
                F.col("dss.site_name").alias("site_name"),
                F.col("ss.study_id"),
                F.col("ss.study_status"),
            )
        )

        return (
            site_info.alias("si")
            .groupBy(
                F.col("si.study_id_sk").alias("study_id_sk"),
                F.col("si.study_id").alias("study_id"),
                F.col("si.site_name").alias("site_name"),
            )
            .agg(
                F.countDistinct(
                    F.when(F.col("si.study_status").isin("Active", "Completed"), F.col("si.study_id"))
                ).alias("total_studies"),
                F.countDistinct(
                    F.when(F.col("si.study_status") == F.lit("Active"), F.col("si.study_id"))
                ).alias("total_active_studies"),
                F.countDistinct(
                    F.when(F.col("si.study_status") == F.lit("Completed"), F.col("si.study_id"))
                ).alias("total_completed_studies"),
            )
        )