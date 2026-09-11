# src/gold/models/gold_v_global_site_activation_trend.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class GoldVGlobalSiteActivationTrendModel(GoldModel):
    name         = "gold_v_global_site_activation_trend"
    write_mode   = "overwrite"
    dependencies = ["fct_participant_visit", "fct_study_site"]
    class_path   = "gold.models.gold_v_global_site_activation_trend.GoldVGlobalSiteActivationTrendModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema
        cat  = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        fpv = spark.table(f"{gold}.fct_participant_visit")
        fss = spark.table(f"{gold}.fct_study_site").select("study_site_id_sk", "study_site_status")

        return (
            fpv
            .join(fss, "study_site_id_sk", "left")
            .groupBy("study_id", "study_id_sk", "month_end_date", "study_site_status")
            .agg(
                F.countDistinct(
                    F.when(F.col("visit_type_group") == "Enrollment", F.col("participant_id_sk"))
                ).alias("actual_enroll_participants"),
                F.countDistinct(
                    F.when(F.col("visit_type_group") == "Screening", F.col("participant_id_sk"))
                ).alias("screen_failed_participants"),
                F.countDistinct("study_site_id_sk").alias("actual_sites"),
            )
            .withColumn("projected_participants",   F.lit(None).cast("long"))
            .withColumn("forecasted_participants",  F.lit(None).cast("long"))
            .withColumn("projected_sites",          F.lit(None).cast("long"))
            .withColumn("original_projected_sites", F.lit(None).cast("long"))
            .withColumnRenamed("study_site_status", "study_phase")
        )
