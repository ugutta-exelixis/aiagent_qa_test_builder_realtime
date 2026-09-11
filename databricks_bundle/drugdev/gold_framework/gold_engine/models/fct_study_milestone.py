# src/gold/models/fct_study_milestone.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class FctStudyMilestoneModel(GoldModel):
    name         = "fct_study_milestone"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.fct_study_milestone.FctStudyMilestoneModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        sm = spark.table(f"{cat}.{ctms}.ctms_study_milestone")
        if "_ingestion_timestamp" in sm.columns:
            load_date_expr = F.to_date(F.col("_ingestion_timestamp")).alias("load_date")
        else:
            load_date_expr = F.current_date().alias("load_date")

        return (sm
                
                .select(
                    F.md5("study_id").alias("study_id_sk"),
                    F.md5("parent_id").alias("participant_id_sk"),
                    F.lit(None).cast("string").alias("study_country_id_sk"),
                    F.col("milestone_name"),
                    F.col("milestone_position"),
                    F.col("milestone_date").cast("date").alias("milestone_date"),
                    F.lit(None).cast("string").alias("cohort"),
                    load_date_expr,
                ).distinct())
