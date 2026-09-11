# src/gold/models/fct_site_milestone.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class FctSiteMilestoneModel(GoldModel):
    name         = "fct_site_milestone"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.fct_site_milestone.FctSiteMilestoneModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        return (spark.table(f"{cat}.{ctms}.ctms_study_milestone")
                
                .filter(F.col("milestone_date").isNotNull())
                .select(
                    F.md5("study_id").alias("study_id_sk"),
                    F.col("parent_id"),
                    F.md5("parent_id").alias("study_site_id_sk"),
                    F.col("milestone_name"),
                    F.col("milestone_position"),
                    F.col("milestone_date").cast("date").alias("milestone_date"),
                ).distinct())
