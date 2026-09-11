# src/gold/models/vw_participant_milestone.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class VwParticipantMilestoneModel(GoldModel):
    name         = "vw_participant_milestone"
    write_mode   = "overwrite"
    dependencies = ["fct_participant_milestone"]
    class_path   = "gold.models.vw_participant_milestone.VwParticipantMilestoneModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema
        cat  = _quoted_catalog(spark)
        gold = _gold_schema(spark)
        fpm  = spark.table(f"{cat}.{gold}.fct_participant_milestone")
        return (
            fpm
            .groupBy("study_id_sk", "participant_id_sk", "milestone_name")
            .agg(
                F.max(F.when(F.col("milestone_position") == "Target",    F.col("milestone_date"))).alias("target"),
                F.max(F.when(F.col("milestone_position") == "Actual",    F.col("milestone_date"))).alias("actual"),
                F.max(F.when(F.col("milestone_position") == "Projected", F.col("milestone_date"))).alias("projected"),
            )
        )
