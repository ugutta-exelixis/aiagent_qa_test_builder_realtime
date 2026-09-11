# src/gold/models/dim_planisware_daily_hist.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from gold_model import GoldModel


class DimPlaniswareDailyHistModel(GoldModel):
    name         = "dim_planisware_daily_hist"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_planisware_daily_hist.DimPlaniswareDailyHistModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        return (
            spark.table(f"{cat}.{ctms}.planisware_milestones")
            .select(
                "project",
                "project_type",
                "is_public_version",
                "line_identifier",
                "name",
                "activity_type",
                "activity_type_actual",
                "protocol_number",
                "study_phase_norm",
                "cohort",
                "planned_finish",
                "actual_finish",
                "approved_baseline",
                "original_baseline",
                "load_date",
                "last_modified_date",
                "status",
                "is_required",
                "protocol_activity_type_actual_sk",
                "protocol_number_sk",
                "protocol_no_study_phase_sk",
            )
        )