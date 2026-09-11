# src/gold/models/dim_cohort_summary.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class DimCohortSummaryModel(GoldModel):
    name         = "dim_cohort_summary"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_cohort_summary.DimCohortSummaryModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        csr = spark.table(f"{cat}.{ctms}.irt_cohort_summary_report")
        if "_ingestion_timestamp" in csr.columns:
            load_date_expr = F.to_date(F.col("_ingestion_timestamp")).alias("load_date")
        else:
            load_date_expr = F.current_date().alias("load_date")
        raw_match = F.regexp_extract(
            F.col("cohort_description"),
            r"(?i)^cohort\s+[A-Za-z0-9]+(?:\s*-\s*[0-9]+[A-Za-z]?)?",
            0,
        )
        
        extracted = F.regexp_replace(raw_match, r"\s*-\s*", "-")
        
        cohort_expr = F.when(raw_match != "", extracted,).otherwise(F.concat(F.lit("Cohort "),
                F.regexp_replace(F.split(F.col("cohort_description"), r"\s+").getItem(0), r"(?i)^cohort", "Cohort"),
            ))

        return (csr
                .withColumn("cohort", cohort_expr)
                .select(
                    "cohort_group","cohort", "cohort_description", "cohort_status",
                    "rank", "screening_cap", "enrollment_cap", "total_screened",
                    "total_enrolled", "dosing_scheme", "dose_level",
                    "list_of_approved_countries",
                    load_date_expr,
                    "study_id"
                ))
