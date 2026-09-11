# src/gold/models/fct_cohort_details.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class FctCohortDetailsModel(GoldModel):
    name         = "fct_cohort_details"
    write_mode   = "overwrite"
    dependencies = ["dim_participant", "dim_cohort_summary"]
    class_path   = "gold.models.fct_cohort_details.FctCohortDetailsModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat  = _quoted_catalog(spark)
        gold = _gold_schema(spark)

        dp = spark.table(f"{cat}.{gold}.dim_participant")
        ds = spark.table(f"{cat}.{gold}.dim_cohort_summary")

        # ── Resolve participant status via metadata table ──────────────────────
        # participant_status_master maps raw (irt_status, edc_status) combos to a
        # canonical participant_status_normalized value. Joining here means adding
        # a new raw status variant only requires a metadata row — no code change.
        psm = meta.get("participant_status")
        if psm is None:
            raise RuntimeError(
                "participant_status_master metadata table is required but was not loaded. "
                "Ensure common.participant_status_master exists before running this model."
            )
        lkp = (psm
               .select(F.upper("irt_status").alias("_li"),
                       F.upper("edc_status").alias("_le"),
                       F.col("participant_status_normalized").alias("_psn"))
               .distinct())
        dp = (dp
              .withColumn("_iu", F.upper("participant_status_irt"))
              .withColumn("_eu", F.upper("participant_status_edc"))
              .join(lkp,
                    F.col("_iu").eqNullSafe(F.col("_li")) &
                    F.col("_eu").eqNullSafe(F.col("_le")),
                    "left")
              .drop("_iu", "_eu", "_li", "_le"))

        psn = F.col("_psn")
        counts = (dp.groupBy("study_id", "tumor_type", "cohort")
                  .agg(
                      F.sum(F.when(psn == "In Screening",   1).otherwise(0)).alias("in_screening"),
                      F.sum(F.when(psn == "Screen Failure", 1).otherwise(0)).alias("screen_failed"),
                      F.sum(F.when(psn.isin("Screen Failure", "In Screening", "Enrolled"), 1).otherwise(0)).alias("total_screened"),
                      F.sum(F.when(psn == "Enrolled",       1).otherwise(0)).alias("total_enrolled"),
                      F.sum(F.when(psn == "Discontinued",   1).otherwise(0)).alias("total_discontinued"),
                  )
                  .drop("_psn"))

        caps = (ds.select("study_id", "cohort",
                          F.col("enrollment_cap").alias("enrollment_target"),
                          "dose_level", "cohort_description", "dosing_scheme")
                .distinct())

        return (counts.join(
            caps,
            (counts["study_id"] == caps["study_id"]) &
            (F.lower(F.coalesce(counts["cohort"], F.lit(""))) ==
             F.lower(F.coalesce(caps["cohort"],   F.lit("")))),
            "left"
        ).select(
            counts["study_id"], counts["tumor_type"], counts["cohort"],
            counts["total_screened"], counts["in_screening"], counts["screen_failed"],
            counts["total_enrolled"], counts["total_discontinued"],
            F.when(counts["total_screened"] > 0,
                   (counts["screen_failed"].cast("decimal(38,10)") /
                    counts["total_screened"].cast("decimal(38,10)")))
             .otherwise(F.lit(0.0).cast("decimal(38,10)")).alias("screen_failed_percent"),
            caps["enrollment_target"].cast("double"),
            caps["dose_level"], caps["cohort_description"], caps["dosing_scheme"],
        ))
