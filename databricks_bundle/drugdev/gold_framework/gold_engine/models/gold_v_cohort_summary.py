# src/gold/models/gold_v_cohort_summary.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class GoldVCohortSummaryModel(GoldModel):
    name         = "gold_v_cohort_summary"
    write_mode   = "overwrite"
    dependencies = ["dim_participant"]
    class_path   = "gold.models.gold_v_cohort_summary.GoldVCohortSummaryModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema, _common_schema
        cat    = _quoted_catalog(spark)
        gold   = f"{cat}.{_gold_schema(spark)}"
        common = f"{cat}.{_common_schema(spark)}"

        dp  = spark.table(f"{gold}.dim_participant")
        cps = (spark.table(f"{common}.ct_portfolio_source")
               .filter(F.col("use_cohort_summary") == True)
               .select("study_id", "cohort_group_formula"))
        tf  = (spark.table(f"{common}.study_tf_plus_tumors")
               .select(F.col("study_id").alias("_tf_sid"),
                       F.col("tumor_type").alias("_tf_tumor"),
                       F.col("display_group").alias("_tf_group")))

        cgf        = F.col("cohort_group_formula")
        tf_matched = F.col("_tf_tumor").isNotNull()

        # -- Resolve canonical status from metadata --------------------------------
        # psm = meta.get("participant_status")
        # if psm is None:
        #     raise RuntimeError(
        #         "participant_status_master metadata table is required but was not loaded. "
        #         "Ensure common.participant_status_master exists before running this model."
        #     )
        # lkp_psm = (psm
        #            .select(F.upper("irt_status").alias("_li"),
        #                    F.upper("edc_status").alias("_le"),
        #                    F.col("participant_status_normalized").alias("_psn"))
        #            .distinct())

        # Build raw rows: one per participant with derived cohort_group and tumor_type
        raw = (dp
            .join(cps, "study_id", "inner")
            .join(tf,
                  (F.col("study_id") == tf["_tf_sid"]) & (F.col("tumor_type") == tf["_tf_tumor"]),
                  "left")
            .drop("_tf_sid")
            .filter(F.col("participant_number").cast("long").isNotNull())
            .withColumn("cohort_group",
                F.when(cgf == "PHASE_COHORT",
                       F.coalesce(F.concat(F.col("study_phase"), F.lit(" Phase "), F.col("cohort")),
                                  F.lit("")))
                 .when((cgf == "TF_PLUS") & tf_matched,
                       F.coalesce(F.col("_tf_group"), F.lit("TF+ Cancers")))
                 .when(cgf.isin("TF_PLUS", "TUMOR_TYPE"), F.col("tumor_type")))
            .withColumn("tumor_type",
                F.when(cgf == "PHASE_COHORT",  F.coalesce(F.col("tumor_type"), F.lit("")))
                 .when((cgf == "TF_PLUS") & tf_matched,
                       F.coalesce(F.col("_tf_group"), F.lit("TF+ Cancers")))
                 .when(cgf.isin("TF_PLUS", "TUMOR_TYPE"), F.col("tumor_type")))
            # .withColumn("_iu", F.upper("participant_status_irt"))
            # .withColumn("_eu", F.upper("participant_status_edc"))
            # .join(lkp_psm,
            #       F.col("_iu").eqNullSafe(F.col("_li")) &
            #       F.col("_eu").eqNullSafe(F.col("_le")),
            #       "left")
            # .drop("_iu", "_eu", "_li", "_le")
            .drop("_tf_tumor", "_tf_group", "cohort_group_formula")
        )

        # Aggregate counts per (study, cohort, cohort_status, cohort_group, tumor_type)
        indices = (raw
            .groupBy("study_id", "cohort", "cohort_status", "cohort_group", "tumor_type")
            .agg(
                F.count(F.when(F.col("participant_status_master") == "In Screening",
                               F.col("participant_id"))).alias("In_Screening"),
                F.count(F.when(F.col("participant_status_master") == "Screen Failure",
                               F.col("participant_id"))).alias("Screen_Failed"),
                F.count(F.when(
                    F.col("participant_status_master").isin(
                        "Randomized", "In Follow-up", "Off Study", "On Treatment") &
                    F.col("participant_status_irt").isin(
                        "Enrolled", "Treatment Completed", "Randomized"),
                    F.col("participant_id")
                )).alias("_enrolled"),
                F.count(F.when(
                    F.col("participant_status_master").isin("In Follow-up", "Off Study") &
                    (F.col("participant_status_irt") == "Discontinued"),
                    F.col("participant_id")
                )).alias("total_discontinued"),
            )
            .withColumn("total_enrolled",  F.col("_enrolled") + F.col("total_discontinued"))
            .withColumn("total_screened",  F.col("In_Screening") + F.col("Screen_Failed") +
                                           F.col("_enrolled") + F.col("total_discontinued"))
            .withColumn("Screen_Failed_percent",
                F.when(F.col("Screen_Failed") != 0,
                       F.round(F.col("Screen_Failed").cast("decimal(7,2)") /
                               (F.col("Screen_Failed") + F.col("total_enrolled")).cast("decimal(7,2)") * 100))
                 .otherwise(F.lit(0)))
            .drop("_enrolled")
        )

        return indices.select(
            "study_id", "cohort", "cohort_status", "cohort_group", "tumor_type",
            "total_screened", "In_Screening", "Screen_Failed",
            "total_enrolled", "total_discontinued", "Screen_Failed_percent",
        )
