# src/gold/models/gold_v_cohort_enrollment_trend.py
# =============================================================================
# PySpark model: gold_v_cohort_enrollment_trend
#
# Study filter and enrollment_visit_names are driven by
# common.ct_portfolio_source WHERE use_cohort_summary = TRUE.
# TF+ tumor-type grouping is driven by common.study_tf_plus_tumors.
# =============================================================================

import logging
from typing import Dict

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window

from gold_model import GoldModel

logger = logging.getLogger(__name__)


class GoldVCohortEnrollmentTrendModel(GoldModel):
    name         = "gold_v_cohort_enrollment_trend"
    write_mode   = "overwrite"
    dependencies = ["fct_participant_visit", "dim_participant", "dim_calendar"]
    tags         = {}
    class_path   = "gold.models.gold_v_cohort_enrollment_trend.GoldVCohortEnrollmentTrendModel"

    def build(
        self,
        spark:  SparkSession,
        silver: Dict[str, DataFrame],
        meta:   Dict[str, DataFrame],
    ) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema, _common_schema
        cat    = _quoted_catalog(spark)
        gold   = f"{cat}.{_gold_schema(spark)}"
        common = f"{cat}.{_common_schema(spark)}"

        fpv = spark.table(f"{gold}.fct_participant_visit")
        dp  = spark.table(f"{gold}.dim_participant")
        cal = spark.table(f"{gold}.dim_calendar").select(F.col("date").alias("cal_date"))
        cps = (spark.table(f"{common}.ct_portfolio_source")
               .filter(F.col("use_cohort_summary") == True)
               .select("study_id", "enrollment_visit_names"))
        tf  = (spark.table(f"{common}.study_tf_plus_tumors")
               .select(F.col("study_id").alias("_tf_sid"),
                       F.col("tumor_type").alias("_tf_tumor"),
                       F.col("display_group").alias("_tf_display")))

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

        # Status normalization is sourced directly from dim_participant.
        dp_slim = (dp
            .select(
                "study_id_sk", "participant_id_sk", "cohort", "tumor_type",
                "cohort_status", "study_phase",
                F.col("participant_number").alias("_dp_pnum"),  # renamed to avoid ambiguity with fpv
                F.col("participant_status_master").alias("_psn"),
            )
            # .withColumn("_iu", F.upper("participant_status_irt"))
            # .withColumn("_eu", F.upper("participant_status_edc"))
            # .join(lkp_psm,
            #       F.col("_iu").eqNullSafe(F.col("_li")) &
            #       F.col("_eu").eqNullSafe(F.col("_le")),
            #       "left")
            # .drop("_iu", "_eu", "participant_status_irt", "participant_status_edc")
        )

        # -- forecast_information: actual enrollment counts per (study, cohort, tumor_type, month) --
        fi = (fpv
            .join(cps, "study_id", "inner")
            .join(dp_slim, ["study_id_sk", "participant_id_sk"], "inner")
            .join(tf,
                  (F.col("study_id") == tf["_tf_sid"]) & (F.col("tumor_type") == tf["_tf_tumor"]),
                  "left")
            .drop("_tf_sid", "_tf_tumor")
            .filter(
                ~F.col("_psn").isin(
                    "Screen Failure", "In Screening", "Screening") &
                F.array_contains(
                    F.split(F.col("enrollment_visit_names"), ","),
                    F.col("visit_description"))
            )
            .withColumn("month_end_date", F.last_day(F.col("date_of_visit_actual").cast("date")))
            .withColumn("tumor_type_d", F.coalesce(F.col("_tf_display"), F.col("tumor_type")))
            .drop("_tf_display", "_psn")
            .groupBy("study_id", "cohort", "tumor_type_d", "cohort_status", "month_end_date", "study_phase")
            .agg(F.countDistinct("_dp_pnum").alias("actual_enroll_participants"))
            .withColumn("forecasted_participants", F.lit(0).cast("long"))
        )

        # -- date_ranges: start/end month per (study, cohort) for date spine --
        w = Window.partitionBy("study_id", "cohort")
        date_ranges = (fi
            .select(
                "study_id", "cohort", "tumor_type_d", "cohort_status", "study_phase",
                F.min("month_end_date").over(w).alias("start_date"),
                F.max("month_end_date").over(w).alias("end_date"),
            )
            .distinct()
        )

        # -- forecast_data: cross calendar dates with date_ranges, left-join actuals --
        # Rename date_ranges columns to avoid ambiguity in the left join
        dr = date_ranges.select(
            F.col("study_id").alias("dr_study_id"),
            F.col("cohort").alias("dr_cohort"),
            F.col("tumor_type_d").alias("dr_tumor_type"),
            F.col("cohort_status").alias("dr_cohort_status"),
            F.col("study_phase").alias("dr_study_phase"),
            "start_date",
            "end_date",
        )

        spine = (dr
            .join(cal,
                  (cal["cal_date"] >= dr["start_date"]) &
                  (cal["cal_date"] <= dr["end_date"]) &
                  (F.dayofmonth(cal["cal_date"]) == F.dayofmonth(F.last_day(cal["cal_date"]))),
                  "inner")
            .drop("start_date", "end_date")
        )

        fi_keyed = fi.select(
            F.col("study_id").alias("fi_study_id"),
            F.col("cohort").alias("fi_cohort"),
            F.col("tumor_type_d").alias("fi_tumor_type"),
            F.col("cohort_status").alias("fi_cohort_status"),
            F.col("study_phase").alias("fi_study_phase"),
            F.col("month_end_date").alias("fi_month_end_date"),
            "actual_enroll_participants",
        )

        result = (spine
            .join(fi_keyed,
                  (F.col("dr_study_id")      == F.col("fi_study_id")) &
                  (F.col("dr_cohort")        == F.col("fi_cohort")) &
                  (F.col("dr_tumor_type")    == F.col("fi_tumor_type")) &
                  (F.col("dr_cohort_status") == F.col("fi_cohort_status")) &
                  (F.col("dr_study_phase")   == F.col("fi_study_phase")) &
                  (F.col("cal_date")         == F.col("fi_month_end_date")),
                  "left")
            .select(
                F.col("dr_study_id").alias("study_id"),
                F.col("dr_cohort").alias("cohort"),
                F.col("dr_tumor_type").alias("tumor_type"),
                F.col("dr_cohort_status").alias("cohort_status"),
                F.col("dr_study_phase").alias("study_phase"),
                F.col("cal_date").alias("month_end_date"),
                F.coalesce(F.col("actual_enroll_participants"), F.lit(0)).alias("actual_enroll_participants"),
                F.lit(0).cast("long").alias("forecasted_participants"),
            )
        )

        logger.info("GoldVCohortEnrollmentTrendModel: build complete")
        return result
