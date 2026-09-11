# src/gold/models/site_dates.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class SiteDatesModel(GoldModel):
    name         = "site_dates"
    write_mode   = "overwrite"
    dependencies = ["dim_study_site", "fct_site_milestone", "fct_participant_visit", "dim_participant"]
    class_path   = "gold.models.site_dates.SiteDatesModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        dss = spark.table(f"{gold}.dim_study_site")
        fsm = spark.table(f"{gold}.fct_site_milestone")
        fpv = spark.table(f"{gold}.fct_participant_visit")
        dp = spark.table(f"{gold}.dim_participant")

        valid_site_statuses = [
            "Active",
            "Active Enrolling",
            "Enrollment Open",
            "Enrollment Closed",
            "Activated",
            "Enrollment On-Hold",
            "On Hold",
            "Close Out Ready",
            "Closed",
            "Approved",
            "Initiated",
            "Selected",
            "Site Closed",
            "Qualified",
            "Submitted",
        ]

        sites = (
            dss.filter(F.col("site_type") == F.lit("Main"))
            .filter(F.col("study_site_status").isin(valid_site_statuses))
            .select(
                "study_site_id_sk",
                "study_site_id",
                "site_number",
                "parent_site_number",
                F.col("main_site_name").alias("site_name"),
                "study_site_status",
                "site_type",
                F.col("study_id").alias("protocol"),
                "study_id_sk",
            )
        )

        site_activation_date = (
            fsm.filter(F.col("milestone_name") == F.lit("Site Activation"))
            .filter(F.col("milestone_position") == F.lit("Actual"))
            .select("study_id_sk", F.col("parent_id"), F.col("milestone_date").alias("site_activation_date"))
        )

        site_selection_date = (
            fsm.filter(F.col("milestone_name").isin("Qualified"))
            .filter(F.col("milestone_position") == F.lit("Actual"))
            .groupBy("study_id_sk", "parent_id")
            .agg(F.min("milestone_date").alias("site_selection_date"))
        )

        default_fps = (
            fpv.filter(~F.col("study_id").isin("XL184-311", "XL184-312", "XL102-101"))
            .groupBy("study_id", "study_id_sk", "study_site_id_sk")
            .agg(F.min("date_of_visit_actual").alias("fps"))
        )

        infscr_fps = (
            fpv.filter(F.col("study_id").isin("XL184-312", "XL184-311"))
            .filter(F.col("visit_description") == F.lit("INFSCR"))
            .groupBy("study_id", "study_id_sk", "study_site_id_sk")
            .agg(F.min("date_of_first_icf").alias("fps"))
        )

        scr_fps = (
            fpv.filter(F.col("study_id") == F.lit("XL102-101"))
            .filter(F.col("most_recent_visit_type") == F.lit("SCR"))
            .groupBy("study_id", "study_id_sk", "study_site_id_sk")
            .agg(F.min("most_recent_date_of_visit").alias("fps"))
        )

        site_fps = default_fps.unionByName(infscr_fps).unionByName(scr_fps)

        default_fpe = (
            fpv.filter(~F.col("study_id").isin("XL184-311", "XL184-312", "XL102-101"))
            .filter(F.col("visit_description").isin("Randomization", "Enrollment", "Enrollment V1", "SSV1 (W1D1)", "W1D1"))
            .groupBy("study_id", "study_id_sk", "study_site_id_sk")
            .agg(F.min("date_of_visit_actual").alias("fpe"))
        )

        special_fpe = (
            fpv.alias("fpv")
            .join(dp.alias("dp"), F.col("dp.participant_id") == F.col("fpv.participant_number"), "inner")
            .filter(F.col("fpv.study_id").isin("XL102-101", "XL184-311", "XL184-312"))
            .filter(~F.col("dp.participant_status_master").isin("Screen Failure"))
            .groupBy(F.col("fpv.study_id"), F.col("fpv.study_id_sk"), F.col("fpv.study_site_id_sk"))
            .agg(F.min(F.col("fpv.randomization_or_screen_fail_date")).alias("fpe"))
        )

        site_fpe = default_fpe.unionByName(special_fpe)

        return (
            sites.alias("s")
            .join(
                site_activation_date.alias("sa"),
                (F.col("s.study_id_sk") == F.col("sa.study_id_sk"))
                & (F.col("s.study_site_id") == F.col("sa.parent_id")),
                "left",
            )
            .join(
                site_selection_date.alias("ss"),
                (F.col("s.study_id_sk") == F.col("ss.study_id_sk"))
                & (F.col("s.study_site_id") == F.col("ss.parent_id")),
                "left",
            )
            .join(
                site_fps.alias("sf"),
                (F.col("s.study_id_sk") == F.col("sf.study_id_sk"))
                & (F.col("s.study_site_id_sk") == F.col("sf.study_site_id_sk")),
                "left",
            )
            .join(
                site_fpe.alias("se"),
                (F.col("s.study_id_sk") == F.col("se.study_id_sk"))
                & (F.col("s.study_site_id_sk") == F.col("se.study_site_id_sk")),
                "left",
            )
            .select(
                F.col("s.study_id_sk"),
                F.col("s.study_site_id_sk"),
                F.col("s.protocol"),
                F.col("s.study_site_id"),
                F.col("s.site_number"),
                F.col("s.parent_site_number"),
                F.col("s.site_name"),
                F.col("s.study_site_status"),
                F.col("s.site_type"),
                F.col("sa.site_activation_date"),
                F.col("ss.site_selection_date"),
                F.col("sf.fps").alias("first_patient_screened"),
                F.col("se.fpe").alias("first_patient_enrolled"),
            )
        )