from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.utils import AnalysisException

from gold_model import GoldModel


class GoldVCountryDashboardCustomModel(GoldModel):
    name = "gold_v_country_dashboard_custom"
    write_mode = "overwrite"
    dependencies = ["fct_study_site", "dim_study_country", "fct_site_milestone", "fct_participant_visit", "dim_participant"]
    class_path = "gold.models.gold_v_country_dashboard_custom.GoldVCountryDashboardCustomModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema, _common_schema

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"
        common = f"{cat}.{_common_schema(spark)}"

        dss = spark.table(f"{gold}.dim_study_site")
        dsc = spark.table(f"{gold}.dim_study_country")
        fsm = spark.table(f"{gold}.fct_site_milestone")
        fpv = spark.table(f"{gold}.fct_participant_visit")
        dp = spark.table(f"{gold}.dim_participant")

        lsef: Optional[DataFrame] = None
        try:
            lsef = spark.table(f"{common}.lkp_sites_enrollment_forecast")
        except AnalysisException:
            lsef = None

        study_ids = [
            "XL092-303", "XL092-002", "XB002-101", "XL092-009", "XL092-305",
            "XL092-304", "XL309-101", "XB010-101", "XB371-101", "XL495-101",
            "XB628-101", "XL092-311", "XL092-201",
        ]

        active_studies = [
            "XB002-101", "XL092-002", "XL092-304", "XL092-009", "XL092-305",
            "XL309-101", "XB010-101", "XB371-101", "XL495-101", "XB628-101",
            "XL092-201",
        ]

        planned_studies = [
            "XB002-101", "XL092-002", "XL092-304", "XL092-009", "XL092-305",
            "XL309-101", "XB010-101", "XB371-101", "XL495-101", "XB628-101",
        ]

        common_active_statuses = [
            "Active Enrolling", "Activated", "Enrollment Open", "Approved",
            "Close Out Ready", "Closed", "Selected", "Enrollment Closed",
            "Enrollment On-Hold", "Qualified", "Site Closed", "Submitted",
        ]

        active_without_trio_statuses = [
            "Active Enrolling", "Activated", "Enrollment Open", "Close Out Ready",
            "Closed", "Enrollment Closed", "Enrollment On-Hold", "Site Closed",
        ]

        planned_without_trio_statuses = [
            "Active Enrolling", "Activated", "Enrollment Open", "Approved",
            "Close Out Ready", "Closed", "Selected", "Enrollment Closed",
            "Enrollment On-Hold", "Qualified", "Site Closed", "Submitted",
        ]

        # Sites_remaining
        sites_remaining = (
            dss.alias("dss").join(
                dsc.select(
                    "study_id",
                    "study_country_id",
                    "study_country_id_sk",
                    F.col("country_region").alias("region"),
                    "country_name",
                ),
                ["study_id", "study_country_id"],
                "inner",
            )
            .join(
                fsm.alias("fsm").select(
                    F.col("study_id_sk").alias("_fsm_study_id_sk"),
                    F.col("parent_id").alias("_fsm_parent_id"),
                    F.col("milestone_name").alias("_fsm_milestone_name"),
                    F.col("milestone_position").alias("_fsm_milestone_position"),
                    F.col("milestone_date").alias("_fsm_milestone_date"),
                ),
                (F.col("dss.study_site_id") == F.col("_fsm_parent_id")) & (F.col("dss.study_id_sk") == F.col("_fsm_study_id_sk")),
                "left",
            )
            .filter(F.col("dss.study_site_status").isin(*common_active_statuses))
            .filter(F.col("dss.study_id").isin(*study_ids))
            .filter(F.col("_fsm_milestone_name").isin("Site Activation", "Qualified"))
            .filter(F.col("_fsm_milestone_position") == "Actual")
            .groupBy(F.col("dss.study_id_sk").alias("study_id_sk"), F.col("dss.study_id").alias("study_id"), F.col("region"), F.col("country_name"))
            .agg(F.min(F.when(F.col("_fsm_milestone_name") == "Site Activation", F.col("_fsm_milestone_date"))).alias("first_site_activation_date"))
        )

        # active_sites
        active_sites_main = (
            dss.join(
                dsc.select("study_id", "study_country_id", F.col("country_region").alias("region"), "country_name"),
                ["study_id", "study_country_id"],
                "inner",
            )
            .filter(F.col("study_id").isin(*active_studies))
            .filter(F.col("study_site_status").isin(*active_without_trio_statuses))
            .filter(F.col("site_type") == "Main")
            .groupBy("study_id", "region", "country_name")
            .agg(F.countDistinct("study_site_id_sk").alias("active_sites"))
        )

        active_sites_trio = (
            dss.join(
                dsc.select("study_id", "study_country_id", F.col("country_region").alias("region"), "country_name"),
                ["study_id", "study_country_id"],
                "inner",
            )
            .filter(F.col("study_id").isin("XL092-303", "XL092-311"))
            .filter(F.col("study_site_status").isin(*active_without_trio_statuses))
            .filter(F.col("site_number") != "TRIO")
            .filter(F.col("site_type") == "Main")
            .groupBy("study_id", "region", "country_name")
            .agg(F.countDistinct("study_site_id_sk").alias("active_sites"))
        )

        active_sites = active_sites_main.unionByName(active_sites_trio)

        # planned_sites
        planned_main = (
            dss.join(
                dsc.select("study_id", "study_country_id", F.col("country_region").alias("region"), "country_name"),
                ["study_id", "study_country_id"],
                "inner",
            )
            .filter(F.col("study_id").isin(*planned_studies))
            .filter(F.col("study_site_status").isin(*planned_without_trio_statuses))
            .filter(F.col("site_type") == "Main")
            .groupBy("study_id", "region", "country_name")
            .agg(F.countDistinct("study_site_id_sk").alias("planned_sites"))
        )

        planned_trio = (
            dss.join(
                dsc.select("study_id", "study_country_id", F.col("country_region").alias("region"), "country_name"),
                ["study_id", "study_country_id"],
                "inner",
            )
            .filter(F.col("study_id").isin("XL092-303", "XL092-311"))
            .filter(F.col("study_site_status").isin(*planned_without_trio_statuses))
            .filter(F.col("site_number") != "TRIO")
            .filter(F.col("site_type") == "Main")
            .groupBy("study_id", "region", "country_name")
            .agg(F.countDistinct("study_site_id_sk").alias("planned_sites"))
        )

        planned_forecast = None
        if lsef is not None:
            planned_forecast = (
                dss.join(
                    dsc.select("study_id", "study_country_id", F.col("country_region").alias("region"), "country_name"),
                    ["study_id", "study_country_id"],
                    "inner",
                )
                .join(lsef.select("study_id", "forecasted", "milestone_name"), "study_id", "inner")
                .filter(F.col("study_id") == "XL092-201")
                .filter(F.col("study_site_status").isin(*planned_without_trio_statuses))
                .filter(F.col("site_type") == "Main")
                .filter(F.col("milestone_name") == "Site Activation")
                .groupBy("study_id", "region", "country_name")
                .agg(F.max("forecasted").alias("planned_sites"))
            )

        planned_sites = planned_main.unionByName(planned_trio)
        if planned_forecast is not None:
            planned_sites = planned_sites.unionByName(planned_forecast)

        # first_participant_in_date
        first_participant = (
            fpv.alias("fpv").join(
                dsc.alias("dsc").select(
                    F.col("dsc.study_id").alias("_dsc_study_id"),
                    F.col("dsc.study_country_id_sk").alias("_dsc_study_country_id_sk"),
                    F.col("dsc.study_id_sk").alias("_dsc_study_id_sk"),
                    F.col("dsc.country_region").alias("region"),
                    F.col("dsc.country_name").alias("country_name"),
                ),
                (F.col("fpv.study_country_id_sk") == F.col("_dsc_study_country_id_sk")) & (F.col("fpv.study_id_sk") == F.col("_dsc_study_id_sk")),
                "inner",
            )
            .filter(F.col("fpv.visit_description") == "Randomization")
            .filter(F.col("fpv.study_id").isin(*study_ids))
            .groupBy(
                F.col("fpv.study_id_sk").alias("study_id_sk"),
                F.col("fpv.study_id").alias("study_id"),
                F.col("region"),
                F.col("country_name"),
            )
            .agg(F.min(F.col("fpv.date_of_visit_actual")).alias("first_participant_in_date"))
        )

        # Enrollment_remaining
        enrollment_remaining = (
            fpv.alias("fpv").join(
                dss.alias("dss").select(
                    F.col("dss.study_site_id_sk").alias("study_site_id_sk"),
                    F.col("dss.study_id").alias("study_id"),
                    F.col("dss.study_id_sk").alias("_dss_study_id_sk"),
                    F.col("dss.study_country_id").alias("study_country_id"),
                    F.col("dss.study_site_status").alias("study_site_status"),
                ),
                ["study_site_id_sk", "study_id"],
                "inner",
            )
            .join(
                dsc.select(
                    "study_id",
                    "study_country_id",
                    "study_country_id_sk",
                    F.col("country_region").alias("region"),
                    "country_name",
                ),
                ["study_id", "study_country_id"],
                "inner",
            )
            .join(
                dp.select("study_id", "participant_id_sk", "participant_status_master"),
                ["study_id", "participant_id_sk"],
                "inner",
            )
            .filter(F.col("study_site_status").isin("Active Enrolling", "Activated", "Enrollment Open", "Selected", "Close Out Ready", "Closed", "Enrollment Closed", "Enrollment On-Hold", "Site Closed"))
            .filter(F.col("fpv.visit_description").isin("Randomization", "Day 1", "Randomized"))
            .filter(F.col("participant_status_master").isin("Off Study", "On Treatment", "Randomized", "In Follow-up", "In Follow Up"))
            .filter(F.col("fpv.study_id").isin(*study_ids))
            .groupBy(
                F.col("fpv.study_id_sk").alias("study_id_sk"),
                F.col("fpv.study_id").alias("study_id"),
                F.col("region"),
                F.col("country_name"),
                F.last_day(F.col("fpv.date_of_visit_actual")).alias("month_end_date"),
            )
            .agg(
                F.min(F.col("fpv.date_of_visit_actual")).alias("first_participant_in_date"),
                F.countDistinct("participant_id_sk").alias("participants"),
            )
            .select(
                "study_id_sk", "study_id", "region", "country_name", "first_participant_in_date", "month_end_date", "participants"
            )
        )

        # Screened_remaining
        screened_remaining = (
            fpv.alias("fpv").join(
                dss.alias("dss").select(
                    F.col("dss.study_site_id_sk").alias("study_site_id_sk"),
                    F.col("dss.study_id").alias("study_id"),
                    F.col("dss.study_id_sk").alias("_dss_study_id_sk"),
                    F.col("dss.study_country_id").alias("study_country_id"),
                    F.col("dss.study_site_status").alias("study_site_status"),
                ),
                ["study_site_id_sk", "study_id"],
                "inner",
            )
            .join(
                dsc.select(
                    "study_id",
                    "study_country_id",
                    "study_country_id_sk",
                    F.col("country_region").alias("region"),
                    "country_name",
                ),
                ["study_id", "study_country_id"],
                "inner",
            )
            .join(
                dp.select("study_id", "participant_id_sk", "participant_status_master"),
                ["study_id", "participant_id_sk"],
                "inner",
            )
            .filter(F.col("study_site_status").isin("Active Enrolling", "Activated", "Enrollment Open", "Selected", "Close Out Ready", "Closed", "Approved", "Enrollment Closed", "Enrollment On-Hold", "Site Closed"))
            .filter(F.col("fpv.visit_description") == "Screening")
            .filter(F.col("fpv.study_id").isin(*study_ids))
            .groupBy(
                F.col("fpv.study_id_sk").alias("study_id_sk"),
                F.col("fpv.study_id").alias("study_id"),
                F.col("region"),
                F.col("country_name"),
                F.last_day(F.col("fpv.date_of_visit_actual")).alias("month_end_date"),
            )
            .agg(
                F.min(F.col("fpv.date_of_visit_actual")).alias("first_participant_in_date"),
                F.countDistinct("participant_id_sk").alias("participants"),
            )
            .select(
                "study_id_sk", "study_id", "region", "country_name", "first_participant_in_date", "month_end_date", "participants"
            )
        )

        active_alias = active_sites.select("study_id", "region", "country_name", F.col("active_sites").alias("active_sites"))
        planned_alias = planned_sites.select("study_id", "region", "country_name", F.col("planned_sites").alias("planned_sites"))

        base = (
            sites_remaining.alias("a")
            .join(enrollment_remaining.alias("b"), [F.col("a.study_id") == F.col("b.study_id"), F.col("a.region") == F.col("b.region"), F.col("a.country_name") == F.col("b.country_name")], "left")
            .join(first_participant.alias("c"), [F.col("a.study_id") == F.col("c.study_id"), F.col("a.region") == F.col("c.region"), F.col("a.country_name") == F.col("c.country_name")], "left")
            .join(active_alias.alias("sa"), [F.col("a.study_id") == F.col("sa.study_id"), F.col("a.region") == F.col("sa.region"), F.col("a.country_name") == F.col("sa.country_name")], "left")
            .join(planned_alias.alias("sp"), [F.col("a.study_id") == F.col("sp.study_id"), F.col("a.region") == F.col("sp.region"), F.col("a.country_name") == F.col("sp.country_name")], "inner")
            .select(
                F.col("a.study_id_sk").alias("study_id_sk"),
                F.col("a.study_id").alias("study_id"),
                F.col("a.region").alias("region"),
                F.col("a.country_name").alias("country_name"),
                F.col("a.first_site_activation_date").alias("first_site_activation_date"),
                F.col("c.first_participant_in_date").alias("first_participant_in_date"),
                F.coalesce(F.col("sa.active_sites"), F.lit(0)).alias("active_sites"),
                F.coalesce(F.col("sp.planned_sites"), F.lit(0)).alias("planned_sites"),
                F.col("b.month_end_date").alias("month_end_date"),
                F.coalesce(F.col("b.participants"), F.lit(0)).alias("participants"),
                F.lit("Enrolled").alias("status"),
            )
        )

        screened_alias = (
            sites_remaining.alias("a")
            .join(screened_remaining.alias("b"), [F.col("a.study_id") == F.col("b.study_id"), F.col("a.region") == F.col("b.region"), F.col("a.country_name") == F.col("b.country_name")], "left")
            .join(first_participant.alias("c"), [F.col("a.study_id") == F.col("c.study_id"), F.col("a.region") == F.col("c.region"), F.col("a.country_name") == F.col("c.country_name")], "left")
            .join(active_alias.alias("sa"), [F.col("a.study_id") == F.col("sa.study_id"), F.col("a.region") == F.col("sa.region"), F.col("a.country_name") == F.col("sa.country_name")], "left")
            .join(planned_alias.alias("sp"), [F.col("a.study_id") == F.col("sp.study_id"), F.col("a.region") == F.col("sp.region"), F.col("a.country_name") == F.col("sp.country_name")], "inner")
            .select(
                F.col("a.study_id_sk").alias("study_id_sk"),
                F.col("a.study_id").alias("study_id"),
                F.col("a.region").alias("region"),
                F.col("a.country_name").alias("country_name"),
                F.col("a.first_site_activation_date").alias("first_site_activation_date"),
                F.col("c.first_participant_in_date").alias("first_participant_in_date"),
                F.coalesce(F.col("sa.active_sites"), F.lit(0)).alias("active_sites"),
                F.coalesce(F.col("sp.planned_sites"), F.lit(0)).alias("planned_sites"),
                F.col("b.month_end_date").alias("month_end_date"),
                F.coalesce(F.col("b.participants"), F.lit(0)).alias("participants"),
                F.lit("Screened").alias("status"),
            )
        )

        return base.unionByName(screened_alias).orderBy("region", "country_name")