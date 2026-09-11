# src/gold/models/gold_v_country_dashboard.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class GoldVCountryDashboardModel(GoldModel):
    name         = "gold_v_country_dashboard"
    write_mode   = "overwrite"
    dependencies = ["fct_study_site", "dim_study_country", "vw_site_milestone", "fct_participant_visit"]
    class_path   = "gold.models.gold_v_country_dashboard.GoldVCountryDashboardModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema
        cat  = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        fss = spark.table(f"{gold}.fct_study_site")
        dsc = spark.table(f"{gold}.dim_study_country")
        vsm = spark.table(f"{gold}.vw_site_milestone")
        fpv = spark.table(f"{gold}.fct_participant_visit")

        # active site statuses from meta (with fallback)
        site_master = meta.get("site_status_master")
        if site_master is not None:
            active_statuses = [
                r.ctms_status for r in
                site_master.filter(F.col("group_name") == "ACTIVE").select("ctms_status").distinct().collect()
            ]
        else:
            active_statuses = ["Active Enrolling", "Active Not Enrolling"]

        # Slim column sets to avoid ambiguity across joins
        fss_base = fss.select(
            "study_id_sk", "study_id", "study_country_id", "study_site_id_sk",
            "study_site_status", "site_type",
        )
        dsc_base = dsc.select(
            "study_id", "study_country_id", "study_country_id_sk",
            F.col("country_region").alias("region"), "region_full_name", "country_name",
        )

        # first_site_activation: earliest qualifying milestone date per (study, region, country)
        fsa = (fss_base
            .join(dsc_base.drop("study_country_id_sk"), ["study_country_id", "study_id"], "inner")
            .join(vsm.select("study_site_id_sk",
                             F.col("milestone_name").alias("_vsm_name"),
                             F.col("actual").alias("_vsm_actual")),
                  "study_site_id_sk", "left")
            .groupBy("study_id_sk", "study_id", "region", "region_full_name", "country_name")
            .agg(
                F.min(F.when(
                    F.col("_vsm_name").isin("Site Activation", "Qualified"),
                    F.col("_vsm_actual")
                )).alias("site_activation_date")
            )
        )

        # screened_monthly: enrollment participant counts per (study, region, country, month)
        sm = (fpv
            .join(dsc_base.select("study_country_id_sk", "region", "country_name"),
                  "study_country_id_sk", "inner")
            .groupBy("study_id", "region", "country_name", "month_end_date")
            .agg(
                F.countDistinct(
                    F.when(F.col("visit_type_group") == "Enrollment", F.col("participant_id_sk"))
                ).alias("participants"),
                F.lit("Enrolled").alias("status"),
            )
        )

        # first_participant_in: earliest enrollment visit per (study, region, country)
        fpi = (fpv
            .filter(F.col("visit_type_group") == "Enrollment")
            .join(dsc_base.select("study_country_id_sk", "region", "country_name"),
                  "study_country_id_sk", "inner")
            .groupBy("study_id", "region", "country_name")
            .agg(F.min("date_of_visit_actual").alias("first_participant_date"))
        )

        # active_sites: distinct active sites per (study, region, country)
        act = (fss_base
            .filter(F.col("study_site_status").isin(active_statuses))
            .join(dsc_base.drop("study_country_id_sk"), ["study_country_id", "study_id"], "inner")
            .groupBy("study_id", "region", "country_name")
            .agg(F.countDistinct("study_site_id_sk").alias("active_sites"))
        )

        # planned_sites: all Main-type sites per (study, region, country)
        pln = (fss_base
            .filter(F.col("site_type") == "Main")
            .join(dsc_base.drop("study_country_id_sk"), ["study_country_id", "study_id"], "inner")
            .groupBy("study_id", "region", "country_name")
            .agg(F.countDistinct("study_site_id_sk").alias("planned_sites"))
        )

        # Final join - fsa drives rows; sm provides the month-end grain
        return (fsa
            .join(sm,  ["study_id", "region", "country_name"], "left")
            .join(fpi, ["study_id", "region", "country_name"], "left")
            .join(act, ["study_id", "region", "country_name"], "left")
            .join(pln, ["study_id", "region", "country_name"], "inner")
            .select(
                "study_id_sk", "study_id", "region", "country_name",
                "site_activation_date", "first_participant_date",
                F.coalesce(F.col("active_sites"),  F.lit(0)).alias("active_sites"),
                F.coalesce(F.col("planned_sites"), F.lit(0)).alias("planned_sites"),
                "month_end_date",
                F.coalesce(F.col("participants"),  F.lit(0)).alias("participants"),
                "status",
                "region_full_name",
            )
        )
