# src/gold/models/gold_v_drill_down_site_info.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class GoldVDrillDownSiteInfoModel(GoldModel):
    name         = "gold_v_drill_down_site_info"
    write_mode   = "overwrite"
    dependencies = ["dim_study_site", "dim_study_country", "fct_site_milestone"]
    class_path   = "gold.models.gold_v_drill_down_site_info.GoldVDrillDownSiteInfoModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        s = spark.table(f"{gold}.dim_study_site").alias("s")
        ps = spark.table(f"{gold}.dim_study_site").alias("ps")
        dc = spark.table(f"{gold}.dim_study_country").alias("dc")
        m = spark.table(f"{gold}.fct_site_milestone").alias("m")

        # study_ids = [
        #     "XB002-101", "XL092-002", "XL092-009", "XL092-303",
        #     "XL092-304", "XL092-305", "XL309-101", "XB010-101",
        #     "XL495-101", "XB371-101", "XB628-101", "XL092-311",
        #     "XL092-201"
        # ]

        return (
            s
            .join(
                ps,
                (s["parent_site_number"] == ps["site_number"]) &
                (s["study_id"] == ps["study_id"]),
                "inner"
            )
            .join(
                dc,
                (s["study_country_id"] == dc["study_country_id"]) &
                (s["study_id"] == dc["study_id"]) &
                (ps["study_id_sk"] == dc["study_id_sk"]),
                "inner"
            )
            .join(
                m,
                (m["parent_id"] == ps["study_site_id"]) &
                (m["study_id_sk"] == ps["study_id_sk"]) &
                (m["milestone_name"].isin("Qualified", "Site Activation")) &
                (m["milestone_position"] == "Actual"),
                "left"
            )
            # .filter(s["study_id"].isin(study_ids))
            .groupBy(
                s["study_site_id_sk"],
                s["parent_site_number"],
                s["study_id"],
                s["study_id_sk"],
                ps["site_number"],
                ps["site_name"],
                ps["pi_name"],
                ps["pi_phone_number"],
                ps["pi_email"],
                ps["sc_name"],
                ps["sc_phone_number"],
                ps["sc_email"],
                ps["study_site_id_sk"],
                ps["site_type"],
                ps["study_site_status"],
                ps["study_id_sk"],
                ps["icn_region"],
                dc["country_region"],
                dc["country_name"],
                m["parent_id"],
                m["study_id_sk"]
            )
            .agg(
                F.min(
                    F.when(
                        m["milestone_name"] == "Qualified",
                        m["milestone_name"]
                    )
                ).alias("site_qualified_status"),

                F.min(
                    F.when(
                        m["milestone_name"] == "Site Activation",
                        m["milestone_name"]
                    )
                ).alias("site_activation_status"),

                F.min(
                    F.when(
                        m["milestone_name"] == "Qualified",
                        m["milestone_date"]
                    )
                ).alias("site_qualified_date"),

                F.min(
                    F.when(
                        m["milestone_name"] == "Site Activation",
                        m["milestone_date"]
                    )
                ).alias("site_activation_date")
            )
            .select(
                s["study_site_id_sk"].alias("study_site_id_s_sk"),
                s["parent_site_number"],
                s["study_id"],
                s["study_id_sk"],
                ps["site_number"],
                ps["site_name"],
                ps["pi_name"],
                ps["pi_phone_number"],
                ps["pi_email"],
                ps["sc_name"],
                ps["sc_phone_number"],
                ps["sc_email"],
                ps["study_site_id_sk"],
                ps["site_type"],
                ps["study_site_status"],
                ps["study_id_sk"].alias("study_id_ps"),
                ps["icn_region"],
                dc["country_region"].alias("Region"),
                m["parent_id"],
                F.col("site_qualified_status"),
                F.col("site_activation_status"),
                F.col("site_qualified_date"),
                F.col("site_activation_date"),
                m["study_id_sk"].alias("Study_id_m_sk"),
                dc["country_name"]
            )
            .distinct()
        )
        