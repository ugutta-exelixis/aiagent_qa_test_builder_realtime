# src/gold/models/site_info.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from gold_model import GoldModel


class SiteInfoModel(GoldModel):
    name         = "site_info"
    write_mode   = "overwrite"
    dependencies = ["dim_site_address", "dim_study_site", "dim_study_country", "fct_site_milestone"]
    class_path   = "gold.models.site_info.SiteInfoModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _bronze_schema, _quoted_catalog, _gold_schema
        # from utils.config_loader import _quoted_catalog, _gold_schema, _silver_schema

        def _quoted_source_col(df: DataFrame, *candidates: str):
            for candidate in candidates:
                if candidate in df.columns:
                    return F.col(f"`{candidate}`")
            raise KeyError(f"None of the candidate columns exist: {candidates}")

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"
        bronze = _bronze_schema(spark)
        # ctms = _silver_schema(spark)

        address_info = (
            spark.table(f"{gold}.dim_site_address")
            .select(
                "study_id",
                "address_id",
                "associated_record_id",
                "address_type",
                "site_address",
                "city",
                "state_province",
                "postal_code",
                "country",
            )
        )

        raw_site_address_src = spark.table(f"{cat}.{bronze}.lkp_sites_address_master")
        raw_site_address = (
            raw_site_address_src
            .select(
                _quoted_source_col(raw_site_address_src, "Site Number").alias("raw_site_number"),
                _quoted_source_col(raw_site_address_src, "Institution name").alias("raw_site_name"),
                _quoted_source_col(raw_site_address_src, "Address15").alias("raw_site_address"),
                _quoted_source_col(raw_site_address_src, "Zip code").alias("raw_zip_code"),
                _quoted_source_col(raw_site_address_src, "City").alias("raw_city"),
                _quoted_source_col(raw_site_address_src, "Country ", "Country").alias("raw_country"),
                _quoted_source_col(raw_site_address_src, "State").alias("raw_state"),
            )
        )

        raw_site_address = raw_site_address.withColumn(
            "rnk",
            F.row_number().over(
                Window.partitionBy("raw_site_number").orderBy(
                    F.when(F.length(F.trim(F.col("raw_site_address"))) > 0, F.lit(1)).otherwise(F.lit(2))
                )
            ),
        ).filter(F.col("rnk") == 1)

        site_base = spark.table(f"{gold}.dim_study_site")
        parent_site = site_base.alias("ps")
        study_site = site_base.alias("s")
        country = spark.table(f"{gold}.dim_study_country").alias("dc")
        milestones = spark.table(f"{gold}.fct_site_milestone").alias("m")

        site_info = (
            study_site
            .join(
                parent_site,
                (F.col("s.parent_site_number") == F.col("ps.site_number")) & (F.col("s.study_id") == F.col("ps.study_id")),
                "inner",
            )
            .join(
                country,
                (F.col("s.study_country_id") == F.col("dc.study_country_id")) & (F.col("s.study_id") == F.col("dc.study_id")),
                "inner",
            )
            .join(
                milestones,
                (F.col("m.parent_id") == F.col("ps.study_site_id"))
                & (F.col("m.study_id_sk") == F.col("ps.study_id_sk"))
                & (F.col("m.milestone_name").isin("Qualified", "Site Activation"))
                & (F.col("m.milestone_position") == F.lit("Actual")),
                "left",
            )
            .filter(F.col("ps.study_id_sk") == F.col("dc.study_id_sk"))
            .select(
                F.col("s.parent_site_number"),
                F.col("s.study_site_id_sk"),
                F.col("s.study_id"),
                F.col("s.study_id_sk"),
                F.col("ps.study_site_id"),
                F.col("ps.site_number"),
                F.col("ps.main_site_name").alias("site_name"),
                F.col("ps.pi_name"),
                F.col("ps.pi_phone_number"),
                F.col("ps.pi_email"),
                F.col("ps.sc_name"),
                F.col("ps.sc_phone_number"),
                F.col("ps.sc_email"),
                F.col("ps.site_type"),
                F.when(F.col("ps.study_site_status") == F.lit("Enrollment open"), F.lit("Enrollment Open")).otherwise(F.col("ps.study_site_status")).alias("study_site_status"),
                F.col("ps.icn_region"),
                F.col("dc.country_name"),
                F.col("m.milestone_name"),
                F.col("m.milestone_position"),
                F.col("m.milestone_date").alias("site_activation_date"),
            )
            .distinct()
        )

        address_master = (
            site_info.alias("si")
            .join(
                address_info.alias("ai"),
                (F.col("ai.study_id") == F.col("si.study_id"))
                & (F.col("ai.associated_record_id") == F.col("si.study_site_id"))
                & (F.col("ai.country") == F.col("si.country_name")),
                "left",
            )
            .join(
                raw_site_address.alias("rs"),
                F.col("rs.raw_site_number") == F.col("si.site_number"),
                "left",
            )
            .select(
                F.col("si.study_site_id_sk"),
                F.col("si.study_site_id"),
                F.col("si.parent_site_number"),
                F.col("si.study_id"),
                F.col("si.study_id_sk"),
                F.col("si.site_number"),
                F.col("si.site_name"),
                F.col("si.pi_name"),
                F.col("si.pi_phone_number"),
                F.col("si.pi_email"),
                F.col("si.sc_name"),
                F.col("si.sc_phone_number"),
                F.col("si.sc_email"),
                F.col("si.site_type"),
                F.col("si.study_site_status"),
                F.col("si.icn_region").alias("region"),
                F.col("ai.address_id"),
                F.col("ai.associated_record_id"),
                F.col("ai.address_type"),
                F.when(F.col("ai.site_address").isNull() & F.col("rs.raw_site_address").isNull(), F.lit(" "))
                .when(F.col("ai.site_address").isNull() & F.col("rs.raw_site_address").isNotNull(), F.col("rs.raw_site_address"))
                .otherwise(F.col("ai.site_address")).alias("Address"),
                F.when(F.col("ai.city").isNull() & F.col("rs.raw_city").isNull(), F.lit(" "))
                .when(F.col("ai.city").isNull() & F.col("rs.raw_city").isNotNull(), F.col("rs.raw_city"))
                .otherwise(F.col("ai.city")).alias("city"),
                F.when(F.col("ai.state_province").isNull() & F.col("rs.raw_state").isNull(), F.lit(" "))
                .when(F.col("ai.state_province").isNull() & F.col("rs.raw_state").isNotNull(), F.col("rs.raw_state"))
                .otherwise(F.col("ai.state_province")).alias("state_province"),
                F.when(F.col("ai.postal_code").isNull() & F.col("rs.raw_zip_code").isNull(), F.lit(" "))
                .when(F.col("ai.postal_code").isNull() & F.col("rs.raw_zip_code").isNotNull(), F.col("rs.raw_zip_code"))
                .otherwise(F.col("ai.postal_code")).alias("postal_code"),
                F.col("si.country_name"),
                F.col("si.milestone_name"),
                F.col("si.milestone_position"),
                F.col("si.site_activation_date"),
            )
            .distinct()
        )

        allowed_statuses = [
            "Enrollment Open",
            "Enrollment On-Hold",
            "Site Closed",
            "Active Enrolling",
            "Qualified",
            "Planned",
            "Enrollment Closed",
            "Approved",
            "Submitted",
            "On Hold",
            "Close Out Ready",
            "Selected",
            "Activated",
            "Closed",
            "Initiated",
        ]

        return (
            address_master
            .filter(F.col("study_site_status").isin(allowed_statuses))
            .filter(F.col("site_name").isNotNull())
            .select(
                "study_site_id_sk",
                "study_site_id",
                "parent_site_number",
                "study_id",
                "study_id_sk",
                "site_number",
                "site_name",
                "pi_name",
                "pi_phone_number",
                "pi_email",
                "sc_name",
                "sc_phone_number",
                "sc_email",
                "site_type",
                "study_site_status",
                "region",
                "address_id",
                "associated_record_id",
                "address_type",
                "Address",
                "city",
                "state_province",
                "postal_code",
                "country_name",
                "milestone_name",
                "milestone_position",
                F.col("site_activation_date").alias("site activation date"),
            )
            .distinct()
        )