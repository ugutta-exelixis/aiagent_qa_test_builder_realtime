# src/gold/models/fct_study_site.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from gold_model import GoldModel


class FctStudySiteModel(GoldModel):
    name         = "fct_study_site"
    write_mode   = "overwrite"
    dependencies = []
    tags         = {"mart": "fct_study_site.sql"}
    class_path   = "gold.models.fct_study_site.FctStudySiteModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        ss = spark.table(f"{cat}.{ctms}.ctms_study_site")
        if "_ingestion_timestamp" in ss.columns:
            load_date_expr = F.to_date(F.col("_ingestion_timestamp")).alias("load_date")
        else:
            load_date_expr = F.current_date().alias("load_date")

        # Derive site_id: strip hyphen-prefixed routing number
        site_id = (
            F.when(F.instr(F.col("site_number"), "-") > 0,
                   F.when(F.length(F.col("site_number")) == 9,
                          F.split(F.col("site_number"), "-")[0])
                    .otherwise(F.split(F.col("site_number"), "-")[1]))
             .otherwise(F.col("site_number")))

        # alt_study_site_id: keep only numeric-looking values
        alt_id = F.when(
            F.col("parent_site").substr(1, 4).rlike(r"^[+-]?[0-9]+(\.[0-9]*)?([Ee][+-]?[0-9]+)?$"),
            F.col("parent_site").substr(1, 4)
        )

        base_df = (ss
            .withColumn("site_id", site_id)
            .select(
                F.md5("study_id").alias("study_id_sk"),
                F.md5(F.concat("study_id", "site_id")).alias("study_site_id_sk"),
                F.md5(F.concat("study_country_id", "study_id")).alias("study_country_id_sk"),
                F.md5("pi_address_id").alias("address_id_sk"),
                F.md5("pi_contact_id").alias("contact_id_sk"),
                F.md5("account_id").alias("account_id_sk"),
                F.md5("study_site_id").alias("study_site_id_doc_id_sk"),
                F.col("protocol_version").alias("protocol_version_number_sk"),
                F.lit(None).cast("string").alias("sub_prot_rdy_date"),
                F.lit(None).cast("string").alias("site_portal_access"),
                F.lit(None).cast("string").alias("last_comm_date"),
                F.lit(None).cast("string").alias("last_comm_type"),
                F.lit(None).cast("long").alias("days_working"),
                F.col("parent_site").alias("parent_site_number"),
                alt_id.alias("alt_study_site_id"),
                F.lit(None).cast("string").alias("nci"),
                load_date_expr,
                F.col("study_site_status"),
                # extra pass-through columns used by downstream gold views
                F.col("study_id"),
                F.col("study_country_id"),
                F.col("site_number"),
                F.lit(None).cast("string").alias("site_name"),
                F.lit(None).cast("string").alias("pi_name"),
                F.lit(None).cast("string").alias("pi_phone_number"),
                F.lit(None).cast("string").alias("pi_email"),
                F.lit(None).cast("string").alias("sc_name"),
                F.lit(None).cast("string").alias("sc_phone_number"),
                F.lit(None).cast("string").alias("sc_email"),
                F.col("site_type"),
                F.col("study_site_id"),
                F.col("icn_region"),
                F.col("parent_site").substr(1, 4).alias("parent_site_number_4"),
            ).distinct())

        # Join site_status_master → site_status_normalized + group_name → boolean site flags
        site_master = meta.get("site_status_master")
        if site_master is not None:
            sm = site_master.select(F.col("ctms_status"), F.col("site_status_normalized"), F.col("group_name"))
            base_df = (base_df
                .join(sm, base_df["study_site_status"] == sm["ctms_status"], "left")
                .drop("ctms_status")
                .withColumn("is_active_site",   F.col("group_name") == "ACTIVE")
                .withColumn("is_enrolling_site", F.col("group_name") == "ENROLLING")
                .withColumn("is_planned_site",   F.col("group_name") == "PLANNED")
                .drop("group_name"))
        else:
            base_df = base_df.withColumn("site_status_normalized", F.lit(None).cast("string"))
            for c in ("is_active_site", "is_enrolling_site", "is_planned_site"):
                base_df = base_df.withColumn(c, F.lit(None).cast("boolean"))

        return base_df
