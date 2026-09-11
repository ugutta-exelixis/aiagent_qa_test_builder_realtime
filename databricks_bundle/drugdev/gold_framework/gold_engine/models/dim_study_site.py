from pyspark.sql import DataFrame, SparkSession, Window
from typing import Dict
from pyspark.sql import functions as F

from gold_model import GoldModel


class DimStudySiteModel(GoldModel):
    name = "dim_study_site"
    write_mode = "overwrite"
    dependencies = []
    class_path = "gold.models.dim_study_site.DimStudySiteModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _common_schema, _silver_schema

        cat    = _quoted_catalog(spark)
        common = _common_schema(spark)
        ctms   = _silver_schema(spark)
        # ------------------------------------------------------------------
        # Source tables
        # ------------------------------------------------------------------
        study_site_src = spark.table(f"{cat}.{ctms}.ctms_study_site")
        account_src = spark.table(f"{cat}.{ctms}.ctms_account")
        contact_src = spark.table(f"{cat}.{ctms}.ctms_contact")
        contact_assoc_src = spark.table(f"{cat}.{ctms}.ctms_contact_association")
        study_site_phase_src = spark.table(f"{cat}.{ctms}.ctms_study_site_phase")
        site_contact_details_phase_src = spark.table(
            f"{cat}.{ctms}.ctms_site_contact_details"
        )

        # ------------------------------------------------------------------
        # study_site
        # ------------------------------------------------------------------
        study_site = (
            study_site_src
            .select(
                F.col("study_id").alias("study_id"),
                F.md5(F.col("study_id")).alias("study_id_sk"),
                "study_site_id",
                F.substring("site_number", 1, 4).alias("site_id"),
                F.md5(
                    F.concat(
                        F.col("study_id"),
                        F.substring("site_number", 1, 4)
                    )
                ).alias("study_site_id_sk"),
                F.col("parent_site").alias("parent_site_number"),
                F.substring("parent_site", 1, 4).alias("alt_study_site_id"),
                F.substring("site_number", 1, 4).alias("site_number"),
                "study_country_id",
                "site_type",
                "account_id",
                "pi_contact_id",
                # "site_priority",
                # "referral_source",
                "icn_region",
                "study_site_status",
                "site_status_reason",
                # F.to_date("load_date").alias("load_date"),
            )
            .distinct()
        )

        # ------------------------------------------------------------------
        # account
        # ------------------------------------------------------------------
        account = (
            account_src
            .select(
                "account_id",
                F.col("name").alias("site_name")
            )
            .distinct()
        )

        # ------------------------------------------------------------------
        # contact
        # ------------------------------------------------------------------
        contact = (
            contact_src
            .select(
                "contact_id",
                "first_name",
                "last_name",
                F.concat_ws(" ", "first_name", "last_name").alias("role_name"),
                F.col("phone_number").alias("role_phone_number"),
                F.col("email").alias("role_email"),
            )
            .distinct()
        )

        # ------------------------------------------------------------------
        # PI Contact
        # ------------------------------------------------------------------
        pi_window = Window.partitionBy(
            F.concat(
                F.col("study_id"),
                F.col("associated_record_id")
            )
        ).orderBy(F.to_date("start_date").desc())

        pi_contact = (
            contact_assoc_src
            .filter(
                (F.col("role") == "Principal Investigator")
                & (F.col("start_date") != "00-000-0000")
            )
            .withColumn("ca_rank", F.rank().over(pi_window))
            .filter(F.col("ca_rank") == 1)
            .groupBy(
                "associated_record_id",
                "study_id"
            )
            .agg(
                F.max("contact_id").alias("pi_contact_id")
            )
            .withColumn(
                "study_id",
                F.col("study_id")
            )
        )

        final_pi_contact = (
            pi_contact.alias("pc")
            .join(
                contact.alias("c"),
                F.col("pc.pi_contact_id") == F.col("c.contact_id"),
                "left"
            )
            .select(
                F.col("pc.study_id"),
                F.col("c.contact_id"),
                F.col("c.role_name").alias("pi_name"),
                F.col("c.role_phone_number").alias("pi_phone_number"),
                F.col("c.role_email").alias("pi_email"),
                F.col("pc.associated_record_id"),
                F.col("pc.study_id"),
                F.col("pc.pi_contact_id")
            )
            .distinct()
        )

        # ------------------------------------------------------------------
        # Study Coordinator Contact
        # ------------------------------------------------------------------
        sc_window = Window.partitionBy(
            F.concat(
                F.col("study_id"),
                F.col("associated_record_id")
            )
        ).orderBy(F.to_date("start_date").desc())

        sc_contact = (
            contact_assoc_src
            .filter(F.col("role") == "Study Coordinator")
            .withColumn("ca_rank", F.rank().over(sc_window))
            .filter(F.col("ca_rank") == 1)
            .groupBy(
                "associated_record_id",
                "study_id"
            )
            .agg(
                F.max("contact_id").alias("sc_contact_id")
            )
            .withColumn(
                "study_id",
                F.col("study_id")
            )
        )

        final_sc_contact = (
            sc_contact.alias("sc")
            .join(
                contact.alias("c"),
                F.col("sc.sc_contact_id") == F.col("c.contact_id"),
                "left"
            )
            .select(
                F.col("sc.study_id"),
                F.col("c.contact_id"),
                F.col("c.role_name").alias("sc_name"),
                F.col("c.role_phone_number").alias("sc_phone_number"),
                F.col("c.role_email").alias("sc_email"),
                F.col("sc.associated_record_id"),
                F.col("sc.study_id"),
                F.col("sc.sc_contact_id")
            )
            .distinct()
        )

        # ------------------------------------------------------------------
        # dim_study_site
        # ------------------------------------------------------------------
        numeric_regex = r'^[+-]?[0-9]+(\.[0-9]*)?([Ee][+-]?[0-9]+)?$'

        dim_study_site = (
            study_site.alias("ss")
            .join(
                account.alias("a"),
                "account_id",
                "left"
            )
            .join(
                final_pi_contact.alias("pc"),
                (
                    (F.col("pc.study_id") == F.col("ss.study_id"))
                    & (
                        F.col("pc.associated_record_id")
                        == F.col("ss.study_site_id")
                    )
                ),
                "left"
            )
            .join(
                final_sc_contact.alias("sc"),
                (
                    (F.col("sc.study_id") == F.col("ss.study_id"))
                    & (
                        F.col("sc.associated_record_id")
                        == F.col("ss.study_site_id")
                    )
                ),
                "left"
            )
            .select(
                F.col("ss.study_id_sk"),
                F.col("ss.study_site_id_sk"),
                F.col("ss.study_id"),
                F.col("ss.study_site_id"),
                F.col("ss.study_country_id"),
                F.when(
                    F.col("ss.alt_study_site_id").rlike(numeric_regex),
                    F.col("ss.alt_study_site_id")
                ).otherwise(F.lit(None)).alias("alt_study_site_id"),
                F.col("ss.parent_site_number"),
                F.col("ss.site_number"),
                F.col("ss.site_type"),
                # F.col("ss.site_priority"),
                # F.col("ss.referral_source"),
                F.col("ss.icn_region"),
                F.col("ss.study_site_status"),
                F.col("ss.site_status_reason"),
                # F.col("ss.load_date"),
                F.col("a.site_name"),
                F.col("pc.pi_name"),
                F.col("pc.pi_phone_number"),
                F.col("pc.pi_email"),
                F.col("sc.sc_name"),
                F.col("sc.sc_phone_number"),
                F.col("sc.sc_email"),
            )
        )

        # ------------------------------------------------------------------
        # dim_study_site_phase
        # ------------------------------------------------------------------
        dim_study_site_phase = (
            study_site_phase_src.alias("ss")
            .join(
                site_contact_details_phase_src.alias("sc"),
                F.md5(
                    F.concat(
                        F.col("ss.study_id"),
                        F.col("ss.site_number")
                    )
                ) == F.col("sc.study_site_id_sk"),
                "left"
            )
            .select(
                F.md5(F.col("ss.study_id")).alias("study_id_sk"),
                F.md5(
                    F.concat(
                        F.col("ss.study_id"),
                        F.col("ss.site_number")
                    )
                ).alias("study_site_id_sk"),
                F.col("ss.study_id"),
                F.col("ss.site_number").alias("study_site_id"),
                F.col("ss.study_country_id"),
                F.lit(None).alias("alt_study_site_id"),
                F.col("ss.parent_site_number"),
                F.col("ss.site_number"),
                F.when(
                    (F.col("ss.study_id") == "XL309-101")
                    & (
                        F.col("ss.site_number")
                        != F.col("ss.parent_site_number")
                    ),
                    "Satellite"
                )
                .otherwise("Main")
                .alias("site_type"),
                # F.lit(None).alias("site_priority"),
                # F.lit(None).alias("referral_source"),
                F.col("ss.region").alias("icn_region"),
                F.col("ss.study_site_status"),
                F.lit(None).alias("site_status_reason"),
                # F.to_date("ss.load_date").alias("load_date"),
                F.col("ss.site_name"),
                F.col("sc.pi_name"),
                F.col("sc.pi_phone").alias("pi_phone_number"),
                F.col("sc.pi_email"),
                F.col("sc.sc_name"),
                F.col("sc.sc_phone").alias("sc_phone_number"),
                F.col("sc.sc_email"),
            )
            .distinct()
        )

        # ------------------------------------------------------------------
        # Final Union
        # ------------------------------------------------------------------
        final_df = dim_study_site.unionByName(dim_study_site_phase)

        return final_df
