# src/gold/models/dim_study_contact.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class DimStudyContactModel(GoldModel):
    name         = "dim_study_contact"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_study_contact.DimStudyContactModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        ca_src = spark.table(f"{cat}.{ctms}.ctms_contact_association")
        if "_ingestion_timestamp" in ca_src.columns:
            load_date_expr = F.to_date(F.col("_ingestion_timestamp")).alias("load_date")
        else:
            load_date_expr = F.current_date().alias("load_date")

        ca = (ca_src
              
              .select("study_id", F.col("associated_record_id").alias("study_site_id"),
                      "contact_id", load_date_expr))

        cd = (spark.table(f"{cat}.{ctms}.ctms_contact")
              
              .select("contact_id", "first_name", "last_name",
                      F.concat("first_name", F.lit(","), "last_name").alias("full_name"),
                      "phone_number", "fax_number", "email", "mid_name", "title", "degree",
                      "inv_flag", "cell_number", "home_number", "pager_number", "pager_pin",
                      "alt_phone_number", "email_2", "email_3"))

        return (ca.join(cd, "contact_id", "inner")
                .select(
                    F.md5(ca["study_id"]).alias("study_id_sk"),
                    F.md5(cd["contact_id"]).alias("contact_id_sk"),
                    cd["contact_id"].cast("string"),
                    ca["study_site_id"],
                    cd["first_name"].cast("string"),
                    cd["last_name"].cast("string"),
                    cd["full_name"].cast("string"),
                    cd["phone_number"].cast("string"),
                    cd["fax_number"].cast("string"),
                    cd["email"].cast("string"),
                    cd["mid_name"].cast("string"),
                    cd["title"].cast("string"),
                    cd["degree"].cast("string"),
                    cd["inv_flag"].cast("boolean"),
                    cd["cell_number"].cast("string"),
                    cd["home_number"].cast("string"),
                    cd["pager_number"].cast("string"),
                    cd["pager_pin"].cast("string"),
                    cd["alt_phone_number"].cast("string"),
                    cd["email_2"].cast("string"),
                    cd["email_3"].cast("string"),
                    ca["load_date"],
                    F.current_date().alias("aud_created_date"),
                    F.current_date().alias("aud_updated_date"),
                ).distinct())
