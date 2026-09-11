# src/gold/models/dim_study_address.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class DimStudyAddressModel(GoldModel):
    name         = "dim_study_address"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_study_address.DimStudyAddressModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        aa = (spark.table(f"{cat}.{ctms}.ctms_address_association")
              
              .select("study_id", F.col("associated_record_id").alias("study_site_id"), "address_id")
              .distinct())

        addr_src = spark.table(f"{cat}.{ctms}.ctms_address")
        if "_ingestion_timestamp" in addr_src.columns:
            load_date_expr = F.to_date(F.col("_ingestion_timestamp")).alias("load_date")
        else:
            load_date_expr = F.current_date().alias("load_date")

        addr = (addr_src
                .select("address_id", "line1", "line2", "line3", "city",
                        "state_province", "postal_code", "country",
                        load_date_expr)
                .distinct())

        return (aa.join(addr, "address_id", "inner")
                .select(
                    F.md5(aa["study_id"]).alias("study_id_sk"),
                    F.md5(addr["address_id"]).alias("address_id_sk"),
                    addr["address_id"].cast("string"),
                    aa["study_site_id"],
                    addr["line1"].cast("string"),
                    addr["line2"].cast("string"),
                    addr["line3"].cast("string"),
                    addr["city"].cast("string"),
                    addr["state_province"].cast("string"),
                    addr["postal_code"].cast("string"),
                    addr["country"].cast("string"),
                    addr["load_date"],
                    F.current_date().alias("aud_created_date"),
                    F.current_date().alias("aud_updated_date"),
                ).distinct())
