# src/gold/models/dim_site_address.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from gold_model import GoldModel


class DimSiteAddressModel(GoldModel):
    name         = "dim_site_address"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_site_address.DimSiteAddressModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        aa = (spark.table(f"{cat}.{ctms}.ctms_address_association")
              
              .select("study_id", "address_id",
                      F.col("associated_record_id"),
                      F.col("type").alias("address_type")))
        c  = spark.table(f"{cat}.{ctms}.ctms_address").select(
            "address_id",
            F.concat_ws(" ", "line1", "line2", "line3").alias("site_address"),
            "city", "state_province", "postal_code", "country")

        w = Window.partitionBy("associated_record_id").orderBy(
            F.when(F.col("address_type") == "Physical Address", 1).otherwise(2),
            F.col("address_type"))

        return (aa.join(c, "address_id", "left")
                .withColumn("rnk", F.row_number().over(w))
                .filter("rnk = 1")
                .select("study_id", "address_id", "associated_record_id",
                        "address_type", "site_address", "city",
                        "state_province", "postal_code", "country"))
