# src/gold/models/dim_study_account.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class DimStudyAccountModel(GoldModel):
    name         = "dim_study_account"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_study_account.DimStudyAccountModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        aa_src = spark.table(f"{cat}.{ctms}.ctms_account_association")
        if "_ingestion_timestamp" in aa_src.columns:
            load_date_expr = F.to_date(F.col("_ingestion_timestamp")).alias("load_date")
        else:
            load_date_expr = F.current_date().alias("load_date")

        aa = (aa_src
              
              .select("study_id",
                      F.col("associated_record_id").alias("study_site_id"),
                      "account_id",
                      load_date_expr))

        acc = (spark.table(f"{cat}.{ctms}.ctms_account")
               
               .select("account_id", "name"))

        return (aa.join(acc, "account_id", "inner")
                .select(
                    F.md5(aa["study_id"]).alias("study_id_sk"),
                    F.md5(acc["account_id"]).alias("account_id_sk"),
                    acc["account_id"], aa["study_site_id"], acc["name"],
                    aa["load_date"],
                    F.current_timestamp().alias("aud_created_date"),
                    F.current_timestamp().alias("aud_updated_date"),
                ).distinct())
