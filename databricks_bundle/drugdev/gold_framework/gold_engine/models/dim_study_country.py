#src/gold/models/dim_study_country.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from gold_model import GoldModel


class DimStudyCountryModel(GoldModel):
    name         = "dim_study_country"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_study_country.DimStudyCountryModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _common_schema, _silver_schema
        cat    = _quoted_catalog(spark)
        common = _common_schema(spark)
        ctms   = _silver_schema(spark)

        sc_src = spark.table(f"{cat}.{ctms}.ctms_study_country")
        if "_ingestion_timestamp" in sc_src.columns:
            load_date_expr = F.to_date(F.col("_ingestion_timestamp")).alias("load_date")
        else:
            load_date_expr = F.current_date().alias("load_date")

        sc = (sc_src
              
              .select("study_id", F.upper("country_name").alias("country_name"), "study_country_id",
                      "country_status",
                      load_date_expr))

        w = Window.orderBy("country")
        _crm_raw = meta["country_region"]
        # region_full_name is added by ALTER TABLE ADD COLUMNS - fall back to NULL
        # for environments where the column migration has not yet been applied.
        if "region_full_name" not in _crm_raw.columns:
            _crm_raw = _crm_raw.withColumn("region_full_name", F.lit(None).cast("string"))

        crm = (_crm_raw
            #    .withColumn("study_country_id",
            #                (F.lit(1000) + F.dense_rank().over(w)).cast("string"))
               .select(F.col("country").alias("country_name"),
                       "study_country_id",
                       F.col("region").alias("country_region"),
                       F.col("region_full_name")))

        return (sc.join(crm.select("country_name", "country_region", "region_full_name"), "country_name", "inner")
                .select(
                    F.md5(sc["study_id"]).alias("study_id_sk"),
                    F.md5(F.concat(sc["study_country_id"], sc["study_id"])).alias("study_country_id_sk"),
                    sc["study_id"], sc["study_country_id"], sc["country_name"],
                    sc["country_status"],
                    crm["country_region"].cast("string"),
                    crm["region_full_name"].cast("string"),
                    sc["load_date"],
                    F.current_date().alias("aud_created_date"),
                    F.current_date().alias("aud_updated_date"),
                    F.lit(True).alias("is_active_bi_study"),
                ))
