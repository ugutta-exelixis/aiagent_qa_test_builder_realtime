# src/gold/models/open_queries.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class OpenQueriesModel(GoldModel):
    name         = "open_queries"
    write_mode   = "overwrite"
    dependencies = ["dim_queries", "dim_study_site"]
    class_path   = "gold.models.open_queries.OpenQueriesModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        dq = spark.table(f"{gold}.dim_queries").alias("dq")
        dss = spark.table(f"{gold}.dim_study_site").alias("dss")

        open_queries = (
            dq.join(
                dss,
                (F.col("dq.study_id") == F.col("dss.study_id"))
                & (F.split(F.col("dq.site"), " ").getItem(0) == F.col("dss.site_number")),
                "left",
            )
            .filter(F.col("dq.query_status") == F.lit("Opened"))
            .filter(F.col("dss.site_type") == F.lit("Main"))
            .groupBy(
                F.col("dq.study_id").alias("studyid"),
                F.col("dq.site").alias("site"),
                F.col("dss.site_name").alias("site_name"),
                F.col("dss.site_number").alias("site_number"),
            )
            .agg(F.countDistinct(F.col("dq.query_id_sk")).alias("open_queries"))
        )

        return open_queries.select(
            "open_queries",
            "studyid",
            "site",
            "site_name",
            "site_number",
        )