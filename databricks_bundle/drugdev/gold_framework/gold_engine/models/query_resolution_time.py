# src/gold/models/query_resolution_time.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class QueryResolutionTimeModel(GoldModel):
    name         = "query_resolution_time"
    write_mode   = "overwrite"
    dependencies = ["dim_queries", "dim_study_site"]
    class_path   = "gold.models.query_resolution_time.QueryResolutionTimeModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        dq = spark.table(f"{gold}.dim_queries").alias("dq")
        dss = spark.table(f"{gold}.dim_study_site").alias("dss")

        site_number_expr = F.trim(F.split(F.col("dq.site"), "-").getItem(0))
        opened_to_answered_expr = F.col("dq.days_open_to_answered").cast("double")

        query_data = (
            dq.select(
                site_number_expr.alias("site_number"),
                F.col("dq.studyid").alias("studyid"),
                opened_to_answered_expr.alias("opened_to_answered"),
            )
        )

        otad = (
            query_data.alias("q")
            .join(
                dss.select("study_id", "site_name", "site_number").alias("s"),
                (F.trim(F.col("s.site_number")) == F.trim(F.col("q.site_number")))
                & (F.col("s.study_id") == F.col("q.studyid")),
                "inner",
            )
            .groupBy(
                F.col("q.site_number").alias("site_number"),
                F.col("s.site_name").alias("site_name"),
                F.col("s.study_id").alias("protocol"),
            )
            .agg(F.sum(F.col("q.opened_to_answered")).alias("opened_to_answered_days"))
        )

        rsq = (
            dq.select(
                site_number_expr.alias("site_number"),
                F.col("dq.studyid").alias("studyid"),
                F.split(F.col("dq.site"), " - ").getItem(1).alias("query_table_site_name"),
                F.col("dq.query_status"),
                F.col("dq.query_id_sk"),
            )
            .filter(F.col("query_status") .isin("Closed", "Answered"))
            .groupBy("site_number", "studyid", "query_table_site_name")
            .agg(F.count(F.lit(1)).alias("resolved_queries"))
        )

        return (
            otad.alias("otad")
            .join(
                rsq.alias("rsq"),
                (F.col("otad.site_number") == F.col("rsq.site_number"))
                & (F.col("otad.protocol") == F.col("rsq.studyid")),
                "left",
            )
            .select(
                F.col("otad.site_number"),
                F.col("otad.site_name"),
                F.col("otad.protocol"),
                F.col("otad.opened_to_answered_days"),
                F.col("rsq.resolved_queries"),
            )
        )