# src/gold/models/fct_queries.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class FctQueriesModel(GoldModel):
    name         = "fct_queries"
    write_mode   = "overwrite"
    dependencies = ["dim_queries"]
    class_path   = "gold.models.fct_queries.FctQueriesModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat  = _quoted_catalog(spark)
        gold = _gold_schema(spark)

        dq  = spark.table(f"{cat}.{gold}.dim_queries")
        oid = F.col("opened_date_id")
        cid = F.col("closed_date_id")
        prev = F.date_sub(F.current_date(), 1)

        return (dq.select(
            "query_id_sk", "query_key",
            F.md5(F.substring(F.col("subject_number"), -4, 4)).alias("participant_id_sk"),
            F.md5(F.concat(F.col("study_country_id"), F.col("study_id"))).alias("study_country_id_sk"),
            F.md5(F.concat(F.col("study_id"), F.substring(F.col("site"), 1, 4))).alias("study_site_id_sk"),
            F.md5(F.col("study_id")).alias("study_id_sk"),
            "query_status", "visit_name", "page_name", "page_link",
            "field_oid", "response", "issued_by_name", "query_recipient",
            "opened_date_id", "answered_date_id", "answered_by",
            "cancelled_date_id", "cancelled_by",
            "closed_date_id", "closed_by",
            "last_opened_query_text", "load_date",
            F.when(F.datediff(F.current_date(), oid) > 30, oid).alias("calc_q_30"),
            F.when(F.datediff(prev, oid) == 31, oid).alias("opn_31"),
            F.when(F.datediff(prev, oid) == 60, oid).alias("opn_60"),
            F.when(F.col("query_status") == "Opened", F.col("query_id_sk")).alias("open_queries"),
            F.when(F.datediff(prev, oid) == 61, oid).alias("opn_61"),
            F.when(F.datediff(prev, oid) == 90, oid).alias("opn_90"),
            F.when(F.datediff(prev, oid) == 91, oid).alias("opn_91"),
            F.when((F.datediff(F.current_date(), oid) > 60) & (F.datediff(F.current_date(), oid) <= 90), oid).alias("query_dates"),
            F.datediff(cid, oid).alias("days_test"),
        ))
