# src/gold/models/dim_queries.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class DimQueriesModel(GoldModel):
    name         = "dim_queries"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_queries.DimQueriesModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        qd = spark.table(f"{cat}.{ctms}.edc_query_details")
        if "_ingestion_timestamp" in qd.columns:
            qd_load_date_expr = F.to_date(F.col("_ingestion_timestamp")).alias("load_date")
        else:
            qd_load_date_expr = F.current_date().alias("load_date")

        sc = (spark.table(f"{cat}.{ctms}.ctms_study_country")
              
              .select("study_id", F.col("country_name"), "study_country_id"))

        prev_day = F.date_sub(F.current_date(), 1)

        opened_dt  = F.when(F.col("opened_date").isNotNull(),  F.to_date("opened_date",  "ddMMMyyyy"))
        answered_dt = F.when(F.col("answered_date").isNotNull(), F.to_date("answered_date", "ddMMMyyyy"))
        cancelled_dt = F.when(F.col("cancelled_date").isNotNull(), F.to_date("cancelled_date", "ddMMMyyyy"))
        closed_dt  = F.when(F.col("closed_date").isNotNull(),  F.to_date("closed_date",  "ddMMMyyyy"))

        joined = (qd.join(sc, (qd["study_id"] == sc["study_id"]) & (qd["country"] == sc["country_name"]), "left")
                  .withColumn("opened_date_id",    opened_dt)
                  .withColumn("answered_date_id",  answered_dt)
                  .withColumn("cancelled_date_id", cancelled_dt)
                  .withColumn("closed_date_id",    closed_dt))

        return (joined.select(
            F.expr("cast(uuid() as string)").alias("query_id_sk"),
            sc["study_country_id"],
            F.md5(qd["query_id"]).alias("query_key"),
            qd["query_id"], qd["subject_number"], qd["study_id"], qd["country"], qd["site"],
            qd["query_status"], qd["visit_name"], qd["page_name"], qd["page_link"], qd["field_oid"],
            qd["query_response"].alias("response"),
            qd["issued_by_name"], qd["query_recipient"],
            qd["days_open_to_closed"], qd["days_open_to_answered"],
            F.col("opened_date_id"), F.col("answered_date_id"),
            qd["answered_by"],
            F.col("cancelled_date_id"), qd["cancelled_by"],
            F.col("closed_date_id"),   qd["closed_by"],
            qd["last_opened_query_text"],
            qd_load_date_expr,
            F.lit(True).alias("is_active_bi_study"),
            F.when((F.datediff(prev_day, F.col("opened_date_id")) > 30) &
                   (F.datediff(prev_day, F.col("opened_date_id")) <= 60), F.lit("31-60")).alias("31_60"),
            F.when((F.datediff(prev_day, F.col("opened_date_id")) > 60) &
                   (F.datediff(prev_day, F.col("opened_date_id")) <= 90), F.lit("61-90")).alias("61_90"),
            F.when(F.datediff(prev_day, F.col("opened_date_id")) > 90, F.lit("91+")).alias("91_days_filter"),
            F.col("opened_date_id").alias("open_date"),
            F.col("closed_date_id").alias("closed_date"),
            qd["query_recipient"].alias("query_recipient_copy"),
            qd["query_status"].alias("query_status_copy"),
            F.datediff(F.current_date(), F.col("opened_date_id")).alias("days_open"),
            F.datediff(F.col("closed_date_id"), F.col("opened_date_id")).alias("days_taken_to_close"),
        ))
