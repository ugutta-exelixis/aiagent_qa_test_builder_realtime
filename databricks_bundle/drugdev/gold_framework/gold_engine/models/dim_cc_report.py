# src/gold/models/dim_cc_report.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from gold_model import GoldModel


class DimCcReportModel(GoldModel):
    name         = "dim_cc_report"
    write_mode   = "overwrite"
    dependencies = ["dim_planisware_milestones"]
    class_path   = "gold.models.dim_cc_report.DimCcReportModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        pm = spark.table(f"{cat}.{ctms}.planisware_milestones")
        valid_proto = F.col("protocol_number").rlike("^X[A-Z][0-9]{3}-[0-9]{3}$")

        max_load = pm.agg(F.max("load_date")).first()[0]
        w_rn = Window.orderBy(F.desc("load_date"))
        prev_load_row = (pm.select("load_date").distinct()
                 .withColumn("_rn", F.row_number().over(w_rn))
                 .filter("_rn = 2").first())
        prev_load = prev_load_row["load_date"] if prev_load_row is not None else None

        today = (pm.filter((F.col("load_date") == max_load) & (F.col("status") == "ok") & valid_proto)
                 .select("activity_type_actual", "protocol_number", "study_phase_norm",
                         F.col("planned_finish").alias("today_planned_date"),
                         F.col("actual_finish").alias("today_actual_date"),
                         F.col("load_date").alias("today_load"),
                         "protocol_activity_type_actual_sk"))

        yest = (pm.filter((F.col("load_date") == prev_load) & (F.col("status") == "ok") & valid_proto)
                .select("activity_type_actual", "protocol_number", "study_phase_norm",
                        F.col("planned_finish").alias("yesterday_planned_date"),
                        F.col("actual_finish").alias("yesterday_actual_date"),
                        F.col("load_date").alias("yesterday_load"),
                        "protocol_activity_type_actual_sk"))

        # First snapshot: emit current rows with null yesterday fields.
        if prev_load is None:
            return (today
                .select(
                    "activity_type_actual",
                    "protocol_number",
                    "study_phase_norm",
                    "today_planned_date",
                    F.lit(None).cast(today.schema["today_planned_date"].dataType).alias("yesterday_planned_date"),
                    "today_actual_date",
                    F.lit(None).cast(today.schema["today_actual_date"].dataType).alias("yesterday_actual_date"),
                    "today_load",
                    F.lit(None).cast(today.schema["today_load"].dataType).alias("yesterday_load"),
                    "protocol_activity_type_actual_sk",
                ))

        # Planned date changes
        pln = (today.join(yest, "protocol_activity_type_actual_sk", "inner")
               .filter(
                   (yest["yesterday_planned_date"].isNull() & today["today_planned_date"].isNotNull()) |
                   (yest["yesterday_planned_date"].isNotNull() & today["today_planned_date"].isNull()) |
                   (yest["yesterday_planned_date"] != today["today_planned_date"])
               )
               .select(today["activity_type_actual"], today["protocol_number"], today["study_phase_norm"],
                       today["today_planned_date"], yest["yesterday_planned_date"],
                       F.lit(None).cast("string").alias("today_actual_date"),
                       F.lit(None).cast("string").alias("yesterday_actual_date"),
                       today["today_load"], yest["yesterday_load"],
                       today["protocol_activity_type_actual_sk"]))

        # Actual date changes
        act = (today.join(yest, "protocol_activity_type_actual_sk", "inner")
               .filter(
                   (yest["yesterday_actual_date"].isNull() & today["today_actual_date"].isNotNull()) |
                   (yest["yesterday_actual_date"].isNotNull() & today["today_actual_date"].isNull()) |
                   (yest["yesterday_actual_date"] != today["today_actual_date"])
               )
               .select(today["activity_type_actual"], today["protocol_number"], today["study_phase_norm"],
                       F.lit(None).cast("string").alias("today_planned_date"),
                       F.lit(None).cast("string").alias("yesterday_planned_date"),
                       today["today_actual_date"], yest["yesterday_actual_date"],
                       today["protocol_activity_type_actual_sk"]))

        combined = (pln.drop("today_actual_date", "yesterday_actual_date")
                       .join(act.select("protocol_activity_type_actual_sk",
                                        "today_actual_date", "yesterday_actual_date"),
                             "protocol_activity_type_actual_sk", "left")
                    .union(
                        act.filter(~act["protocol_activity_type_actual_sk"]
                                   .isin([r.protocol_activity_type_actual_sk for r in pln.select("protocol_activity_type_actual_sk").collect()]))
                        .withColumn("today_planned_date",     F.lit(None).cast("string"))
                        .withColumn("yesterday_planned_date", F.lit(None).cast("string"))
                        .withColumn("today_load",             F.lit(None).cast("date"))
                        .withColumn("yesterday_load",         F.lit(None).cast("date"))
                        .select(pln.columns)
                    ))
        return combined
