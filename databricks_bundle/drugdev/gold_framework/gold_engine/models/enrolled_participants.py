# src/gold/models/enrolled_participants.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class EnrolledParticipantsModel(GoldModel):
    name         = "enrolled_participants"
    write_mode   = "overwrite"
    dependencies = ["dim_participant", "fct_participant_visit", "dim_study_site"]
    class_path   = "gold.models.enrolled_participants.EnrolledParticipantsModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        dp = spark.table(f"{gold}.dim_participant").alias("dp")
        fpv = spark.table(f"{gold}.fct_participant_visit").alias("fpv")
        dss = spark.table(f"{gold}.dim_study_site").alias("dss")

        valid_statuses = [
            "Maintenance Phase",
            "In Survival FU",
            "Off Study",
            "Randomized",
            "On Treatment",
            "In Follow Up",
            "In Follow-up",
        ]

        invalid_participant_numbers = [
            "-",
            "--",
            "`---",
            "VOID",
            "OID.",
            "Void",
            "OID1",
            "OID2",
            "0000",
        ]

        enrolled = (
            dp.join(
                fpv,
                (F.col("dp.study_id") == F.col("fpv.study_id"))
                & (F.col("dp.participant_id_sk") == F.col("fpv.participant_id_sk")),
                "left",
            )
            .join(
                dss,
                (F.col("fpv.study_id") == F.col("dss.study_id"))
                & (F.col("fpv.study_site_id_sk") == F.col("dss.study_site_id_sk")),
                "left",
            )
            .filter(F.col("dp.participant_status_master").isin(valid_statuses))
            .filter(F.col("fpv.visit_description").isin("Randomization", "Enrollment", "Enrollment V1", "Day 1", "SSV1 (W1D1)", "W1D1"))
            .filter(~F.col("dp.participant_number").isin(invalid_participant_numbers))
            .groupBy(
                F.col("dp.study_id").alias("study_id"),
                F.col("dss.main_site_name").alias("main_site_name"),
                F.col("dss.site_number").alias("site_number"),
                F.col("fpv.study_site_id_sk").alias("study_site_id_sk"),
            )
            .agg(F.countDistinct(F.col("dp.participant_number")).alias("enrolled_participants"))
        )

        return enrolled.select(
            "enrolled_participants",
            "study_id",
            "main_site_name",
            "site_number",
            "study_site_id_sk",
        )