# src/gold/models/dim_missing_pages.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class DimMissingPagesModel(GoldModel):
    name         = "dim_missing_pages"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_missing_pages.DimMissingPagesModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        mp = (spark.table(f"{cat}.{ctms}.edc_missing_pgs")
              )
        sc = (spark.table(f"{cat}.{ctms}.ctms_study_country")
              
              .select("study_id", "country_name", "study_country_id"))

        return (mp.join(sc, (mp["study_id"] == sc["study_id"]) & (mp["country"] == sc["country_name"]), "left")
                .select(
                    F.expr("cast(uuid() as string)").alias("page_id_sk"),
                    mp["study_id"],
                    sc["study_country_id"],
                    F.substring(mp["subject_number"], -4, 4).alias("participant_number"),
                    F.md5(F.substring(mp["subject_number"], -4, 4)).alias("participant_id_sk"),
                    F.md5(F.concat(sc["study_country_id"], mp["study_id"])).alias("study_country_id_sk"),
                    F.md5(F.concat(mp["study_id"], F.substring(mp["site"], 1, 4))).alias("study_site_id_sk"),
                    F.md5(mp["study_id"]).alias("study_id_sk"),
                    F.substring(mp["site"], 1, 4).alias("site_number"),
                    mp["subject_status"], mp["study_stage"],
                    F.when(mp["enroll_or_screen_fail_date"].isNotNull(),
                           F.to_date(mp["enroll_or_screen_fail_date"], "ddMMMyyyy")).alias("enroll_or_screen_fail_date"),
                    mp["cohort"], mp["treatment_arm"], mp["visit_name"],
                    mp["page_name"], mp["log_line"],
                    F.when(mp["expected_date"].isNotNull(),
                           F.to_date(mp["expected_date"], "ddMMMyyyy")).alias("expected_date"),
                    mp["comments"], mp["days_overdue"], mp["overdue_timeframe"],
                    mp["unique_id"].alias("page_id"),
                ))
