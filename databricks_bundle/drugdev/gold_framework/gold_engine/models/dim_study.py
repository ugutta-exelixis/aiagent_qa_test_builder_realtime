# src/gold/models/dim_study.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class DimStudyModel(GoldModel):
    name         = "dim_study"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_study.DimStudyModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _common_schema, _silver_schema
        cat    = _quoted_catalog(spark)
        common = _common_schema(spark)
        ctms   = _silver_schema(spark)

        cps = spark.table(f"{cat}.{common}.ct_portfolio_source")
        ctms_study = spark.table(f"{cat}.{ctms}.ctms_study")
        enroll_proj = spark.table(f"{cat}.{ctms}.ctms_enroll_projection")

        ct = (cps.select(
            F.md5("study_id").alias("study_id_sk"),
            "study_id", "cro", "study_type",
            F.col("brief_title").alias("study_title"),
            F.col("compound").alias("study_drug"),
            "phase", "indication", "study_stage", "study_state",
            "study_status", "active_flag", "compound_program_name",
            "franchise",
            "csl_indication",
            "phase_override",
            "participant_on_active_treatment",
            "program_name_override",
        ))

        sd = (ctms_study.select(
            F.md5("study_id").alias("study_id_sk"),
            "study_title", "study_drug",
            F.lit(None).cast("string").alias("phase"),
            F.col("therapeutic_area").alias("indication"),
            "study_id",
            F.current_date().alias("load_date"),
        ))

        sep = (enroll_proj
               .filter(
                   (F.year(F.to_date("month_end_date", "DD-MON-YYYY")) == F.year(F.current_date())) &
                   (F.month(F.to_date("month_end_date", "DD-MON-YYYY")) == F.month(F.current_date()))
               )
               .groupBy("study_id")
               .agg(F.sum("planned_subjects").alias("enrollment_target")))

        df = (ct.alias("cps")
              .join(sd.alias("sd"), "study_id", "left")
              .join(sep.alias("sep"), ct["study_id"] == sep["study_id"], "left")
              .select(
                  ct["study_id_sk"],
                  ct["study_id"],
                  F.coalesce(sd["study_title"], ct["study_title"]).alias("study_title"),
                  F.coalesce(sd["study_drug"],  ct["study_drug"]).alias("study_drug"),
                  F.coalesce(ct["phase"],        sd["phase"]).alias("phase"),
                  F.coalesce(ct["indication"],   sd["indication"]).alias("indication"),
                  F.coalesce(ct["csl_indication"], ct["indication"], sd["indication"]).alias("csl_indication"),
                  ct["cro"].cast("string"),
                  ct["study_type"].cast("string"),
                  ct["study_stage"].cast("string"),
                  ct["study_state"].cast("string"),
                  ct["study_status"].cast("string"),
                  ct["active_flag"].cast("boolean"),
                  ct["compound_program_name"].cast("string"),
                  ct["franchise"].cast("string"),
                  sd["load_date"],
                  F.current_date().alias("aud_created_date"),
                  F.current_date().alias("aud_updated_date"),
                #   ct["cro"].alias("cro_name"),
                #   F.coalesce(ct["indication"], sd["indication"]).alias("indication_copy"),
                #   F.coalesce(ct["csl_indication"], ct["indication"], sd["indication"]).alias("csl_indication"),
                #   F.substring(ct["study_id"], 1, 5).alias("product"),
                #   ct["compound_program_name"].cast("string").alias("program_name"),
                #   F.coalesce(ct["program_name_override"], ct["compound_program_name"]).alias("program_name_copy"),
                #   F.coalesce(ct["compound_program_name"], F.lit("Unmapped")).alias("program_name_filter"),
                #   F.trim(ct["study_state"]).alias("state"),
                #   F.concat(ct["compound_program_name"], F.lit("-"), F.substring(ct["study_id"], -3, 3)).alias("study_name"),
                #   F.coalesce(ct["phase_override"], ct["phase"]).alias("phase_copy"),
                #   F.coalesce(ct["phase_override"], ct["phase"]).alias("note"),
                  ct["participant_on_active_treatment"].cast("string"),
              ))
        return df
