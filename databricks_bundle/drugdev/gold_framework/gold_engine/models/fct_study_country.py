# src/gold/models/fct_study_country.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class FctStudyCountryModel(GoldModel):
    name         = "fct_study_country"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.fct_study_country.FctStudyCountryModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _silver_schema
        cat  = _quoted_catalog(spark)
        ctms = _silver_schema(spark)

        ep   = spark.table(f"{cat}.{ctms}.ctms_enroll_projection")
        ps   = spark.table(f"{cat}.{ctms}.ctms_proj_sites")
        sct  = spark.table(f"{cat}.{ctms}.ctms_study_country")
        site = spark.table(f"{cat}.{ctms}.ctms_study_site")
        edc  = spark.table(f"{cat}.{ctms}.edc_overall_edc_study_metrics")

        srm = (ep.groupBy("study_id", "country_name",
                          F.expr("try_cast(month_end_date as date)").alias("month_end_date"))
               .agg(F.sum("planned_subjects").alias("projected_participants"))
               .withColumn("srm_sk", F.md5(F.concat("country_name", "study_id"))))

        proj_sites = (ps.groupBy("study_id", "country_name")
                      .agg(F.sum("planned_sites").alias("num_planned_sites"))
                      .withColumn("proj_sk", F.md5(F.concat("country_name", "study_id"))))

        sc = (sct.withColumn("sc_sk",  F.md5(F.concat("country_name",    "study_id")))
                 .withColumn("sc_id_sk", F.md5(F.concat("study_country_id", "study_id")))
                 .withColumn("study_id_sk", F.md5("study_id"))
                 .withColumn("study_country_id_sk", F.md5(F.concat("study_country_id", "study_id"))))

        site_master = meta.get("site_status_master")
        if site_master is not None:
            active_statuses = [r.ctms_status for r in site_master.filter(F.col("group_name") == "ACTIVE").select("ctms_status").distinct().collect()]
        else:
            active_statuses = ("Activated","Active Enrolling","Enrollment Open",
                               "Enrollment Closed","Closed","Close Out Ready","Enrollment On-Hold")

        actual = (site.filter(F.col("study_site_status").isin(*active_statuses))
                  .filter("site_type = 'Main'")
                  .withColumn("id_sk", F.md5(F.concat("study_country_id", "study_id")))
                  .groupBy("study_id", "id_sk")
                  .agg(F.countDistinct("study_site_id").alias("num_actual_sites")))

        edc_m = (edc.withColumn("edc_sk", F.md5(F.concat("country_name", "studyid")))
                 .select("studyid", "country_name", "on_treatment",
                         "screen_failed_subjects", "total_randomized_subjects",
                         "in_survival_fu", "off_study", "edc_sk").distinct())

        joined = (sc.join(srm, sc["sc_sk"] == srm["srm_sk"], "left")
                  .join(proj_sites, sc["sc_sk"] == proj_sites["proj_sk"], "left")
                  .join(edc_m, sc["sc_sk"] == edc_m["edc_sk"], "left")
                  .join(actual, sc["sc_id_sk"] == actual["id_sk"], "left")
                  .select(
                      sc["study_country_id_sk"], sc["study_id"], sc["study_id_sk"],
                      sc["country_status"],
                      srm["projected_participants"],
                      F.lit(None).cast("long").alias("forecasted_participants"),
                      proj_sites["num_planned_sites"],
                      actual["num_actual_sites"],
                      edc_m["screen_failed_subjects"],
                      edc_m["total_randomized_subjects"],
                      edc_m["on_treatment"],
                      edc_m["in_survival_fu"],
                      edc_m["off_study"],
                  ))

        return (joined.groupBy("study_country_id_sk", "study_id", "study_id_sk", "country_status")
                .agg(
                    F.max("projected_participants").alias("projected_participants"),
                    F.max("forecasted_participants").alias("forecasted_participants"),
                    F.max("num_planned_sites").alias("num_planned_sites"),
                    F.max("num_actual_sites").alias("num_actual_sites"),
                    F.max("screen_failed_subjects").alias("screen_failed_subjects"),
                    F.max("total_randomized_subjects").alias("total_randomized_subjects"),
                    F.max("on_treatment").alias("on_treatment"),
                    F.max("in_survival_fu").alias("in_survival_fu"),
                    F.max("off_study").alias("off_study"),
                ).distinct())
