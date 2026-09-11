# src/gold/models/final_monthly_screening_rate.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from gold_model import GoldModel


class FinalMonthlyScreeningRateModel(GoldModel):
    name         = "final_monthly_screening_rate"
    write_mode   = "overwrite"
    dependencies = ["dim_planisware_milestones", "dim_study", "dim_participant", "fct_participant_visit", "dim_study_site", "fct_site_milestone"]
    class_path   = "gold.models.final_monthly_screening_rate.FinalMonthlyScreeningRateModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        dp = spark.table(f"{gold}.dim_participant").alias("dp")
        fpv = spark.table(f"{gold}.fct_participant_visit").alias("fpv")
        dss = spark.table(f"{gold}.dim_study_site").alias("dss")
        fsm = spark.table(f"{gold}.fct_site_milestone").alias("fsm")
        ds = spark.table(f"{gold}.dim_study").alias("ds")
        pm = spark.table(f"{gold}.dim_planisware_milestones").alias("m")

        valid_site_statuses = [
            "Enrollment Open",
            "Enrollment on-Hold",
            "Site Closed",
            "Active Enrolling",
            "Enrollment Closed",
            "Approved",
            "Submitted",
            "on Hold",
            "On Hold",
            "Close Out Ready",
            "selected",
            "Selected",
            "Activated",
            "Closed",
            "initiated",
            "Initiated",
            "Qualified",
            "Enrollment On-Hold",
        ]

        valid_participant_statuses = [
            "In Screening",
            "Screen Failed",
            "Screen Failure",
            "In Follow-up",
            "Off Study",
            "On Treatment",
            "In Follow Up",
            "Randomized",
            "In Survival FU",
            "Enrolled",
            "Maintenance Phase",
        ]

        recruitment_statuses = [
            "In Follow-up",
            "Off Study",
            "On Treatment",
            "In Follow Up",
            "Randomized",
            "In Survival FU",
            "Enrolled",
            "Maintenance Phase",
        ]

        invalid_participant_numbers = ["-", "--", "`---", "VOID", "OID.", "Void", "OID1", "OID2", "0000"]

        study_milestone_info = (
            pm.join(ds, F.col("m.protocol_number") == F.col("ds.study_id"), "right")
            .filter(F.col("m.activity_type_actual") == F.lit("LPI"))
            .select(
                F.col("m.protocol_number"),
                F.col("ds.study_id").alias("study_id"),
                F.when(F.col("ds.study_state").isin("Open", "Open ", "Active", "Planning"), F.lit("Active"))
                .when(F.col("ds.study_state").isin("Closing", "Completed"), F.lit("Completed"))
                .otherwise(F.col("ds.study_state")).alias("study_status"),
                F.to_date(F.col("m.actual_finish")).alias("lpi_date"),
                F.row_number().over(
                    Window.partitionBy(F.col("ds.study_id")).orderBy(
                        F.when(F.col("m.study_phase_norm") == F.lit("Expansion"), F.lit(1))
                        .when(F.col("m.study_phase_norm") == F.lit("Escalation"), F.lit(2))
                        .otherwise(F.lit(3))
                    )
                ).alias("rn"),
            )
            .filter(F.col("rn") == 1)
            .select("protocol_number", "study_id", "study_status", "lpi_date")
        )

        latest_site = (
            fpv.select(
                "study_id",
                "participant_id_sk",
                "study_site_id_sk",
                "date_of_visit_actual",
                F.row_number().over(
                    Window.partitionBy("study_id", "participant_id_sk").orderBy(F.col("date_of_visit_actual").asc())
                ).alias("rn"),
            )
            .filter(F.col("rn") == 1)
            .select("study_id", "participant_id_sk", "study_site_id_sk")
        )

        screened = (
            dp.join(
                latest_site.alias("ls"),
                (F.col("dp.study_id") == F.col("ls.study_id")) & (F.col("dp.participant_id_sk") == F.col("ls.participant_id_sk")),
                "left",
            )
            .groupBy(F.col("dp.study_id").alias("study_id"), F.col("ls.study_site_id_sk").alias("study_site_id_sk"))
            .agg(
                F.countDistinct(
                    F.when(F.col("dp.participant_status_master").isin(valid_participant_statuses), F.col("dp.participant_number"))
                ).alias("final_total_screened"),
                F.countDistinct(
                    F.when(
                        F.col("dp.participant_status_master").isin(
                            "Screen Failed",
                            "Screen Failure",
                            "In Follow-up",
                            "Off Study",
                            "On Treatment",
                            "In Follow Up",
                            "Randomized",
                            "In Survival FU",
                            "Enrolled",
                            "Maintenance Phase",
                        ),
                        F.col("dp.participant_number"),
                    )
                ).alias("total_screened"),
            )
        )

        recruitment = (
            dp.join(
                fpv,
                (F.col("dp.study_id") == F.col("fpv.study_id")) & (F.col("dp.participant_id_sk") == F.col("fpv.participant_id_sk")),
                "left",
            )
            .groupBy(F.col("dp.study_id").alias("study_id"), F.col("fpv.study_site_id_sk").alias("study_site_id_sk"))
            .agg(
                F.countDistinct(
                    F.when(
                        F.col("dp.participant_status_master").isin(recruitment_statuses)
                        & F.col("fpv.visit_description").isin("Randomization", "Enrollment", "Enrollment V1", "Day 1", "SSV1 (W1D1)", "W1D1"),
                        F.col("dp.participant_number"),
                    )
                ).alias("total_recruitment"),
            )
        )

        participant_count = (
            screened.alias("s")
            .join(
                recruitment.alias("r"),
                (F.col("s.study_id") == F.col("r.study_id")) & (F.col("s.study_site_id_sk") == F.col("r.study_site_id_sk")),
                "full_outer",
            )
            .select(
                F.coalesce(F.col("s.study_id"), F.col("r.study_id")).alias("study_id"),
                F.coalesce(F.col("s.study_site_id_sk"), F.col("r.study_site_id_sk")).alias("study_site_id_sk"),
                F.col("s.final_total_screened"),
                F.col("s.total_screened"),
                F.col("r.total_recruitment"),
            )
        )

        site_milestones = (
            fsm.join(
                dss,
                (F.col("fsm.study_id_sk") == F.col("dss.study_id_sk")) & (F.col("fsm.parent_id") == F.col("dss.study_site_id")),
                "left",
            )
            .filter(F.col("fsm.milestone_position") == F.lit("Actual"))
            .filter(F.col("dss.study_site_status").isin(valid_site_statuses))
            .filter(F.col("fsm.milestone_name").isin("Site Activation", "Closed", "Actual Site Closure"))
            .select(
                F.col("fsm.study_id_sk"),
                F.col("dss.study_id"),
                F.col("fsm.parent_id"),
                F.col("dss.main_site_name").alias("site_name"),
                F.col("dss.site_number"),
                F.col("dss.parent_site_number"),
                F.col("dss.study_site_status"),
                F.col("dss.study_site_id_sk"),
                F.col("fsm.milestone_name"),
                F.col("fsm.milestone_date"),
            )
        )

        site_milestone_dates = (
            site_milestones.groupBy("study_id", "study_site_id_sk", "site_name", "site_number", "parent_site_number")
            .agg(
                F.max(F.when(F.col("milestone_name").isin("Closed", "Actual Site Closure"), F.col("milestone_date"))).alias("closed_date"),
                F.min(F.when(F.col("milestone_name") == F.lit("Site Activation"), F.col("milestone_date"))).alias("site_activation_date"),
            )
        )

        study_site_info = (
            site_milestone_dates.alias("smd")
            .join(participant_count.alias("pc"), (F.col("smd.study_id") == F.col("pc.study_id")) & (F.col("smd.study_site_id_sk") == F.col("pc.study_site_id_sk")), "left")
            .join(study_milestone_info.alias("sm"), F.col("smd.study_id") == F.col("sm.protocol_number"), "left")
            .select(
                F.col("smd.study_id"),
                F.col("smd.study_site_id_sk"),
                F.col("smd.site_name"),
                F.col("smd.site_number"),
                F.col("smd.parent_site_number"),
                F.col("sm.study_status"),
                F.col("sm.lpi_date").alias("actual_lpi_date"),
                F.col("smd.closed_date"),
                F.col("smd.site_activation_date"),
                F.when(
                    F.col("sm.lpi_date").isNotNull() & F.col("smd.closed_date").isNotNull(),
                    F.when(F.col("smd.closed_date") < F.col("sm.lpi_date"), F.col("smd.closed_date")).otherwise(F.col("sm.lpi_date")),
                ).otherwise(F.coalesce(F.col("sm.lpi_date"), F.col("smd.closed_date"), F.current_date())).alias("final_lpi_date"),
                F.col("pc.final_total_screened"),
                F.col("pc.total_screened"),
                F.col("pc.total_recruitment"),
            )
        )

        final_lpi_expr = F.when(
            F.col("actual_lpi_date").isNotNull() & F.col("closed_date").isNotNull(),
            F.when(F.col("closed_date") < F.col("actual_lpi_date"), F.col("closed_date")).otherwise(F.col("actual_lpi_date")),
        ).otherwise(F.coalesce(F.col("actual_lpi_date"), F.col("closed_date"), F.current_date()))

        months_expr = (F.datediff(final_lpi_expr, F.col("site_activation_date")) / F.lit(30.0))

        return (
            study_site_info
            .select(
                "study_id",
                "study_site_id_sk",
                "site_name",
                "site_number",
                "parent_site_number",
                "study_status",
                "actual_lpi_date",
                "closed_date",
                "site_activation_date",
                final_lpi_expr.alias("final_lpi_date"),
                "final_total_screened",
                "total_screened",
                "total_recruitment",
                F.when(F.col("study_status").isin("Completed", "Active"), F.round(F.col("total_screened").cast("double") / F.nullif(months_expr, F.lit(0.0)), 4)).alias("monthly_screening_rate"),
                F.when(F.col("study_status").isin("Completed", "Active"), F.round(F.col("total_recruitment").cast("double") / F.nullif(months_expr, F.lit(0.0)), 4)).alias("monthly_recruitment_rate"),
            )
        )