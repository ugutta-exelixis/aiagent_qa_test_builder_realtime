# src/gold/models/dim_planisware_milestones.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from gold_model import GoldModel



_COLS = [
    "project", "project_type", "is_public_version", "line_identifier", "name",
    "activity_type", "activity_type_actual", "protocol_number", "study_phase_norm",
    "cohort", "planned_finish", "actual_finish", "approved_baseline", "original_baseline",
    "load_date", "last_modified_date", "status", "is_required",
    "protocol_activity_type_actual_sk", "protocol_number_sk", "protocol_no_study_phase_sk",
    "vendor_milestone_name",
]


class DimPlaniswareMilestonesModel(GoldModel):
    name         = "dim_planisware_milestones"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.dim_planisware_milestones.DimPlaniswareMilestonesModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _common_schema, _silver_schema
        cat    = _quoted_catalog(spark)
        common = _common_schema(spark)
        ctms   = _silver_schema(spark)

        pm_tbl   = f"{cat}.{ctms}.planisware_milestones"
        pm = spark.table(pm_tbl)

        # Recreate the SQL CTE logic with PySpark DataFrame operations.
        duplicate_days = (
            pm
            .filter(F.col("status").isin("duplicate", "missing"))
            .groupBy("protocol_number")
            .agg(F.max("load_date").alias("today_load"))
            .alias("td")
        )

        prior_clean = (
            pm.alias("pm")
            .join(duplicate_days, F.col("pm.protocol_number") == F.col("td.protocol_number"), "inner")
            .filter(F.col("pm.load_date") < F.col("td.today_load"))
            .groupBy(F.col("pm.protocol_number").alias("protocol_number"), F.col("pm.load_date").alias("load_date"))
            .agg(F.sum(F.when(F.col("pm.status").isin("duplicate", "missing"), F.lit(1)).otherwise(F.lit(0))).alias("bad_cnt"))
        )

        valid_prior = (
            prior_clean
            .filter(F.col("bad_cnt") == 0)
            .groupBy("protocol_number")
            .agg(F.max("load_date").alias("recent_valid_load"))
            .alias("vpr")
        )

        selected_cols = [c for c in _COLS if c != "vendor_milestone_name"]

        clean = (
            pm.alias("pm")
            .join(duplicate_days, F.col("pm.protocol_number") == F.col("td.protocol_number"), "left_anti")
            .select(*[F.col(f"pm.{c}").alias(c) for c in selected_cols])
        )

        fallback = (
            pm.alias("pm")
            .join(valid_prior, (F.col("pm.protocol_number") == F.col("vpr.protocol_number")) & (F.col("pm.load_date") == F.col("vpr.recent_valid_load")), "inner")
            .select(*[F.col(f"pm.{c}").alias(c) for c in selected_cols])
        )

        combined = clean.unionByName(fallback)

        # Derive vendor_milestone_name from milestone_code_map (meta["milestone_map"]).
        # The table has two row types keyed by (activity_code, study_phase_norm):
        #   study_phase_norm = ''           → generic label (any phase)
        #   study_phase_norm = 'Escalation' / 'Expansion' → phase-specific label
        # A phase-specific row wins over the generic one via COALESCE.
        mm = meta.get("milestone_map")
        if mm is None:
            raise RuntimeError(
                "milestone_code_map metadata table is required but was not loaded. "
                "Ensure common.milestone_code_map exists and is seeded before running this model."
            )
        if mm is not None:
            _mm_has_phase = "study_phase_norm" in mm.columns
            if _mm_has_phase:
                # New schema: phase-specific rows win over generic (study_phase_norm = '')
                mm_phase = (mm.filter(F.col("study_phase_norm") != "")
                              .select(F.col("activity_code").alias("_mm_code_p"),
                                      F.col("study_phase_norm").alias("_mm_snorm"),
                                      F.col("milestone_name").alias("_mm_phase_name")))
                mm_generic = (mm.filter((F.col("study_phase_norm").isNull()) | (F.col("study_phase_norm") == ""))
                                .select(F.col("activity_code").alias("_mm_code_g"),
                                        F.col("milestone_name").alias("_mm_generic_name")))
                combined = (combined
                    .join(mm_phase,
                          (F.col("activity_type_actual") == F.col("_mm_code_p")) &
                          (F.col("study_phase_norm")     == F.col("_mm_snorm")),
                          "left")
                    .join(mm_generic,
                          F.col("activity_type_actual") == F.col("_mm_code_g"),
                          "left")
                    .withColumn("vendor_milestone_name",
                        F.coalesce("_mm_phase_name", "_mm_generic_name",
                                   F.col("activity_type_actual")))
                    .drop("_mm_code_p", "_mm_snorm", "_mm_phase_name",
                          "_mm_code_g", "_mm_generic_name"))
            else:
                # Legacy schema (study_phase_norm column not yet added via DDL ALTER):
                # treat all rows as generic — no phase-specific lookup
                mm_generic = (mm.select(F.col("activity_code").alias("_mm_code_g"),
                                        F.col("milestone_name").alias("_mm_generic_name")))
                combined = (combined
                    .join(mm_generic,
                          F.col("activity_type_actual") == F.col("_mm_code_g"),
                          "left")
                    .withColumn("vendor_milestone_name",
                        F.coalesce("_mm_generic_name", F.col("activity_type_actual")))
                    .drop("_mm_code_g", "_mm_generic_name"))

        # De-dup for non-FPI/LPI/CCO rows: keep 1 per (protocol_number, activity_type_actual)
        w_dedup = Window.partitionBy("protocol_number", "activity_type_actual").orderBy(F.desc("study_phase_norm"))
        non_phase = (combined
                     .filter(~F.col("activity_type_actual").isin("FPI", "LPI", "CCO"))
                     .withColumn("_rnk", F.row_number().over(w_dedup))
                     .filter("_rnk = 1")
                     .drop("_rnk"))
        phase_rows = combined.filter(F.col("activity_type_actual").isin("FPI", "LPI", "CCO"))
        final = phase_rows.union(non_phase)

        # Join planisware_exs_ref for trial_identifier
        try:
            ref = spark.table(f"{cat}.{common}.planisware_exs_ref")
            final = (final.join(ref,
                                (final["protocol_number"] == ref["protocol_number"]) &
                                (final["study_phase_norm"] == ref["study_phase"]) &
                                (final["activity_type_actual"] == ref["milestones"]),
                                "left")
                     .drop(ref["protocol_number"])
                     .withColumnRenamed("trial_identifier", "trial_identifier"))
        except Exception:
            final = final.withColumn("trial_identifier", F.lit(None).cast("string"))

        # Join ct_portfolio_source for per-study escalation/expansion and name override
        _cps = (spark.table(f"{cat}.{common}.ct_portfolio_source")
                .select(F.col("study_id").alias("_cps_sid"),
                        F.col("escalation_or_expansion").alias("_cps_esc_exp"),
                        F.col("study_name_display_override").alias("_cps_name_override")))
        final = final.join(_cps, final["protocol_number"] == _cps["_cps_sid"], "left")

        # Derived columns
        date_diff = F.datediff(
            F.try_to_timestamp(F.col("planned_finish")),
            F.coalesce(F.try_to_timestamp(F.col("actual_finish")),
                       F.to_timestamp(F.lit("1900-01-01"))))

        color = (
            F.when((F.try_to_timestamp(F.col("planned_finish")) >= F.try_to_timestamp(F.col("actual_finish"))) &
                   F.col("actual_finish").isNotNull(), F.lit("Happened on time"))
             .when((F.try_to_timestamp(F.col("planned_finish")) >= F.current_timestamp()) &
                   F.col("actual_finish").isNull(), F.lit("Planned for future")))

        date_label = F.when(F.col("actual_finish").isNull(), F.col("planned_finish")).otherwise(F.col("actual_finish"))

        milestone_abbr = (
            F.when(F.col("study_phase_norm").isNull() |
                   F.col("activity_type_actual").isin("FPA", "FSA", "PSA") |
                   (F.col("study_phase_norm") == "empty"), F.col("activity_type_actual"))
             .when(F.col("study_phase_norm") == "Escalation", F.concat(F.col("activity_type_actual"), F.lit("-ES")))
             .when(F.col("study_phase_norm") == "Expansion",  F.concat(F.col("activity_type_actual"), F.lit("-EX"))))

        # esc_exp_filter = F.coalesce(F.col("_cps_esc_exp"), F.lit("empty"))

        w_prot = Window.partitionBy("protocol_number")

        return (final
            .withColumn("color",      color)
                .withColumn("date_label", date_label)
            # .withColumn("bad_label",  F.when(F.col("color") == "Happened but it was late", F.col("date_label")))
            # .withColumn("good_label", F.when(F.col("color") != "Happened but it was late", F.col("date_label")))
                .withColumn("milestone",  milestone_abbr)
                .withColumn("milestone_name_abbreviations", F.lit("milestone"))
            # .withColumn("date_diff",  date_diff)
                .withColumn("milestone_tool_tip",
                            F.when(date_diff == 0, F.lit("Achieved on Time"))
                             .when(date_diff > 0,  F.concat(F.lit("Achieved "), date_diff.cast("string"), F.lit(" days early")))
                             .when(date_diff < 0,  F.concat(F.lit("Achieved "), F.abs(date_diff).cast("string"), F.lit(" days late")))
                             .when(F.col("color") == "Overdue", F.lit("Overdue"))
                             .when(F.col("color") == "Planned for future", F.lit("Planned for future")))
            # .withColumn("escalation_or_expansion",     F.col("study_phase_norm"))
            # .withColumn("escalation_or_expansion_csl", F.col("study_phase_norm"))
            # .withColumn("escalation_or_expansion_filter_csl", esc_exp_filter)
            # .withColumn("fpa", F.when(F.col("activity_type_actual") == "FPA", F.col("actual_finish")))
            # .withColumn("fpi", F.when(F.col("activity_type_actual") == "FPI", F.try_to_timestamp("actual_finish").cast("date")))
            # .withColumn("fsa", F.when(F.col("activity_type_actual") == "FSA", F.try_to_timestamp("actual_finish").cast("date")))
            # .withColumn("lpi", F.max(F.when(F.col("activity_type_actual") == "LPI",
            #                    F.try_to_timestamp("actual_finish").cast("date"))).over(w_prot))
            # .withColumn("first_participant_in_date_actual",
            #         F.min(F.when(F.col("study_phase_norm").isin("Escalation","Expansion") &
            #              (F.col("activity_type_actual") == "FPI"),
            #              F.try_to_timestamp("actual_finish").cast("date"))).over(w_prot))
            # .withColumn("first_site_activated_date_actual",
            #         F.min(F.when(F.col("study_phase_norm").isin("Escalation","Expansion") &
            #              (F.col("activity_type_actual") == "FSA"),
            #              F.try_to_timestamp("actual_finish").cast("date"))).over(w_prot))
            # .withColumn("last_participant_in_date_actual",
            #         F.max(F.when(F.col("study_phase_norm").isin("Escalation","Expansion") &
            #              (F.col("activity_type_actual") == "LPI"),
            #              F.try_to_timestamp("actual_finish").cast("date"))).over(w_prot))
            # .withColumn("protocol_approval_date_actual",
            #         F.max(F.when(F.col("study_phase_norm").isin("Escalation","Expansion") &
            #              (F.col("activity_type_actual") == "FPA"),
            #              F.try_to_timestamp("actual_finish").cast("date"))).over(w_prot))
            # .withColumn("study_name_copy",
            #         F.coalesce(F.col("_cps_name_override"), F.col("name")))
                .drop("_cps_sid", "_cps_esc_exp", "_cps_name_override"))
