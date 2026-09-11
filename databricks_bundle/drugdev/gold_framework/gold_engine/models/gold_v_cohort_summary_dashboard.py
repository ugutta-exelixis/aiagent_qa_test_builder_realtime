from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class GoldVCohortSummaryDashboardModel(GoldModel):
    name = "gold_v_cohort_summary_dashboard"
    write_mode = "overwrite"
    dependencies = ["dim_participant", "dim_cohort_summary", "fct_participant_visit", "dim_study_site"]
    class_path = "gold.models.gold_v_cohort_summary_dashboard.GoldVCohortSummaryDashboardModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _gold_schema

        cat = _quoted_catalog(spark)
        gold = f"{cat}.{_gold_schema(spark)}"

        dp = spark.table(f"{gold}.dim_participant")
        dcs = spark.table(f"{gold}.dim_cohort_summary")
        fpv = spark.table(f"{gold}.fct_participant_visit")
        dss = spark.table(f"{gold}.dim_study_site")

        phase_studies = {"XL092-002", "XL092-009", "XL309-101"}
        tf_plus_studies = {"XB002-101"}
        tumor_studies = {"XB010-101", "XL495-101", "XB371-101", "XB628-101"}
        detail_studies = phase_studies | tf_plus_studies | tumor_studies

        tf_plus_tumors = {
            "Brain",
            "Colorectal",
            "Gastric Cancer and Gastro-esophageal Junction",
            "Melanoma",
            "Other",
            "Thyroid",
            "Urothelial",
        }

        enrolled_master = ("Randomized", "In Follow-up", "Off Study", "On Treatment")
        enrolled_irt = ("Enrolled", "Treatment Completed", "Randomized", "Discontinued")
        discontinued_master = ("In Follow-up", "Off Study")
        discontinued_irt = "Discontinued"

        # def _allowed_participants(study_ids: List[str]) -> DataFrame:
        #     site_ids = dss.filter(F.col("study_id").isin(*study_ids)).select("study_site_id_sk").distinct()
        #     return (
        #         fpv.filter(F.col("study_id").isin(*study_ids))
        #         .join(site_ids, "study_site_id_sk", "inner")
        #         .select(
        #             F.col("study_id"),
        #             F.col("participant_number").cast("string").alias("participant_id"),
        #         )
        #         .distinct()
        #     )

        # allowed_009 = _allowed_participants(["XL092-009"])
        # allowed_009_309 = _allowed_participants(["XL092-009", "XL309-101"])
        def _allowed_participants(study_ids: List[str] | None = None) -> DataFrame:
            site_ids = (
                dss
                # .filter(F.col("study_id").isin(*study_ids))
                .select("study_site_id_sk")
                .distinct()
                .alias("site_ids")
            )
            fpv_filtered = fpv.alias("fpv")
            if study_ids is not None:
                fpv_filtered = fpv_filtered.filter(F.col("study_id").isin(*study_ids))
            return (
                fpv_filtered
                .join(site_ids, F.col("fpv.study_site_id_sk") == F.col("site_ids.study_site_id_sk"), "inner")
                .select(
                    F.col("fpv.study_id").alias("study_id"),
                    F.col("fpv.participant_number").cast("string").alias("participant_id"),
                )
                .distinct()
            )

        def _base_subset(study_id: str, allowed: DataFrame | None = None) -> DataFrame:
            subset = dp.filter(F.col("study_id") == study_id)
            if allowed is not None:
                subset = subset.join(
                    allowed,
                    (subset["study_id"] == allowed["study_id"]) &
                    (subset["participant_id"].cast("string") == allowed["participant_id"]),
                    "inner",
                )
            return subset

        def _subject_labels(study_id: str) -> tuple[F.Column, F.Column]:
            # ---- Common cohort_group expression for ALL studies ----
            cohort_group = F.concat(
                F.coalesce(F.col("study_phase"), F.lit("")),
                F.lit(" Phase "),
                F.coalesce(F.col("cohort"), F.lit("")),
            )
            # if study_id in phase_studies:
            #     cohort_group = F.concat(
            #         F.coalesce(F.col("study_phase"), F.lit("")),
            #         F.lit(" Phase "),
            #         F.coalesce(F.col("cohort"), F.lit("")),
            #     )
            #     tumor_type = F.coalesce(F.col("tumor_type"), F.lit(""))
            # el
            if study_id in tf_plus_studies:
                tumor_type = F.when(
                    F.col("tumor_type").isin(*tf_plus_tumors),
                    F.lit("TF+ Cancers"),
                ).otherwise(F.coalesce(F.col("tumor_type"), F.lit("")))
                # cohort_group = tumor_type
            # elif study_id in tumor_studies:
            #     tumor_type = F.coalesce(F.col("tumor_type"), F.lit(""))
            #     cohort_group = tumor_type
            else:
                tumor_type = F.coalesce(F.col("tumor_type"), F.lit(""))
                # cohort_group = F.coalesce(F.col("cohort_group"), F.lit(""))
            return cohort_group, tumor_type

        def _subject_aggregate(df: DataFrame, study_id: str) -> DataFrame:
            cohort_group_expr, tumor_type_expr = _subject_labels(study_id)
            return (
                df.withColumn("cohort_group", cohort_group_expr)
                .withColumn("tumor_type", tumor_type_expr)
                .groupBy("study_id", "cohort", "cohort_status", "cohort_group", "tumor_type")
                .agg(
                    F.sum(F.when(F.col("participant_status_master") == "In Screening", 1).otherwise(0)).alias("In_Screening"),
                    F.sum(F.when(F.col("participant_status_master") == "Screen Failure", 1).otherwise(0)).alias("Screen_Failed"),
                    F.sum(
                        F.when(
                            F.col("participant_status_master").isin(*enrolled_master)
                            & F.col("participant_status_irt").isin(*enrolled_irt),
                            1,
                        ).otherwise(0)
                    ).alias("total_enrolled"),
                    F.sum(
                        F.when(
                            F.col("participant_status_master").isin(*discontinued_master)
                            & ((F.col("participant_status_irt") == discontinued_irt) | ((F.trim(F.coalesce(F.col("participant_status_irt"), F.lit(""))) == "") & (F.col("participant_status_edc") == "Off Study"))),
                            1,
                        ).otherwise(0)
                    ).alias("total_discontinued"),
                )
                .withColumn(
                    "total_screened",
                    F.col("In_Screening") + F.col("Screen_Failed") + F.col("total_enrolled") ,
                )
                .withColumn(
                    "Screen_Failed_percent",
                    F.when(
                        F.col("Screen_Failed") != 0,
                        F.round(
                            (F.col("Screen_Failed").cast("decimal(7,2)"))
                            / (
                                F.col("Screen_Failed")
                                + F.col("total_enrolled")
                                # + F.col("total_discontinued")
                            ).cast("decimal(7,2)")
                            * 100,
                        ),
                    ).otherwise(F.lit(0)),
                )
            )

        def _dose_labels(study_id: str) -> tuple[F.Column, F.Column, DataFrame | None]:
            # ---- Common cohort_group expression for ALL studies ----
            cohort_group = F.concat(
                F.coalesce(F.col("study_phase"), F.lit("")),
                F.lit(" Phase "),
                F.coalesce(F.col("cohort"), F.lit("")),
            )
            # if study_id in phase_studies:
            #     cohort_group = F.concat(
            #         F.coalesce(F.col("study_phase"), F.lit("")),
            #         F.lit(" Phase "),
            #         F.coalesce(F.col("cohort"), F.lit("")),
            #     )
            #     tumor_type = F.when(
            #         F.col("study_id") == "XL092-002",
            #         F.coalesce(F.col("tumor_type"), F.lit("")),
            #     ).otherwise(F.coalesce(F.col("tumor_type"), F.lit("")))
            #     dose_arm = F.col("dose_level_treatment_arm")
            #     extra_filter = None
            # el
            if study_id == "XB002-101":
                tumor_type = F.when(
                    F.col("tumor_type").isin(*tf_plus_tumors),
                    F.lit("TF+ Cancers"),
                ).otherwise(F.coalesce(F.col("tumor_type"), F.lit("")))
                # cohort_group = tumor_type
                dose_arm = F.col("dose_level_treatment_arm")
                extra_filter = F.col("study_phase") == "Expansion"
            # elif study_id in {"XB010-101", "XL495-101", "XB371-101"}:
            #     tumor_type = F.coalesce(F.col("tumor_type"), F.lit(""))
            #     # cohort_group = tumor_type
            #     dose_arm = F.col("dose_level_treatment_arm")
            #     extra_filter = None
            elif study_id == "XB628-101":
                tumor_type = F.coalesce(F.col("tumor_type"), F.lit(""))
                # cohort_group = tumor_type
                dose_arm = F.col("treatment_arm")
                extra_filter = None
            else:
                tumor_type = F.coalesce(F.col("tumor_type"), F.lit(""))
                # cohort_group = F.coalesce(F.col("cohort_group"), F.lit(""))
                dose_arm = F.col("dose_level_treatment_arm")
                extra_filter = None
            return cohort_group, tumor_type, dose_arm, extra_filter

        def _dose_aggregate(df: DataFrame, study_id: str) -> DataFrame:
            cohort_group_expr, tumor_type_expr, dose_arm_expr, extra_filter = _dose_labels(study_id)
            dose_df = df.withColumn("cohort_group", cohort_group_expr).withColumn("tumor_type", tumor_type_expr)
            if extra_filter is not None:
                dose_df = dose_df.filter(extra_filter)
            if study_id == "XB628-101":
                dose_df = dose_df.filter(F.coalesce(F.col("treatment_arm"), F.col("dose_level_treatment_arm")).isNotNull())
            else:
                dose_df = dose_df.filter(dose_arm_expr.isNotNull())
            dose_df = dose_df.withColumn(
                "dose_level_treatment_arm",
                F.when(F.col("study_id") == "XB628-101", (F.coalesce(F.col("treatment_arm"), F.col("dose_level_treatment_arm")))).otherwise(dose_arm_expr),
            ).withColumn(
                "dose_level",
                F.when(
                    F.col("study_id") == "XB628-101",
                    (F.coalesce(F.col("treatment_arm"), F.col("dose_level_treatment_arm"))),
                ).otherwise(F.lower(F.split(F.col("dose_level_treatment_arm"), "_").getItem(1))),
            )
            return (
                dose_df.groupBy(
                    "study_id",
                    "cohort",
                    "cohort_status",
                    "cohort_group",
                    "tumor_type",
                    "dose_level_treatment_arm",
                    "dose_level",
                )
                .agg(
                # (F.countDistinct(F.col("participant_id")).alias("assigned_dose_level"),
                    F.sum(
                        F.when(
                            (
                                F.col("participant_status_master").isin(*enrolled_master)
                                & F.col("participant_status_irt").isin(*enrolled_irt)
                            )
                            | (
                                (
                                    ~F.col("participant_status_master").isin(*enrolled_master)
                                    | ~F.col("participant_status_irt").isin(*enrolled_irt)
                                )
                                & (F.col("dose_level_treatment_arm").isNotNull() &
                                (F.trim(F.col("dose_level_treatment_arm")) != ""))
                            ),
                            1,
                        ).otherwise(0)
                    ).alias("Assigned_Dose_Level"),
                    # F.sum(F.when(F.col("participant_status_master") == "In Screening", 1).otherwise(0)).alias("assigned_dose_level"),
                    F.sum(
                        F.when(
                            F.col("participant_status_master").isin("In Follow-up", "Off Study", "On Treatment")
                            | F.col("participant_status_master").isNull(),
                            F.when(F.col("participant_status_irt") == "Discontinued", 1).otherwise(0),
                        ).otherwise(0)
                    ).alias("discontinued_by_dose_level"),
                )
            )

        subject_parts: List[DataFrame] = []
        dose_parts: List[DataFrame] = []
        study_ids = [
            row["study_id"]
            for row in (
                dp.select("study_id")
                .filter(F.col("study_id").isNotNull() & (F.trim(F.col("study_id")) != ""))
                .distinct()
                .orderBy("study_id")
                .collect()
            )
        ]

        # Subject summary indices
        # Legacy hardcoded study handling kept for reference:
        # subject_parts.append(_subject_aggregate(_base_subset("XL092-002"), "XL092-002"))
        # subject_parts.append(_subject_aggregate(_base_subset("XL092-009"),  "XL092-009"))
        # subject_parts.append(_subject_aggregate(_base_subset("XL309-101"), "XL309-101"))
        # subject_parts.append(_subject_aggregate(_base_subset("XB002-101"), "XB002-101"))
        # subject_parts.append(_subject_aggregate(_base_subset("XB010-101"), "XB010-101"))
        # subject_parts.append(_subject_aggregate(_base_subset("XL495-101"), "XL495-101"))
        # subject_parts.append(_subject_aggregate(_base_subset("XB371-101"), "XB371-101"))
        # subject_parts.append(_subject_aggregate(_base_subset("XB628-101"), "XB628-101"))
        for study_id in study_ids:
            subject_parts.append(_subject_aggregate(_base_subset(study_id), study_id))

        subject_indices = subject_parts[0]
        for part in subject_parts[1:]:
            subject_indices = subject_indices.unionByName(part)

        # Dose-level aggregates
        # Legacy hardcoded study handling kept for reference:
        # dose_parts.append(_dose_aggregate(_base_subset("XL092-002"), "XL092-002"))
        # dose_parts.append(_dose_aggregate(_base_subset("XL092-009"), "XL092-009"))
        # dose_parts.append(_dose_aggregate(_base_subset("XL309-101"), "XL309-101"))
        # dose_parts.append(_dose_aggregate(_base_subset("XB002-101"), "XB002-101"))
        # dose_parts.append(_dose_aggregate(_base_subset("XB010-101"), "XB010-101"))
        # dose_parts.append(_dose_aggregate(_base_subset("XL495-101"), "XL495-101"))
        # dose_parts.append(_dose_aggregate(_base_subset("XB371-101"), "XB371-101"))
        # dose_parts.append(_dose_aggregate(_base_subset("XB628-101"), "XB628-101"))
        for study_id in study_ids:
            dose_parts.append(_dose_aggregate(_base_subset(study_id), study_id))

        dose_level = dose_parts[0]
        for part in dose_parts[1:]:
            dose_level = dose_level.unionByName(part)

        subject_keys = ["study_id", "cohort", "cohort_status", "cohort_group", "tumor_type"]

        subject_with_dose = (
            subject_indices.join(
                dose_level.select(*subject_keys, "dose_level_treatment_arm", "dose_level"),
                subject_keys,
                "left",
            )
            .join(
                dose_level.select(
                    *subject_keys,
                    "dose_level_treatment_arm",
                    F.col("assigned_dose_level"),
                    F.col("discontinued_by_dose_level"),
                ),
                subject_keys + ["dose_level_treatment_arm"],
                "left",
            )
            # .withColumn(
            #     "cohort_group",
            #     F.when(
            #         (F.col("study_id") == "XB010-101") & (F.col("cohort_group") == "SOLID TUMORS"),
            #         F.concat(F.lit("Escalation Phase "), F.col("cohort")),
            #     )
            #     .when(
            #         (F.col("study_id") == "XB010-101") & (~F.col("cohort_group").isin("SOLID TUMORS", "")),
            #         F.concat(F.lit("Expansion Phase "), F.initcap(F.col("cohort"))),
            #     )
            #     .otherwise(F.col("cohort_group")),
            # )
        )

        cohort_targets = (
            dcs.select(
                "study_id",
                F.col("cohort").alias("target_cohort"),
                F.col("cohort_description").alias("cohort_desc"),
                F.coalesce(F.col("enrollment_cap"), F.lit(0)).alias("Enrollment_target"),
            )
            .groupBy("study_id", "target_cohort","cohort_desc")
            .agg(F.sum("Enrollment_target").alias("Enrollment_target"))
        )

        # total_target = cohort_targets.groupBy("study_id").agg(F.sum("Enrollment_target").alias("Enrollment_target"))
        # total_target = (
        #     subject_final.groupBy("study_id")
        #     .agg(F.sum("Enrollment_target").alias("Enrollment_target"))
        # )

        subject_final = (
            subject_with_dose.join(
                cohort_targets,
                (subject_with_dose["study_id"] == cohort_targets["study_id"]) &
                # (F.lower(F.coalesce(subject_with_dose["cohort"], F.lit(""))) == F.lower(F.coalesce(cohort_targets["target_cohort"], F.lit("")))),
                (
                    (F.lower(F.coalesce(subject_with_dose["cohort"], F.lit(""))) == F.lower(F.coalesce(cohort_targets["target_cohort"], F.lit(""))))
                    |
                    (F.coalesce(subject_with_dose["cohort"], F.lit("")))
                        .eqNullSafe(F.upper(F.coalesce(cohort_targets["cohort_desc"], F.lit(""))))
                ),
                "left",
            )
            .select(
                subject_with_dose["study_id"],
                subject_with_dose["cohort_group"],
                subject_with_dose["tumor_type"],
                subject_with_dose["cohort"],
                subject_with_dose["cohort_status"],
                subject_with_dose["total_screened"],
                subject_with_dose["In_Screening"],
                subject_with_dose["Screen_Failed"],
                subject_with_dose["total_enrolled"],
                subject_with_dose["total_discontinued"],
                subject_with_dose["Screen_Failed_percent"],
                F.coalesce(F.col("Enrollment_target"), F.lit(0)).alias("Enrollment_target"),
                F.coalesce(subject_with_dose["dose_level_treatment_arm"], F.lit("")).alias("dose_level_treatment_arm"),
                F.coalesce(F.col("assigned_dose_level"), F.lit(0)).alias("assigned_dose_level"),
                F.coalesce(F.col("discontinued_by_dose_level"), F.lit(0)).alias("discontinued_by_dose_level"),
            )
        ).distinct()
        total_target = total_target = (
            subject_final.groupBy("study_id")
            .agg(F.sum("Enrollment_target").alias("Enrollment_target")
                #  ,F.sum("assigned_dose_level").alias("assigned_dose_level")
                )
            )
        def _total_branch(study_id: str, df: DataFrame, filter_expr: F.Column) -> DataFrame:
            filtered = df.filter(filter_expr)
            if study_id in {"XL092-002", "XL092-009"}:
                filtered = filtered.filter(F.col("cohort").isNotNull())
            elif study_id == "XB002-101":
                filtered = filtered.filter(F.col("cohort").isNotNull() & (F.upper(F.col("cohort")) != "COHORT A"))
            elif study_id in { "XL495-101", "XB371-101"}:
                filtered = filtered.filter(F.col("tumor_type").isNotNull())
            # elif study_id == "XB628-101":
            #     filtered = filtered.filter(F.col("tumor_type").isNotNull() & F.col("treatment_arm").isNotNull())

            return (
                filtered.groupBy("study_id")
                .agg(
                    F.sum(F.when(F.col("participant_status_master") == "In Screening", 1).otherwise(0)).alias("In_Screening"),
                    F.sum(F.when(F.col("participant_status_master") == "Screen Failure", 1).otherwise(0)).alias("Screen_Failed"),
                    F.sum(
                        F.when(
                            F.col("participant_status_master").isin(*enrolled_master)
                            & F.col("participant_status_irt").isin(*enrolled_irt),
                            1,
                        ).otherwise(0)
                    ).alias("total_enrolled"),
                    # F.sum(
                    #     F.when(
                    #         F.col("participant_status_master").isin(*enrolled_master)
                    #         & F.col("participant_status_irt").isin("Enrolled", "Treatment Completed", "Discontinued")
                    #         & F.col("dose_level_treatment_arm").isNotNull(),
                    #         1,
                    #     ).otherwise(0)
                    # ).alias("total_enrolled_dose_level"),
                    F.sum(
                        F.when(
                            (
                                F.col("participant_status_master").isin(*enrolled_master)
                                & F.col("participant_status_irt").isin(*enrolled_irt)
                            )
                            | (
                                (
                                    ~F.col("participant_status_master").isin(*enrolled_master)
                                    | ~F.col("participant_status_irt").isin(*enrolled_irt)
                                )
                                & F.col("dose_level_treatment_arm").isNotNull()& (F.col("dose_level_treatment_arm").isNotNull() &
                                    (F.trim(F.col("dose_level_treatment_arm")) != ""))
                            ),
                            1,
                        ).otherwise(0)
                    ).alias("total_enrolled_dose_level"),
                    F.sum(
                        F.when(
                            F.col("participant_status_master").isin("In Follow-up", "Off Study", "On Treatment")
                            & (F.col("participant_status_irt") == "Discontinued"),
                            1,
                        ).otherwise(0)
                    ).alias("total_discontinued"),
                )
                .withColumn(
                    "total_screened",
                    F.col("In_Screening") + F.col("Screen_Failed") + F.col("total_enrolled") ,
                )
                .withColumn(
                    "Screen_Failed_percent",
                    F.when(
                        F.col("Screen_Failed") != 0,
                        F.round(
                            (F.col("Screen_Failed").cast("decimal(7,2)"))
                            / (
                                F.col("Screen_Failed")
                                + F.col("total_enrolled")
                                # + F.col("total_discontinued")
                            ).cast("decimal(7,2)")
                            * 100,
                        ),
                    ).otherwise(F.lit(0)),
                )
            )

        total_parts: List[DataFrame] = []
        # Legacy hardcoded study handling kept for reference:
        # total_parts.append(_total_branch("XL092-002", dp.filter(F.col("study_id") == "XL092-002"), F.col("study_id") == "XL092-002"))
        # total_parts.append(_total_branch("XB002-101", dp.filter(F.col("study_id") == "XB002-101"), F.col("study_id") == "XB002-101"))
        # total_parts.append(_total_branch("XB010-101", dp.filter(F.col("study_id") == "XB010-101"), F.col("study_id") == "XB010-101"))
        # total_parts.append(_total_branch("XL495-101", dp.filter(F.col("study_id") == "XL495-101"), F.col("study_id") == "XL495-101"))
        # total_parts.append(_total_branch("XB371-101", dp.filter(F.col("study_id") == "XB371-101"), F.col("study_id") == "XB371-101"))
        # total_parts.append(_total_branch("XB628-101", dp.filter(F.col("study_id") == "XB628-101"), F.col("study_id") == "XB628-101"))
        # total_parts.append(_total_branch("XL309-101", dp.filter(F.col("study_id") == "XL309-101"), F.col("study_id") == "XL309-101"))
        for study_id in study_ids:
            total_parts.append(
                _total_branch(
                    study_id,
                    dp.filter(F.col("study_id") == study_id),
                    F.col("study_id") == study_id,
                )
            )

        total_count = total_parts[0]
        for part in total_parts[1:]:
            total_count = total_count.unionByName(part)

        total_rows = (
            total_count.join(total_target, "study_id", "left")
            .select(
                F.col("study_id"),
                F.lit("Total").alias("cohort_group"),
                F.lit(" ").alias("tumor_type"),
                F.lit(" ").alias("cohort"),
                F.lit(" ").alias("cohort_status"),
                F.col("total_screened"),
                F.col("In_Screening"),
                F.col("Screen_Failed"),
                F.col("total_enrolled"),
                F.col("total_discontinued"),
                F.col("Screen_Failed_percent"),
                F.coalesce(F.col("Enrollment_target"), F.lit(0)).alias("Enrollment_target"),
                F.lit(" ").alias("dose_level_treatment_arm"),
                F.col("total_enrolled_dose_level").alias("assigned_dose_level"),
                # F.col("assigned_dose_level"),
                F.col("total_discontinued").alias("discontinued_by_dose_level"),
            )
        )

        result = subject_final.unionByName(total_rows)
        return result
