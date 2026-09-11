# src/gold/models/dim_participant.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from gold_model import GoldModel


class DimParticipantModel(GoldModel):
    name         = "dim_participant"
    write_mode   = "overwrite"
    dependencies = []
    tags         = {"mart": "dim_participant.sql"}
    class_path   = "gold.models.dim_participant.DimParticipantModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _common_schema, _silver_schema
        cat    = _quoted_catalog(spark)
        common = _common_schema(spark)
        ctms   = _silver_schema(spark)
        irt    = _silver_schema(spark)
        edc    = _silver_schema(spark)

        irt_sr  = spark.table(f"{cat}.{irt}.irt_subject_summary_report")
        edc_ss  = spark.table(f"{cat}.{edc}.edc_subject_summary")
        ctms_sm = spark.table(f"{cat}.{ctms}.ctms_study_milestone")
        ctms_su = spark.table(f"{cat}.{ctms}.ctms_study_subject")
        irt_csr = spark.table(f"{cat}.{irt}.irt_cohort_summary_report")

        def _load_date_expr(df: DataFrame, alias_name: str):
            if "_ingestion_timestamp" in df.columns:
                return F.to_date(F.col("_ingestion_timestamp")).alias(alias_name)
            return F.current_date().alias(alias_name)

        # Studies that use a 6-character participant ID (driven by ct_portfolio_source)
        _cps_df = spark.table(f"{cat}.{common}.ct_portfolio_source")
        # Schema guard: new columns may not exist yet if the migration DDL hasn't been run.
        # Fall back to safe defaults so the model can still execute.
        _cps_cols = set(_cps_df.columns)
        _sel = [
            F.col("study_id"),
            (F.coalesce("cohort_source", F.lit("IRT")) if "cohort_source" in _cps_cols
             else F.lit("IRT")).alias("cohort_source"),
            (F.coalesce("participant_id_length", F.lit(4)) if "participant_id_length" in _cps_cols
             else F.lit(4)).alias("pid_len"),
            (F.coalesce("study_phase_source", F.lit("IRT")) if "study_phase_source" in _cps_cols
             else F.lit("IRT")).alias("study_phase_source"),
            (F.col("cohort_status_key_formula") if "cohort_status_key_formula" in _cps_cols
             else F.lit(None).cast("string")).alias("cohort_status_key_formula"),
            (F.col("use_cohort_status_join") if "use_cohort_status_join" in _cps_cols
             else F.lit(True)).alias("use_cohort_status_join"),
        ]
        _cps_rows = _cps_df.select(*_sel).collect()

        _cohort_source      = {r.study_id: r.cohort_source      for r in _cps_rows}
        _phase_source       = {r.study_id: r.study_phase_source  for r in _cps_rows}
        _cs_key_formula     = {r.study_id: r.cohort_status_key_formula for r in _cps_rows}
        _six_char = tuple(r.study_id for r in _cps_rows if r.pid_len == 6) or ("__no_six_char_studies__",)

        # IRT_EDC_SUPPLEMENT: IRT-primary studies that also pull EDC rows not in IRT
        _irt_edc_supp  = tuple(sid for sid, src in _cohort_source.items() if src == "IRT_EDC_SUPPLEMENT")
        # IRT_EDC_JOIN: IRT rows joined to EDC for tumor_type
        _irt_edc_join  = tuple(sid for sid, src in _cohort_source.items() if src == "IRT_EDC_JOIN")
        # IRT_LIVER_REMAP: IRT rows with cohort_desc remap (liver/non-liver)
        _irt_liver     = tuple(sid for sid, src in _cohort_source.items() if src == "IRT_LIVER_REMAP")
        # IRT: plain IRT-only
        _irt_only      = tuple(sid for sid, src in _cohort_source.items() if src == "IRT")

        # Studies where cohort_status is surfaced (driven by ct_portfolio_source)
        _cohort_status_studies = tuple(
            r.study_id for r in _cps_rows if r.use_cohort_status_join
        ) or ("__no_cohort_status_studies__",)

        # ── study_milestone ──────────────────────────────────────────────────
        study_milestone = (ctms_sm
            .filter(F.col("parent_dataset") == "SUBJECT")
            .groupBy("study_id", "parent_id")
            .agg(F.max("milestone_date").alias("enrollment_date"))
            .withColumn("study_milestone_sk", F.md5(F.concat("study_id", "parent_id"))))

        # ── study_phase + cohort (refactored flow) ───────────────────────────
        phase_rules_tbl = spark.table(f"{cat}.{common}.dim_participant_phase_rules")
        edc_eps = spark.table(f"{cat}.{edc}.edc_early_phase_subject_status")

        # ── study_subject (PRA/CTMS) ──────────────────────────────────────────
        study_subject = (ctms_su
            .withColumn("_pid", F.substring("subject_number", -4, 4))
            .select(
                F.col("_pid").alias("participant_id_pra"),
                F.col("study_site_id").alias("site_number_pra"),
                F.md5("study_id").alias("study_id_sk_pra"),
                F.md5(F.col("_pid")).alias("participant_id_sk_pra"),
                F.md5(F.concat("study_id", "subject_id")).alias("study_subject_sk_pra"),
                F.md5(F.concat("study_id", "_pid")).alias("study_subject_sk2_pra"),
                F.col("study_id").alias("study_id_pra"),
                F.lit("pra").alias("source_pra"),
                F.col("_pid").alias("participant_number_pra"),
                "randomization_id", "enrollment_id",
                F.col("subject_status").alias("participant_status_pra"),
                F.col("subject_status_reason").alias("participant_status_reason_pra"),
                _load_date_expr(ctms_su, "load_date"),
            ))

        dim_pra = (study_subject
            .join(study_milestone.select("study_milestone_sk",
                                         F.to_date("enrollment_date", "dd-MMM-yyyy").alias("enroll_dt")),
                  study_subject["study_subject_sk_pra"] == study_milestone["study_milestone_sk"], "left")
            .drop("study_milestone_sk")
            .withColumnRenamed("enroll_dt", "enrollment_date"))

        # ── subject_summary (EDC) ─────────────────────────────────────────────
        ss_edc = (edc_ss
            .withColumn("_pid", F.substring("subject_number", -4, 4))
            .select(
                F.col("_pid").alias("participant_id_edc"),
                F.substring("site", 1, 4).alias("site_number_edc"),
                F.col("_pid").alias("participant_number_edc"),
                F.md5(F.col("_pid")).alias("participant_id_sk_edc"),
                F.md5(F.concat("study_id", "_pid")).alias("study_subject_sk_edc"),
                F.col("subject_number").alias("subject_number_edc"),
                F.md5(F.concat("study_id", "_pid")).alias("subject_summary_sk_edc"),
                F.col("race"), F.col("ethnic"),
                F.lit("edc").alias("source_edc"),
                F.col("study_id").alias("study_id_edc"),
                F.md5("study_id").alias("study_id_sk_edc"),
                F.col("subject_status").alias("participant_status_edc"),
                F.col("subject_status").alias("subject_status"),
                F.col("study_phase").alias("study_phase_edc"),
                F.col("tumor_type").alias("tumor_type_edc"),
                F.col("cohort").alias("cohort_edc"),
                F.col("cohort_desc").alias("cohort_desc_edc"),
                F.col("treatment_arm").alias("treatment_arm_edc"),
                F.col("_pid").alias("subject_number"),
                F.col("treatment_arm").alias("dose_level_treatment_arm_edc"),
                _load_date_expr(edc_ss, "load_date_edc"),
            ).distinct())

        # ── subject_summary_report (IRT) ──────────────────────────────────────
        ss_irt = (irt_sr
            .withColumn("_pid",
                F.when(F.col("study_id").isin(*_six_char), F.substring("subject", -6, 6))
                 .otherwise(F.substring("subject", -4, 4)))
            .withColumn("_pid_sk",
                F.when(F.col("study_id").isin(*_six_char), F.md5(F.substring("subject", -6, 6)))
                 .otherwise(F.md5(F.substring("subject", -4, 4))))
            .select(
                F.col("_pid").alias("participant_id_irt"),
                F.col("studysite").alias("site_number_irt"),
                F.col("_pid").alias("participant_number_irt"),
                F.col("_pid_sk").alias("participant_id_sk_irt"),
                F.md5(F.concat("study_id", "_pid")).alias("subject_summary_report_sk_irt"),
                F.col("subject").alias("subject_number_irt"),
                F.md5(F.concat("study_id", F.substring("subject", -4, 4))).alias("study_subject_sk_irt"),
                F.lit("irt").alias("source_irt"),
                F.md5("study_id").alias("study_id_sk_irt"),
                F.col("study_id").alias("study_id_irt"),
                F.col("status").alias("participant_status_irt"),
                F.col("status").alias("status_irt"),
                F.col("study_phase").alias("study_phase_irt"),
                F.col("tumor_type").alias("tumor_type_irt"),
                F.col("cohort").alias("cohort_irt"),
                F.col("cohort_desc").alias("cohort_desc_irt"),
                F.col("treatment_arm").alias("treatment_arm_irt"),
                F.col("_pid_sk").alias("subject"),
                F.col("dose_level_treatment_arm").alias("dose_level_treatment_arm_irt"),
                _load_date_expr(irt_sr, "load_date_irt"),
            ).distinct())

        _eps_tarm = F.col("treatment_arm") if "treatment_arm" in edc_eps.columns else F.lit(None).cast("string")
        _ep_load = _load_date_expr(edc_eps, "load_date_ep")

        ss_early = (edc_eps
            .withColumn("_pid", F.split(F.col("subject"), "-").getItem(2))
            .select(
                F.col("_pid").alias("participant_id_ep"),
                F.col("site_number").alias("site_number_ep"),
                F.col("_pid").alias("participant_number_ep"),
                F.md5(F.col("_pid")).alias("participant_id_sk_ep"),
                F.md5(F.concat("study_id", "_pid")).alias("study_subject_sk_ep"),
                F.md5(F.concat("study_id", "_pid")).alias("subject_summary_sk_ep"),
                F.lit("early_phase").alias("source_ep"),
                F.col("study_id").alias("study_id_ep"),
                F.md5("study_id").alias("study_id_sk_ep"),
                F.col("participant_status").alias("participant_status_ep"),
                F.col("participant_status").alias("status_ep"),
                F.col("participant_type").alias("study_phase_ep"),
                F.split(F.col("cohort"), ":").getItem(1).alias("tumor_type_ep"),
                F.split(F.col("cohort"), ":").getItem(0).alias("cohort_ep"),
                F.col("cohort").alias("cohort_desc_ep"),
                _eps_tarm.alias("treatment_arm_ep"),
                F.lit("").alias("dose_level_treatment_arm_ep"),
                F.col("_pid").alias("subject_number_ep"),
                _ep_load,
            ).distinct())

        # ── participant_master union ──────────────────────────────────────────
        pm = (dim_pra.select(F.col("study_subject_sk_pra").alias("study_subject_sk"))
              .union(ss_edc.select(F.col("subject_summary_sk_edc").alias("study_subject_sk")))
              .union(ss_irt.select(F.col("subject_summary_report_sk_irt").alias("study_subject_sk")))
              .union(ss_early.select(F.col("subject_summary_sk_ep").alias("study_subject_sk")))
              .distinct())

        full = (pm
            .join(ss_edc, pm["study_subject_sk"] == ss_edc["subject_summary_sk_edc"], "left")
            .join(dim_pra, pm["study_subject_sk"] == dim_pra["study_subject_sk_pra"], "left")
            .join(ss_irt, pm["study_subject_sk"] == ss_irt["subject_summary_report_sk_irt"], "left")
            .join(ss_early, pm["study_subject_sk"] == ss_early["subject_summary_sk_ep"], "left"))

        _load = lambda col: F.col(col).cast("date")

        edc_rows = (full.filter(F.col("source_edc") == "edc")
            .select(
                "participant_id_sk_edc","site_number_edc","study_subject_sk_edc",
                "participant_id_edc","participant_number_edc","subject_summary_sk_edc",
                "race","ethnic","enrollment_date",
                "study_id_edc","study_id_sk_edc",
                "participant_status_pra","participant_status_edc","participant_status_irt",
                "study_phase_edc","study_phase_irt","study_phase_ep",
                "tumor_type_edc","tumor_type_irt","tumor_type_ep",
                "cohort_edc","cohort_irt","cohort_ep",
                "treatment_arm_edc","treatment_arm_irt","treatment_arm_ep", "dose_level_treatment_arm_edc",
                "dose_level_treatment_arm_irt","dose_level_treatment_arm_ep",
                "cohort_desc_edc","cohort_desc_irt","cohort_desc_ep",
                "status_irt","status_ep","participant_status_ep",
                "participant_status_reason_pra","subject_number",
                _load("load_date_edc").alias("load_date")))
        edc_rows = (edc_rows
            .withColumnRenamed("participant_id_sk_edc","participant_id_sk")
            .withColumn("site_number", F.col("site_number_edc").cast("string"))
            .withColumnRenamed("study_subject_sk_edc","study_subject_sk")
            .withColumnRenamed("participant_id_edc","participant_id")
            .withColumnRenamed("participant_number_edc","participant_number")
            .withColumnRenamed("subject_summary_sk_edc","subject_summary_sk")
            .withColumnRenamed("study_id_edc","study_id")
            .withColumnRenamed("study_id_sk_edc","study_id_sk")
            .withColumnRenamed("participant_status_reason_pra","participant_status_reason")
            .drop("site_number_edc"))

        pra_rows = (full.filter(F.col("source_pra") == "pra")
            .withColumn("participant_id_sk", F.col("participant_id_sk_pra"))
            .withColumn("site_number", F.col("site_number_pra").cast("string"))
            .withColumn("study_subject_sk", F.col("study_subject_sk2_pra"))
            .withColumn("participant_id", F.col("participant_id_pra"))
            .withColumn("participant_number", F.substring("participant_number_pra", -4, 4))
            .withColumn("subject_summary_sk", F.col("study_subject_sk_pra"))
            .withColumn("study_id", F.col("study_id_pra"))
            .withColumn("study_id_sk", F.col("study_id_sk_pra"))
            .withColumn("participant_status_reason", F.col("participant_status_reason_pra"))
            .withColumn("subject_number", F.col("subject_number_irt"))
            .withColumn("load_date", _load("load_date"))
            .select(edc_rows.columns))

        irt_rows = (full.filter(F.col("source_irt") == "irt")
            .withColumn("participant_id_sk", F.col("participant_id_sk_irt"))
            .withColumn("site_number", F.col("site_number_irt").cast("string"))
            .withColumn("study_subject_sk", F.col("study_subject_sk_irt"))
            .withColumn("participant_id", F.col("participant_id_irt"))
            .withColumn("participant_number", F.col("participant_number_irt"))
            .withColumn("subject_summary_sk", F.col("subject_summary_report_sk_irt"))
            .withColumn("study_id", F.col("study_id_irt"))
            .withColumn("study_id_sk", F.col("study_id_sk_irt"))
            .withColumn("participant_status_reason", F.col("participant_status_reason_pra"))
            .withColumn("subject_number", F.col("subject_number_irt"))
            .withColumn("load_date", _load("load_date_irt"))
            .select(edc_rows.columns))

        early_rows = (full.filter(F.col("source_ep") == "early_phase")
            .withColumn("participant_id_sk", F.col("participant_id_sk_ep"))
            .withColumn("site_number", F.col("site_number_ep").cast("string"))
            .withColumn("study_subject_sk", F.col("study_subject_sk_ep"))
            .withColumn("participant_id", F.col("participant_id_ep"))
            .withColumn("participant_number", F.col("participant_number_ep"))
            .withColumn("subject_summary_sk", F.col("subject_summary_sk_ep"))
            .withColumn("study_id", F.col("study_id_ep"))
            .withColumn("study_id_sk", F.col("study_id_sk_ep"))
            .withColumn("participant_status_pra", F.lit(None).cast("string"))
            .withColumn("participant_status_edc", F.col("participant_status_ep"))
            .withColumn("participant_status_irt", F.lit(None).cast("string"))
            .withColumn("participant_status_reason", F.lit(None).cast("string"))
            .withColumn("subject_number", F.col("subject_number_ep"))
            .withColumn("load_date", _load("load_date_ep"))
            .select(edc_rows.columns))

        dp_union = edc_rows.union(pra_rows).union(irt_rows).union(early_rows)

        # Exclude participants only seen under dummy site 9999
        site9999 = (dp_union.filter(F.coalesce("site_number", F.lit("")) == "9999")
                    .select("study_id","participant_id").distinct())
        dp_filt = dp_union.join(site9999, ["study_id","participant_id"], "left_anti")

        dp = (dp_filt
            .groupBy("participant_id","participant_id_sk","study_subject_sk",
                     "participant_number","study_id","study_id_sk")
            .agg(F.max("race").alias("race"),
                 F.max("ethnic").alias("ethnic"),
                 F.max("enrollment_date").alias("enrollment_date"),
                 F.max("participant_status_pra").alias("participant_status_pra"),
                 F.max("participant_status_edc").alias("participant_status_edc"),
                 F.max("participant_status_irt").alias("participant_status_irt"),
                 F.max("participant_status_reason").alias("participant_status_reason"),
                 F.max("study_phase_irt").alias("study_phase_irt"),
                 F.max("study_phase_edc").alias("study_phase_edc"),
                 F.max("study_phase_ep").alias("study_phase_ep"),
                 F.max("tumor_type_irt").alias("tumor_type_irt"),
                 F.max("tumor_type_edc").alias("tumor_type_edc"),
                 F.max("tumor_type_ep").alias("tumor_type_ep"),
                 F.max("cohort_irt").alias("cohort_irt"),
                 F.max("cohort_edc").alias("cohort_edc"),
                 F.max("cohort_ep").alias("cohort_ep"),
                 F.max("cohort_desc_irt").alias("cohort_desc_irt"),
                 F.max("cohort_desc_edc").alias("cohort_desc_edc"),
                 F.max("cohort_desc_ep").alias("cohort_desc_ep"),
                 F.max("treatment_arm_irt").alias("treatment_arm_irt"),
                 F.max("treatment_arm_edc").alias("treatment_arm_edc"),
                 F.max("treatment_arm_ep").alias("treatment_arm_ep"),
                 F.max("dose_level_treatment_arm_irt").alias("dose_level_treatment_arm_irt"),
                 F.max("dose_level_treatment_arm_edc").alias("dose_level_treatment_arm_edc"),
                 F.max("dose_level_treatment_arm_ep").alias("dose_level_treatment_arm_ep")))

        dp_final = (dp
            .withColumn("study_phase", F.coalesce(F.col("study_phase_irt"), F.col("study_phase_ep"), F.col("study_phase_edc")))
            .withColumn("tumor_type", F.coalesce(F.col("tumor_type_irt"), F.col("tumor_type_ep"), F.col("tumor_type_edc")))
            .withColumn("cohort", F.coalesce(F.col("cohort_irt"), F.col("cohort_ep"), F.col("cohort_edc")))
            .withColumn("cohort_desc", F.coalesce(F.col("cohort_desc_irt"), F.col("cohort_desc_ep"), F.col("cohort_desc_edc")))
            .withColumn("treatment_arm", F.coalesce(F.col("treatment_arm_irt"), F.col("treatment_arm_ep"), F.col("treatment_arm_edc")))
            .withColumn("dose_level_treatment_arm", F.coalesce(F.col("dose_level_treatment_arm_irt"), F.col("dose_level_treatment_arm_ep"), F.col("dose_level_treatment_arm_edc"))))

        phase_rules = phase_rules_tbl.select(
            F.col("study_id").alias("_pr_study_id"),
            F.col("irt_status").alias("_pr_irt_status"),
            F.col("tumor_type").alias("_pr_tumor_type"),
            F.col("study_phase").alias("_pr_study_phase"),
        )

        study_phase = (
            dp_final
            .join(
                phase_rules,
                (dp_final["study_id"] == phase_rules["_pr_study_id"])
                & dp_final["participant_status_irt"].eqNullSafe(phase_rules["_pr_irt_status"])
                & dp_final["tumor_type"].eqNullSafe(phase_rules["_pr_tumor_type"]),
                "left",
            )
            # Keep a single stable column name for phase.
            .withColumn("study_phase_new", F.coalesce(F.col("study_phase"), F.col("_pr_study_phase")))
            .drop("_pr_study_id", "_pr_irt_status", "_pr_tumor_type", "_pr_study_phase")
        )

        csm_tbl = spark.table(f"{cat}.{common}.cohort_summary_mapping").distinct()

        sp = study_phase.alias("sp")
        csm = csm_tbl.alias("csm")

        study_phase_cohort = (
            sp.join(
                csm,
                on=[
                    sp["study_id"] == csm["study_id"],
                    F.coalesce(sp["study_phase"], F.lit("")).eqNullSafe(F.coalesce(csm["study_phase"], F.lit(""))),
                    sp["tumor_type"].eqNullSafe(csm["tumor_type"]),
                    sp["participant_status_irt"].eqNullSafe(csm["status"]),
                    sp["treatment_arm"].eqNullSafe(csm["treatment_arm"]),
                    sp["cohort"].isNull(),
                ],
                how="left",
            )
            .select(
                *[sp[col] for col in sp.columns],
                F.coalesce(sp["cohort"], csm["cohort"]).alias("final_cohort")
            )
            .distinct()
        )

        # cohort / tumor_type overrides from dim_participant_cohort_overrides
        # (moved before cohort_status — overrides applied on study_phase_cohort output)
        cohort_ovr = meta.get("cohort_overrides")
        if cohort_ovr is not None:
            ovr = (
                cohort_ovr.select(
                    "study_id",
                    "rule_priority",
                    "match_study_phase",
                    "match_tumor_type",
                    "match_cohort",
                    F.coalesce("match_cohort_null", F.lit(False)).alias("match_cohort_null"),
                    F.coalesce("match_tumor_null",  F.lit(False)).alias("match_tumor_null"),
                    "tumor_type_override",
                    "cohort_override",
                    "cohort_formula",
                )
            )
            dp_final_with_overrides = (
                study_phase_cohort.join(
                    ovr,
                    (
                        (study_phase_cohort["study_id"] == ovr["study_id"])
                        & (ovr["match_study_phase"].isNull() | (study_phase_cohort["study_phase_new"] == ovr["match_study_phase"]))
                        & (ovr["match_tumor_type"].isNull() | (study_phase_cohort["tumor_type"] == ovr["match_tumor_type"]))
                        & (ovr["match_cohort"].isNull() | (study_phase_cohort["final_cohort"] == ovr["match_cohort"]))
                        & (~ovr["match_cohort_null"] | study_phase_cohort["final_cohort"].isNull())
                        & (~ovr["match_tumor_null"] | study_phase_cohort["tumor_type"].isNull())
                    ),
                    "left",
                )
                .withColumn("_ovr_rn", F.row_number().over(
                    Window.partitionBy(study_phase_cohort["study_id"], "participant_id")
                    .orderBy(F.asc("rule_priority")),
                ))
                .filter((F.col("_ovr_rn") == 1) | F.col("rule_priority").isNull())
                .withColumn("tumor_type", F.coalesce("tumor_type_override", "tumor_type"))
                .withColumn(
                    "cohort",
                    F.when(
                        F.col("cohort_formula") == "COHORT_PREFIX_FIRST_TOKEN",
                        F.concat(F.lit("Cohort "), F.split(F.coalesce(F.col("cohort"), F.col("final_cohort")), " ")[0]),
                    )
                    .when(F.col("cohort_override").isNotNull(), F.col("cohort_override"))
                    .otherwise(F.col("final_cohort")),
                )
                .drop(
                    ovr["study_id"],
                    "rule_priority",
                    "match_study_phase",
                    "match_tumor_type",
                    "match_cohort",
                    "match_cohort_null",
                    "match_tumor_null",
                    "tumor_type_override",
                    "cohort_override",
                    "cohort_formula",
                    "_ovr_rn",
                )
            )
            # display(dp_final_with_overrides.filter(F.col("study_id") == "XB002-101"))
        else:
            dp_final_with_overrides = study_phase_cohort
            print("No cohort_overrides in meta — skipping overrides")

        # 
        raw_extract = F.regexp_extract(
            F.col("cohort_description"),
            r"(?i)^cohort\s+([A-Za-z0-9]+(?:\s*-\s*[0-9]+[A-Za-z]?)?)",
            1
        )
        
        cohort_normalized_expr = F.when(
            raw_extract != "",
            F.concat(F.lit("Cohort "), F.regexp_replace(raw_extract, r"\s*-\s*", "-"))
        ).otherwise(
            F.concat(F.lit("Cohort "), F.split(F.col("cohort_description"), r"\s+").getItem(0))
        )


        cohort_summary_report = (
            irt_csr
            .select("study_id", "cohort_description", "cohort_status")
            # .withColumn(
            #     "cohort_normalized",
            #     F.concat(
            #         F.lit("Cohort "),
            #         F.regexp_extract(F.col("cohort_description"), r"(?i)^cohort\s+([A-Za-z0-9]+)", 1),
            #     ),
            # )
            .withColumn("cohort_normalized", cohort_normalized_expr)
            # .drop("cohort_description")
            .distinct()
        )

        csr = (
            cohort_summary_report
            .withColumn("_csr_cohort_normalized_norm", F.upper(F.trim(F.col("cohort_normalized"))))
            .withColumn("_csr_cohort_status_norm", F.upper(F.trim(F.col("cohort_status"))))
            .groupBy("study_id", "_csr_cohort_normalized_norm")
            .agg(
                F.count(F.lit(1)).alias("_csr_cohort_dup_cnt"),
                F.max(F.when(F.col("_csr_cohort_status_norm") == "OPEN", F.lit(1)).otherwise(F.lit(0))).alias("_csr_has_open"),
                F.max(F.when(F.col("_csr_cohort_status_norm") == "CLOSED", F.lit(1)).otherwise(F.lit(0))).alias("_csr_has_closed"),
                F.first(F.col("_csr_cohort_status_norm"), ignorenulls=True).alias("_csr_any_status"),
            )
            .withColumn(
                "cohort_status_resolved",
                F.when(F.col("_csr_has_open") == 1, F.lit("Open"))
                .when(F.col("_csr_has_closed") == 1, F.lit("Closed"))
                .otherwise(F.initcap(F.col("_csr_any_status"))),
            )
            .alias("csr")
        )
        
        csr_desc = (
            cohort_summary_report
            .withColumn("_csr_cohort_desc_norm", F.upper(F.trim(F.col("cohort_description"))))
            .withColumn("_csr_cohort_status_norm", F.upper(F.trim(F.col("cohort_status"))))
            .groupBy("study_id", "_csr_cohort_desc_norm")
            .agg(
                F.max(F.when(F.col("_csr_cohort_status_norm") == "OPEN", F.lit(1)).otherwise(F.lit(0))).alias("_csr_desc_has_open"),
                F.max(F.when(F.col("_csr_cohort_status_norm") == "CLOSED", F.lit(1)).otherwise(F.lit(0))).alias("_csr_desc_has_closed"),
                F.first(F.col("_csr_cohort_status_norm"), ignorenulls=True).alias("_csr_desc_any_status"),
            )
            .withColumn(
                "cohort_status_resolved_desc",
                F.when(F.col("_csr_desc_has_open") == 1, F.lit("Open"))
                .when(F.col("_csr_desc_has_closed") == 1, F.lit("Closed"))
                .otherwise(F.initcap(F.col("_csr_desc_any_status"))),
            )
            .alias("csr_desc")
        )

        spc = (
            dp_final_with_overrides.alias("spc")
            .withColumn(
                "_spc_cohort_description",
                F.coalesce(F.col("cohort_desc_irt"), F.col("cohort_desc_ep"), F.col("cohort_desc_edc")),
            )
            .withColumn(
                "_spc_has_cohort_description",
                F.length(F.trim(F.coalesce(F.col("_spc_cohort_description"), F.lit("")))) > 0,
            )
            .withColumn("_spc_final_cohort_norm", F.upper(F.trim(F.col("final_cohort"))))
            .withColumn("_spc_cohort_description_norm", F.upper(F.trim(F.col("_spc_cohort_description"))))
        )

        cohort_status = (
            spc
            .join(
                csr,
                (F.col("spc.study_id").eqNullSafe(F.col("csr.study_id")))
                & (F.col("_spc_final_cohort_norm").eqNullSafe(F.col("csr._csr_cohort_normalized_norm"))),
                "left",
            )
            .join(
                csr_desc,
                (F.col("spc.study_id").eqNullSafe(F.col("csr_desc.study_id")))
                & (F.col("_spc_cohort_description_norm").eqNullSafe(F.col("csr_desc._csr_cohort_desc_norm"))),
                "left",
            )
            .withColumn(
                "cohort_status",
                F.when(
                    F.col("csr.study_id").isNotNull()
                    & (
                        (F.col("csr._csr_cohort_dup_cnt") == 1)
                        | (~F.col("_spc_has_cohort_description"))
                    ),
                    F.col("csr.cohort_status_resolved"),
                )
                .when(
                    F.col("csr.study_id").isNotNull()
                    & (F.col("csr._csr_cohort_dup_cnt") > 1)
                    & F.col("_spc_has_cohort_description")
                    & F.col("csr_desc.study_id").isNotNull(),
                    F.col("csr_desc.cohort_status_resolved_desc"),
                )
                .when(
                    F.col("csr.study_id").isNull()
                    & F.col("csr_desc.study_id").isNotNull(),
                    F.col("csr_desc.cohort_status_resolved_desc"),
                )
                .when(
                    F.col("csr.study_id").isNotNull(),
                    F.col("csr.cohort_status_resolved"),
                ),
            )
            .select("spc.*", "cohort_status")
            .drop("_spc_cohort_description", "_spc_has_cohort_description", "_spc_final_cohort_norm", "_spc_cohort_description_norm")
            .drop(
                "study_phase_irt", "study_phase_edc", "study_phase_ep",
                "tumor_type_irt", "tumor_type_edc", "tumor_type_ep", "status_irt",
                "cohort_irt", "cohort_ep", "cohort_edc",
                "cohort_desc_irt", "cohort_desc_ep", "cohort_desc_edc",
                "treatment_arm_irt", "treatment_arm_edc", "treatment_arm_ep", "dose_level_treatment_arm_edc", "dose_level_treatment_arm_ep", "dose_level_treatment_arm_irt",
            )
        )

        # Continue downstream from cohort_status DataFrame.
        dp_final = cohort_status

        dp_final = (
            dp_final
            .withColumn("cohort", F.upper(F.coalesce(F.col("cohort"), F.col("final_cohort"))))
            .withColumn("study_phase",  F.coalesce(F.col("study_phase"), F.col("study_phase_new")))
            # .drop("final_cohort")
        )

        # # cohort_status lookup
        # # Key formula driven by ct_portfolio_source.cohort_status_key_formula:
        # #   SPLIT_COLON          → first token before ":"
        # #   WORDS_12             → first two space-separated words
        # #   WORDS_12_STRIP_COLON → first two words with ":" removed
        # #   COHORT_PREFIX        → "COHORT " + first word
        # #   NULL / absent        → study excluded from cohort_status lookup
        # _w2 = F.concat(F.split("cohort_description"," ")[0], F.lit(" "), F.split("cohort_description"," ")[1])
        # _key_expr_map = {
        #     "SPLIT_COLON":          F.split("cohort_description", ":")[0],
        #     "WORDS_12":             _w2,
        #     "WORDS_12_STRIP_COLON": F.regexp_replace(_w2, ":", ""),
        #     "COHORT_PREFIX":        F.concat(F.lit("COHORT "), F.split("cohort_description"," ")[0]),
        # }
        # _cs_parts = []
        # for _sid, _formula in _cs_key_formula.items():
        #     if _formula not in _key_expr_map:
        #         continue
        #     _cs_parts.append(
        #         irt_csr.filter(F.col("study_id") == _sid)
        #                .filter(F.col("cohort_status").isNotNull())
        #                .withColumn("_ck", _key_expr_map[_formula])
        #                .select("study_id", F.col("_ck").alias("_cs_key"),
        #                        F.col("cohort_status").alias("_cs_raw"))
        #     )

        # if _cs_parts:
        #     cs_all = _cs_parts[0]
        #     for _p in _cs_parts[1:]:
        #         cs_all = cs_all.union(_p)
        #     cs_lkp = (cs_all
        #         .groupBy("study_id","_cs_key")
        #         .agg(F.max(F.when(F.col("_cs_raw") == "Open", 1).otherwise(0)).alias("_open"))
        #         .withColumn("cohort_status", F.when(F.col("_open") == 1, "Open").otherwise("Closed"))
        #         .select("study_id", F.col("_cs_key").alias("_lkp_cohort"), "cohort_status"))
        #     dp_final = (dp_final
        #         .join(cs_lkp,
        #               (dp_final["study_id"] == cs_lkp["study_id"]) &
        #               (F.lower(dp_final["cohort"]) == F.lower(cs_lkp["_lkp_cohort"])),
        #               "left")
        #         .drop(cs_lkp["study_id"]).drop("_lkp_cohort"))
        # else:
        #     # No cohort_status formulas defined (column absent or all NULL) — initialise as NULL
        #     # so the subsequent withColumn below can reference it safely.
        #     dp_final = dp_final.withColumn("cohort_status", F.lit(None).cast("string"))
        # dp_final = dp_final.withColumn("cohort_status",
        #     F.when(F.col("study_id").isin(*_cohort_status_studies), F.col("cohort_status")))

        # missing_pages
        mp = (edc_ss
            .withColumn("_pid", F.substring("subject_number",-4,4))
            .select(F.col("_pid").alias("_mp_pid"),
                    F.md5(F.concat("study_id","_pid")).alias("_mp_ssk"),
                    F.md5("_pid").alias("_mp_psk"),
                    F.md5("study_id").alias("_mp_sisk"),
                    F.col("total_expected_pgs"),
                    F.col("missing_pgs_entry_not_started").alias("missing_pgs"),
                    F.col("entered_pgs")))

        dp_final = (dp_final
            .join(mp,
                  (dp_final["study_id_sk"] == mp["_mp_sisk"]) &
                  (dp_final["study_subject_sk"] == mp["_mp_ssk"]) &
                  (dp_final["participant_id_sk"] == mp["_mp_psk"]),
                  "left")
            .drop("_mp_pid","_mp_ssk","_mp_psk","_mp_sisk"))

        # participant_status_master (Option B)
        lkp = meta.get("participant_status")
        if lkp is not None:
            lkp_sel = lkp.select(
                F.upper("irt_status").alias("_li"),
                F.upper("edc_status").alias("_le"),
                F.upper("ctms_status").alias("_lc"),
                F.col("participant_status_normalized"))
            dp_final = (dp_final
                .withColumn("_iu", F.upper("participant_status_irt"))
                .withColumn("_eu", F.upper("participant_status_edc"))
                .withColumn("_cu", F.upper("participant_status_pra"))
                .join(lkp_sel,
                      F.col("_iu").eqNullSafe(F.col("_li")) &
                      F.col("_eu").eqNullSafe(F.col("_le")) &
                      F.col("_cu").eqNullSafe(F.col("_lc")), "left") 
                .withColumn("participant_status_master",
                    F.coalesce("participant_status_normalized","participant_status_irt"))
                .drop("_iu","_eu","_li","_le","participant_status_normalized", "_cu","_lc"))
        else:
            dp_final = dp_final.withColumn("participant_status_master",
                                            F.col("participant_status_irt"))

        # Final derived columns
        # participant_status_master holds participant_status_normalized from the
        # metadata join above — use it here instead of raw participant_status_edc
        # so these derived flags stay consistent with the normalised status.
        # psm = F.col("participant_status_master")
        # dp_final = (dp_final
        #     .withColumn("actual_participants_list",
        #         F.when(psm.isin(
        #             "On Treatment","Randomized","In Follow-up","Off Study"),
        #             F.col("participant_number")))
        #     .withColumn("participant_status", F.col("participant_status_edc"))
        #     .withColumn("participant_status_ip",
        #         F.when(psm == "On Treatment", F.col("participant_id_sk")))
            # .withColumn("Participant_Status_Master_filter_logic",
            #     psm.isin("Screening","In Screening"))
            # .withColumn("Participant_Status_Master_filter_logic_copy",
            #     psm.isin("Screening","In Screening"))
            # .withColumn("pie_slice_group",
            #     F.when(psm == "Off Study",   "Off Study")
            #      .when(psm == "In Follow-up","In Follow-up")
            #      .when(psm.isin("On Treatment","Randomized"), "Randomized")))

        # cohort_normalized / cohort_group (Option A — per-study grain)
        cm = meta.get("cohort_status")
        if cm is not None:
            cm_sel = cm.select(
                F.col("study_id").alias("_csm_study_id"),
                F.col("irt_cohort"),
                F.col("cohort_normalized"),
                F.col("cohort_group"),
            )
            dp_final = (dp_final
                .join(cm_sel,
                      (dp_final["study_id"] == cm_sel["_csm_study_id"]) &
                      F.lower(dp_final["cohort"]).eqNullSafe(F.lower(cm_sel["irt_cohort"])),
                      "left")
                .withColumn("cohort_normalized", F.coalesce("cohort_normalized", "cohort"))
                .drop("_csm_study_id", "irt_cohort"))

        # treatment_arm_normalized (Option B)
        tam = meta.get("treatment_arm")
        if tam is not None:
            tam_sel = tam.select(F.col("irt_treatment_arm"), F.col("treatment_arm_normalized"))
            dp_final = (dp_final
                .join(tam_sel,
                      F.lower(dp_final["treatment_arm"]).eqNullSafe(F.lower(tam_sel["irt_treatment_arm"])), "left")
                .withColumn("dose_level_treatment_arm",
                    F.coalesce("treatment_arm_normalized","dose_level_treatment_arm"))
                .drop("irt_treatment_arm"))

        return dp_final.distinct()
