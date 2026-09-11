# src/gold/models/fct_participant_visit.py
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from gold_model import GoldModel


class FctParticipantVisitModel(GoldModel):
    name         = "fct_participant_visit"
    write_mode   = "overwrite"
    dependencies = []
    class_path   = "gold.models.fct_participant_visit.FctParticipantVisitModel"

    def build(self, spark: SparkSession, silver: Dict[str, DataFrame], meta: Dict[str, DataFrame]) -> DataFrame:
        from utils.config_loader import _quoted_catalog, _common_schema, _silver_schema
        cat    = _quoted_catalog(spark)
        common = _common_schema(spark)
        ctms   = _silver_schema( spark)
        irt    = _silver_schema(spark)
        edc    = _silver_schema(spark)

        ivsr = spark.table(f"{cat}.{irt}.irt_subject_visit_summary_report")
        ess  = spark.table(f"{cat}.{edc}.edc_subject_summary_by_visit")
        sc   = spark.table(f"{cat}.{ctms}.ctms_study_country")
        epv  = spark.table(f"{cat}.{ctms}.edc_early_phase_subject_visit")
        ssi  = spark.table(f"{cat}.{ctms}.ctms_study_site")
        ssp  = spark.table(f"{cat}.{ctms}.ctms_study_site_phase")

        ess_cols = set(ess.columns)
        epv_cols = set(epv.columns)

        def _safe_date(col_name: str) -> F.Column:
            # Handle mixed date formats that can appear across studies/vendors.
            return F.coalesce(
                F.to_date(F.col(col_name), "ddMMMyyyy"),
                F.to_date(F.col(col_name), "dd-MMM-yy"),
                F.to_date(F.col(col_name), "yyyy-MM-dd"),
                F.col(col_name).cast("date"),
            )

        # ep_participant_col = (
        #     "participant_number"
        #     if "participant_id" in epv_cols
        #     else ("subject_number" if "subject_number" in epv_cols else None)
        # )
        # ep_site_col = (
        #     "site_number"
        #     if "site_number" in epv_cols
        #     else ("site_name" if "site_name" in epv_cols else None)
        # )
        # ep_actual_col = "date_of_visit_actual" if "date_of_visit_actual" in epv_cols else None

        # ── Config driven by ct_portfolio_source ──────────────────────────────
        _cps = spark.table(f"{cat}.{common}.ct_portfolio_source").select(
            "study_id", "participant_id_length", "site_id_source", "visit_date_format"
        )

        # Studies that use a 6-character participant ID
        _six_char_studies = tuple(
            r.study_id
            for r in _cps.filter(F.col("participant_id_length") == 6).select("study_id").collect()
        ) or ("__no_six_char__",)

        # Studies where site_id is taken from the IRT studysite column directly
        _studysite_studies = tuple(
            r.study_id
            for r in _cps.filter(F.col("site_id_source") == "STUDYSITE").select("study_id").collect()
        ) or ("__no_studysite__",)

        # Studies that require a non-default IRT date format (e.g. "yyyy-MM-dd")
        _nondefault_date_formats = {
            r.study_id: r.visit_date_format
            for r in _cps.filter(F.col("visit_date_format").isNotNull())
            .select("study_id", "visit_date_format").collect()
        }

        def _date_expr(col_name: str, study_col: str = "study_id") -> F.Column:
            """Return a date Column applying per-study format overrides."""
            result = _safe_date(col_name)
            for sid, fmt in _nondefault_date_formats.items():
                result = F.when(F.col(study_col) == sid, F.to_date(F.col(col_name), fmt)).otherwise(result)
            return result

        # ── Derived columns ───────────────────────────────────────────────────
        part_num = (
            F.when(F.col("study_id").isin(*_six_char_studies), F.substring(F.col("subject"), -6, 6))
             .otherwise(F.substring(F.col("subject"), -4, 4)))

        site_id = (
            F.when(F.col("study_id").isin(*_studysite_studies), F.col("studysite"))
             .when(F.length(F.col("subject")) >= 9, F.split(F.col("subject"), "-")[0])
             .when(F.length(F.col("subject")) < 5,  F.substring(F.col("studysite"), -4, 4)))

        vr = (ivsr.select(
            _date_expr("scheduled_date").alias("date_of_visit_scheduled"),
            _date_expr("actual_date").alias("date_of_visit_actual"),
            "visit_description",
            "country",
            F.md5(part_num).alias("participant_id_sk"),
            F.md5(part_num).alias("participant_number_sk"),
            part_num.alias("participant_number"),
            site_id.alias("site_id"),
            "study_id", F.md5("study_id").alias("study_id_sk"),
        ))

        sbv = (ess.join(sc, (F.upper(ess['country']) == F.upper(sc['country_name'])) & (ess["study_id"] == sc["study_id"]), "left")
               .select(
                   sc["study_country_id"],
                   ess["participant_number"],
                   F.md5(ess["participant_number"]).alias("participant_id_sk"),
                   F.md5(ess["participant_number"]).alias("participant_number_sk"),
                #    F.when(ess["study_id"].isin(*_studysite_studies),
                #           F.md5(F.concat(ess["study_id"], F.substring(ess["site"], 1, 4))))
                #     .otherwise(F.md5(F.concat(ess["study_id"], F.substring(ess["subject_number"], 8, 4))))
                #     .alias("study_site_id_sk"),
                    F.when((ess["study_id"].isin(*_studysite_studies)) |
                        ((F.substring(ess["site"], 1, 4).isNotNull()) & (F.length(F.trim(F.substring(ess["site"], 1, 4))) > 0)
                        ),F.md5(F.concat(ess["study_id"], F.substring(ess["site"], 1, 4))))
                    .otherwise(F.md5(F.concat(ess["study_id"], F.substring(ess["subject_number"], 8, 4))) )
                    .alias("study_site_id_sk"),
                   F.md5(ess["study_id"]).alias("study_id_sk"),
                   F.md5(F.concat(sc["study_country_id"], ess["study_id"])).alias("study_country_id_sk"),
                   ess["study_id"],
                   _safe_date("date_of_first_icf").alias("date_of_first_icf"),
                   _safe_date("enroll_or_screen_fail_date").alias("randomization_or_screen_fail_date"),
                   ess["protocol_version_randomized"], ess["treatment_arm"],
                   F.col("most_recent_visit").alias("most_recent_visit_type"),
                   _safe_date("most_recent_date_of_visit").alias("most_recent_date_of_visit"),
                   _safe_date("eotxb_date").alias("xl092_eot_date"),
                   _safe_date("nivolumab_eot_date").alias("nivolumab_eot_date"),
                   _safe_date("sunitinib_eot_date").alias("sunitinib_eot_date"),
                   _safe_date("death_date").alias("death_date"),
                   _safe_date("eorfup_date").alias("eoruf_date"),
                   ess["eos_reason"],
                   (F.col("visit_name") if "visit_name" in ess_cols else F.lit(None).cast("string")).alias("visit_name"),
               ))

        ep = (
            epv.alias("ep")
            .join(
                ssi.alias("ss"),
                (
                    (F.col("ep.study_id") == F.col("ss.study_id"))
                    & (F.col("ep.site_number") == F.col("ss.site_number"))
                ),
                "left",
            )
            .select(
                _date_expr("ep.date_of_visit_scheduled", "ep.study_id").alias("date_of_visit_scheduled"),
                _date_expr("ep.date_of_visit_actual", "ep.study_id").alias("date_of_visit_actual"),
                F.col("ep.visit_description").alias("visit_description"),
                F.md5(F.col("ep.participant_id")).alias("participant_id_sk"),
                F.md5(F.col("ep.participant_id")).alias("participant_number_sk"),
                F.col("ep.participant_id").alias("participant_number"),
                F.md5(F.concat(F.col("ep.study_id"), F.col("ep.site_number"))).alias("study_site_id_sk"),
                F.col("ss.study_country_id").alias("study_country_id"),
                F.md5(F.concat(F.col("ss.study_country_id"), F.col("ep.study_id"))).alias("study_country_id_sk"),
                F.col("ep.study_id").alias("study_id"),
                F.md5(F.col("ep.study_id")).alias("study_id_sk"),
            )
            .distinct())

        fct_part_visit = (
            vr.alias("sr")
            .join(
                sc.alias("sc"),
                (F.col("sr.country") == F.col("sc.country_name"))
                & (F.col("sr.study_id") == F.col("sc.study_id")),
                "left",
            )
            .join(
                sbv.alias("sv"),
                (F.col("sr.participant_id_sk") == F.col("sv.participant_number_sk"))
                & (F.col("sr.study_id_sk") == F.col("sv.study_id_sk")),
                "left",
            )
            .select(
                F.col("sr.participant_id_sk").alias("participant_id_sk_sr"),
                F.md5(F.concat(F.col("sr.study_id"), F.col("sr.site_id"))).alias("study_site_id_sk_sr"),
                F.col("sr.study_id").alias("study_id_sr"),
                F.col("sr.study_id_sk").alias("study_id_sk_sr"),
                F.col("sr.participant_number_sk").alias("participant_number_sk_sr"),
                F.col("sr.participant_number").alias("participant_number_sr"),
                F.col("sc.study_country_id").alias("study_country_id"),
                F.md5(F.concat(F.col("sc.study_country_id"), F.col("sr.study_id"))).alias("study_country_id_sk_sr"),
                F.col("sr.date_of_visit_scheduled"),
                F.col("sr.date_of_visit_actual"),
                F.col("sr.visit_description"),
                F.col("sv.date_of_first_icf"),
                F.col("sv.randomization_or_screen_fail_date"),
                F.col("sv.protocol_version_randomized"),
                F.col("sv.treatment_arm"),
                F.col("sv.most_recent_visit_type"),
                F.col("sv.most_recent_date_of_visit"),
                F.col("sv.xl092_eot_date"),
                F.col("sv.nivolumab_eot_date"),
                F.col("sv.sunitinib_eot_date"),
                F.col("sv.death_date"),
                F.col("sv.eoruf_date"),
                F.col("sv.eos_reason"),
            )
            .distinct()
        )

        base_visits = (
            fct_part_visit
            .filter(F.col("participant_id_sk_sr").isNotNull())
            .select(
                F.col("participant_id_sk_sr").alias("participant_id_sk"),
                F.col("study_site_id_sk_sr").alias("study_site_id_sk"),
                F.col("study_country_id_sk_sr").alias("study_country_id_sk"),
                F.col("study_id_sr").alias("study_id"),
                F.col("study_id_sk_sr").alias("study_id_sk"),
                F.col("participant_number_sk_sr").alias("participant_number_sk"),
                F.col("participant_number_sr").alias("participant_number"),
                F.col("study_country_id"),
                F.col("date_of_visit_scheduled").cast("date").alias("date_of_visit_scheduled"),
                F.col("date_of_visit_actual").cast("date").alias("date_of_visit_actual"),
                F.col("visit_description"),
                F.col("date_of_first_icf"),
                F.col("randomization_or_screen_fail_date"),
                F.col("protocol_version_randomized"),
                F.col("treatment_arm"),
                F.col("most_recent_visit_type"),
                F.col("most_recent_date_of_visit"),
                F.col("xl092_eot_date"),
                F.col("nivolumab_eot_date"),
                F.col("sunitinib_eot_date"),
                F.col("death_date"),
                F.col("eoruf_date"),
                F.col("eos_reason"),
            )
            .distinct()
        )

        ep_enriched = (
            ep.alias("ep")
            .join(
                sbv.alias("sv"),
                (F.col("ep.participant_number") == F.col("sv.participant_number"))
                & (F.col("ep.study_id") == F.col("sv.study_id")),
                "left",
            )
            .select(
                F.col("ep.participant_id_sk").alias("participant_id_sk"),
                F.col("ep.study_site_id_sk").alias("study_site_id_sk"),
                F.col("ep.study_country_id_sk").alias("study_country_id_sk"),
                F.col("ep.study_id").alias("study_id"),
                F.col("ep.study_id_sk").alias("study_id_sk"),
                F.col("ep.participant_number_sk").alias("participant_number_sk"),
                F.col("ep.participant_number").alias("participant_number"),
                F.col("ep.study_country_id").alias("study_country_id"),
                F.col("ep.date_of_visit_scheduled").alias("date_of_visit_scheduled"),
                F.col("ep.date_of_visit_actual").alias("date_of_visit_actual"),
                F.col("ep.visit_description").alias("visit_description"),
                F.col("sv.date_of_first_icf").alias("date_of_first_icf"),
                F.col("sv.randomization_or_screen_fail_date").alias("randomization_or_screen_fail_date"),
                F.col("sv.protocol_version_randomized").alias("protocol_version_randomized"),
                F.col("sv.treatment_arm").alias("treatment_arm"),
                F.col("sv.most_recent_visit_type").alias("most_recent_visit_type"),
                F.col("sv.most_recent_date_of_visit").alias("most_recent_date_of_visit"),
                F.col("sv.xl092_eot_date").alias("xl092_eot_date"),
                F.col("sv.nivolumab_eot_date").alias("nivolumab_eot_date"),
                F.col("sv.sunitinib_eot_date").alias("sunitinib_eot_date"),
                F.col("sv.death_date").alias("death_date"),
                F.col("sv.eoruf_date").alias("eoruf_date"),
                F.col("sv.eos_reason").alias("eos_reason"),
            )
            .distinct()
        )

        ep_unique = (
            ep_enriched.alias("ep")
            .join(
                base_visits.select("study_id", "participant_number").distinct().alias("bv"),
                (F.col("ep.study_id") == F.col("bv.study_id"))
                & (F.col("ep.participant_number") == F.col("bv.participant_number")),
                "left_anti",
            )
            .select(
                F.col("ep.participant_id_sk").alias("participant_id_sk"),
                F.col("ep.study_site_id_sk").alias("study_site_id_sk"),
                F.col("ep.study_country_id_sk").alias("study_country_id_sk"),
                F.col("ep.study_id").alias("study_id"),
                F.col("ep.study_id_sk").alias("study_id_sk"),
                F.col("ep.participant_number_sk").alias("participant_number_sk"),
                F.col("ep.participant_number").alias("participant_number"),
                F.col("ep.study_country_id").alias("study_country_id"),
                # Preserve the SQL behavior where early-phase scheduled/actual are swapped.
                F.col("ep.date_of_visit_actual").cast("date").alias("date_of_visit_scheduled"),
                F.col("ep.date_of_visit_scheduled").cast("date").alias("date_of_visit_actual"),
                F.col("ep.visit_description").alias("visit_description"),
                F.col("ep.date_of_first_icf").alias("date_of_first_icf"),
                F.col("ep.randomization_or_screen_fail_date").alias("randomization_or_screen_fail_date"),
                F.col("ep.protocol_version_randomized").alias("protocol_version_randomized"),
                F.col("ep.treatment_arm").alias("treatment_arm"),
                F.col("ep.most_recent_visit_type").alias("most_recent_visit_type"),
                F.col("ep.most_recent_date_of_visit").alias("most_recent_date_of_visit"),
                F.col("ep.xl092_eot_date").alias("xl092_eot_date"),
                F.col("ep.nivolumab_eot_date").alias("nivolumab_eot_date"),
                F.col("ep.sunitinib_eot_date").alias("sunitinib_eot_date"),
                F.col("ep.death_date").alias("death_date"),
                F.col("ep.eoruf_date").alias("eoruf_date"),
                F.col("ep.eos_reason").alias("eos_reason"),
            )
            .distinct()
        )

        fct_participant_visit = base_visits.unionByName(ep_unique).distinct()

        summary_only = (
            sbv.alias("sv")
            .join(
                fct_participant_visit.select("study_id", "participant_number").distinct().alias("fpv"),
                (F.col("sv.study_id") == F.col("fpv.study_id"))
                & (F.col("sv.participant_number") == F.col("fpv.participant_number")),
                "left",
            )
            .select(
                F.col("sv.participant_id_sk").alias("participant_id_sk"),
                F.col("sv.study_site_id_sk").alias("study_site_id_sk"),
                F.col("sv.study_country_id_sk").alias("study_country_id_sk"),
                F.col("sv.study_id").alias("study_id"),
                F.col("sv.study_id_sk").alias("study_id_sk"),
                F.col("sv.participant_number_sk").alias("participant_number_sk"),
                F.col("sv.participant_number").alias("participant_number"),
                F.col("sv.study_country_id").alias("study_country_id"),
                F.lit(None).cast("date").alias("date_of_visit_scheduled"),
                F.lit(None).cast("date").alias("date_of_visit_actual"),
                F.col("sv.visit_name").alias("visit_description"),
                F.col("sv.date_of_first_icf"),
                F.col("sv.randomization_or_screen_fail_date"),
                F.col("sv.protocol_version_randomized"),
                F.col("sv.treatment_arm"),
                F.col("sv.most_recent_visit_type"),
                F.col("sv.most_recent_date_of_visit"),
                F.col("sv.xl092_eot_date"),
                F.col("sv.nivolumab_eot_date"),
                F.col("sv.sunitinib_eot_date"),
                F.col("sv.death_date"),
                F.col("sv.eoruf_date"),
                F.col("sv.eos_reason"),
            )
            .distinct()
        )

        fct_participant_visit_final = fct_participant_visit.unionByName(summary_only).distinct()

        site_info = (
            ssi.select(
                F.col("study_id").alias("study_id"),
                F.md5(F.col("study_id")).alias("study_id_sk"),
                F.md5(F.concat(F.col("study_id"), F.substring(F.col("site_number"), 1, 4))).alias("study_site_id_sk"),
            )
            .unionByName(
                ssp.select(
                    F.col("study_id").alias("study_id"),
                    F.md5(F.col("study_id")).alias("study_id_sk"),
                    F.md5(F.concat(F.col("study_id"), F.col("site_number"))).alias("study_site_id_sk"),
                )
            )
            .distinct()
        )

        visit_type = (
            F.when(F.col("visit_description").isin("Randomization","Randomized","Day 1","W1D1"), "Enrollment")
             .when(F.col("visit_description") == "Screening", "Screening")
             .otherwise("Other"))

        return (
            fct_participant_visit_final.alias("fpv")
            .join(
                site_info.select("study_id_sk", "study_site_id_sk").distinct().alias("si"),
                (F.col("fpv.study_id_sk") == F.col("si.study_id_sk"))
                & (F.col("fpv.study_site_id_sk") == F.col("si.study_site_id_sk")),
                "left",
            )
            .select(
                F.col("fpv.participant_id_sk"),
                F.col("fpv.study_site_id_sk"),
                F.col("fpv.study_country_id_sk"),
                F.col("fpv.study_id"),
                F.col("fpv.participant_number_sk"),
                F.col("fpv.participant_number"),
                F.col("fpv.study_country_id"),
                F.col("fpv.date_of_visit_scheduled"),
                F.col("fpv.date_of_visit_actual"),
                F.col("fpv.visit_description"),
                F.col("fpv.date_of_first_icf"),
                F.col("fpv.randomization_or_screen_fail_date"),
                F.col("fpv.protocol_version_randomized"),
                F.col("fpv.treatment_arm"),
                F.col("fpv.most_recent_visit_type"),
                F.col("fpv.most_recent_date_of_visit"),
                F.col("fpv.xl092_eot_date"),
                F.col("fpv.nivolumab_eot_date"),
                F.col("fpv.sunitinib_eot_date"),
                F.col("fpv.death_date"),
                F.col("fpv.eoruf_date"),
                F.col("fpv.eos_reason"),
                F.col("fpv.study_id_sk"),
                F.last_day(F.col("fpv.date_of_visit_actual")).cast("date").alias("month_end_date"),
                visit_type.alias("visit_type_group"),
                F.lit(True).alias("is_active_bi_study"),
            )
            .distinct()
        )