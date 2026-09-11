# src/gold/gold_model.py
# =============================================================================
# Abstract base class for all Gold layer objects (PySpark model approach).
#
# USAGE
# ─────
# Every Gold object is a Python class that:
#   1. Extends GoldModel.
#   2. Sets class-level metadata (name, write_mode, order, dependencies).
#   3. Implements build(spark, silver, meta) → DataFrame.
#
# The GoldEngine discovers class_path from gold_object_registry, imports the
# class via importlib, and calls model.build(spark, silver, meta).
#
# SILVER DICT KEYS  (injected by engine)
# ───────────────────────────────────────
#   'irt.irt_subject_summary_report'   silver IRT subject summary
#   'edc.edc_subject_summary'          silver EDC subject summary
#   'edc.edc_subject_status'           silver EDC subject status
#   'ctms.ctms_study_site'             silver CTMS study site
#   'ctms.ctms_study_subject'          silver CTMS study subject
#   'ctms.irt_cohort_summary_report'   silver IRT cohort summary (in CTMS domain)
#
# META DICT KEYS  (injected by engine)
# ─────────────────────────────────────
#   'participant_status'   common.participant_status_master  (Option B — no study_id)
#   'site_status_master'   common.site_status_master         (group_name column drives boolean site flags)
#   'cohort_status'        common.cohort_status_master       (Option B — no study_id)
#   'treatment_arm'        common.treatment_arm_master       (Option B — no study_id)
#   'study_config'         common.ct_portfolio_source
#   'milestone_map'        common.milestone_code_map
#   'cohort_parse'         common.study_cohort_parse_config
#   'tf_plus_tumors'       common.study_tf_plus_tumors
#   'country_region'       common.country_region_mapping
# =============================================================================

import logging
from abc import ABC, abstractmethod
from typing import Dict

from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


class GoldModel(ABC):
    """Abstract base class for all Gold layer Python model objects."""

    # ── Subclass MUST override these ─────────────────────────────────────────
    name:         str         # matches gold_object_registry.object_name
    write_mode:   str         # FULL_REFRESH | INCREMENTAL_APPEND | VIEW_DDL
    order:        int         # execution order (lower = first)

    # ── Subclass MAY override ─────────────────────────────────────────────────
    dependencies: list = []
    tags:         dict  = {}
    class_path:   str   = ""  # e.g. 'gold.models.dim_participant.DimParticipantModel'

    @abstractmethod
    def build(
        self,
        spark:  SparkSession,
        silver: Dict[str, DataFrame],
        meta:   Dict[str, DataFrame],
    ) -> DataFrame:
        """
        Build and return the Gold DataFrame for this object.

        Args:
            spark:  Active SparkSession.
            silver: Cached silver DataFrames, keyed by 'domain.table_name'.
            meta:   Cached common.* metadata DataFrames.

        Returns:
            DataFrame that will be written to gold.<name> by the engine.
            For VIEW_DDL objects: call spark.sql('CREATE OR REPLACE VIEW ...')
            and return an empty DataFrame (engine reads count=0).
        """
        ...

    # ── Helpers available to all subclasses ───────────────────────────────────

    def _resolve_tokens(self, sql: str, tokens: dict) -> str:
        """Replace {token} placeholders in a SQL string."""
        for key, val in tokens.items():
            sql = sql.replace(key, val)
        return sql

    def _build_tokens(self, spark: SparkSession) -> dict:
        """Return the standard {token} → FQN mapping for SQL templates."""
        from utils.config_loader import (
            _quoted_catalog, _gold_schema, _common_schema,
            _load_domain_registry, _domain_schema_cache,
            REGISTRY, METRICS, TRACKING, DQ_RESULTS,
        )
        _load_domain_registry(spark)
        cat = _quoted_catalog(spark)
        tokens = {
            "{catalog}":  cat,
            "{gold}":     f"{cat}.{_gold_schema(spark)}",
            "{common}":   f"{cat}.{_common_schema(spark)}",
            "{reg}":      REGISTRY(),
            "{metrics}":  METRICS(),
            "{tracking}": TRACKING(),
            "{dq}":       DQ_RESULTS(),
        }
        for domain, info in _domain_schema_cache.items():
            tokens[f"{{silver.{domain}}}"] = f"{cat}.{info['silver_schema']}"
            tokens[f"{{bronze.{domain}}}"] = f"{cat}.{info['bronze_schema']}"
        return tokens
