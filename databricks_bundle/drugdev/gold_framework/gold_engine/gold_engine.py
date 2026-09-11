"""
Gold Engine
Reads gold_object_registry from metadata tables and executes each registered Gold object.

Supported build strategies:
  PYSPARK_MODEL  — dynamically imports class_path and calls model.build(spark) → DataFrame
  FULL_REFRESH   — executes sql_template SELECT and overwrites target Gold table
  VIEW_DDL       — executes sql_template CREATE OR REPLACE VIEW DDL

Spark conf parameters required:
  drugdev.METADATA_CATALOG   Unity Catalog for metadata tables
  drugdev.METADATA_SCHEMA    Schema holding DQ/ingestion metadata
  drugdev.REGISTRY_SCHEMA    Schema holding gold_object_registry
  drugdev.CATALOG            Unity Catalog for Silver/Gold data tables
  drugdev.SILVER_SCHEMA      Silver schema inside CATALOG
  drugdev.GOLD_SCHEMA        Gold schema inside CATALOG
  drugdev.environment        Runtime environment label (dev/qa/prod)
  drugdev.RUN_DATE           (optional) Run date YYYYMMDD — defaults to today

Entry point:
  main(mode, object_name=None)
    mode: "all"    — build all enabled Gold objects in execution_order
          "object" — build a single object (object_name required)
"""

import importlib
import inspect
import json
import logging
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

_META_DFS_CACHE = None


def _compact_error_message(exc: Exception, max_chars: int = 700) -> str:
    """Return a concise, log-friendly error message without JVM stacktrace noise."""
    raw = str(exc) if exc is not None else ""
    if not raw:
        return exc.__class__.__name__ if exc is not None else "Unknown error"

    for marker in (
        "\n\nJVM stacktrace:",
        "\nJVM stacktrace:",
        "\nTraceback (most recent call last):",
    ):
        idx = raw.find(marker)
        if idx >= 0:
            raw = raw[:idx]

    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if not lines:
        return exc.__class__.__name__ if exc is not None else "Unknown error"

    compact = " | ".join(lines[:3])
    if len(lines) > 3:
        compact += " | ..."

    if len(compact) > max_chars:
        compact = compact[: max_chars - 3] + "..."

    return compact


# ── Spark conf ─────────────────────────────────────────────────────────────────

def _get_conf():
    required_keys = {
        "drugdev.METADATA_CATALOG",
        "drugdev.METADATA_SCHEMA",
        "drugdev.REGISTRY_SCHEMA",
        "drugdev.CATALOG",
        "drugdev.SILVER_SCHEMA",
        "drugdev.GOLD_SCHEMA",
        "drugdev.environment",
    }
    conf = {}
    missing = []
    for key in required_keys:
        val = spark.conf.get(key, "")
        if not val:
            missing.append(key)
        conf[key.split(".")[-1]] = val

    if missing:
        raise ValueError(f"Missing required Spark conf(s): {', '.join(missing)}")

    conf["METADATA_CATALOG"] = spark.conf.get("drugdev.METADATA_CATALOG")
    conf["METADATA_SCHEMA"]  = spark.conf.get("drugdev.METADATA_SCHEMA")
    conf["REGISTRY_SCHEMA"]  = spark.conf.get("drugdev.REGISTRY_SCHEMA")
    conf["CATALOG"]          = spark.conf.get("drugdev.CATALOG")
    conf["SILVER_SCHEMA"]    = spark.conf.get("drugdev.SILVER_SCHEMA")
    conf["GOLD_SCHEMA"]      = spark.conf.get("drugdev.GOLD_SCHEMA")
    conf["environment"]      = spark.conf.get("drugdev.environment")
    conf["RUN_DATE"]         = spark.conf.get(
        "drugdev.RUN_DATE", datetime.now().strftime("%Y%m%d")
    )
    return conf


# ── SQL token resolver ─────────────────────────────────────────────────────────

def _resolve_sql_tokens(sql_template: str, conf: dict) -> str:
    """Replace {catalog}, {silver_schema}, {gold_schema} placeholders in SQL templates."""
    if not sql_template:
        return sql_template
    return (
        sql_template
        .replace("{catalog}", f"`{conf['CATALOG']}`")
        .replace("{silver_schema}", f"`{conf['CATALOG']}`.{conf['SILVER_SCHEMA']}")
        .replace("{gold_schema}", f"`{conf['CATALOG']}`.{conf['GOLD_SCHEMA']}")
        .replace("{metadata_catalog}", f"`{conf['METADATA_CATALOG']}`")
        .replace("{metadata_schema}", f"`{conf['METADATA_CATALOG']}`.{conf['METADATA_SCHEMA']}")
        .replace("{registry_schema}", f"`{conf['METADATA_CATALOG']}`.{conf['REGISTRY_SCHEMA']}")
    )


# ── Metadata query ─────────────────────────────────────────────────────────────

def _gold_object_registry_tbl(conf: dict) -> str:
    return f"`{conf['METADATA_CATALOG']}`.{conf['REGISTRY_SCHEMA']}.gold_object_registry"


def _fetch_objects(conf: dict, object_name: str = None) -> list:
    """Return gold_object_registry rows, ordered by execution_order."""
    tbl = _gold_object_registry_tbl(conf)
    filter_clause = f"AND object_name = '{object_name}'" if object_name else ""
    df = spark.sql(f"""
        SELECT
            object_id,
            object_name,
            object_type,
            build_strategy,
            class_path,
            write_mode,
            execution_order,
            dependencies,
            sql_template,
            partition_cols,
            is_enabled,
            tags
        FROM {tbl}
        WHERE is_enabled = true
          {filter_clause}
        ORDER BY execution_order, object_name
    """)
    return df.collect()


def _group_by_execution_order(rows) -> list[list]:
    """Group objects by execution_order for parallel execution within each wave."""
    groups: dict[int, list] = {}
    for row in rows:
        order = row["execution_order"] or 99
        groups.setdefault(order, []).append(row)
    return [groups[k] for k in sorted(groups)]


# ── Gold table name builder ────────────────────────────────────────────────────

def _gold_table_fqn(conf: dict, object_name: str) -> str:
    return f"`{conf['CATALOG']}`.{conf['GOLD_SCHEMA']}.{object_name}"


def _pipeline_metrics_tbl(conf: dict) -> str:
    return f"`{conf['CATALOG']}`.{conf['METADATA_SCHEMA']}.pipeline_execution_metrics"


def _log_execution_metric(
    conf: dict,
    run_id: str,
    task_name: str,
    status: str,
    start_time: datetime,
    end_time: datetime,
    rows_written: int = 0,
    error_message: str = "",
    error_type: str = "",
) -> None:
    """Persist one Gold object execution event to shared pipeline metrics."""
    duration = int(max((end_time - start_time).total_seconds(), 0))
    row = {
        "run_id": run_id,
        "job_name": "gold_pipeline",
        "task_name": task_name,
        "domain": "",
        "vendor": "",
        "study_id": "",
        "layer": "GOLD",
        "status": status,
        "error_message": (error_message or "")[:500],
        "error_type": (error_type or "")[:120],
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration,
        "execution_timestamp": end_time,
        "execution_date": end_time.date(),
        "records_read": 0,
        "records_written": int(rows_written or 0),
        "files_processed": 0,
        "bytes_processed": 0,
        "checkpoint_path": "",
        "triggered_by": "gold_engine",
    }
    try:
        spark.createDataFrame([row]).write.mode("append").saveAsTable(_pipeline_metrics_tbl(conf))
    except Exception as exc:
        logger.warning("Could not persist Gold execution metric for %s: %s", task_name, _compact_error_message(exc))


def _load_meta_dfs(conf: dict) -> dict:
    """Load/cached common.* metadata DataFrames used by model.build(..., meta=...)."""
    global _META_DFS_CACHE
    if _META_DFS_CACHE is not None:
        return _META_DFS_CACHE

    meta = {}
    common = conf["REGISTRY_SCHEMA"]
    cat = conf["METADATA_CATALOG"]

    meta_tables = {
        "participant_status": f"`{cat}`.{common}.participant_status_master",
        "site_status_master": f"`{cat}`.{common}.site_status_master",
        "cohort_status":      f"`{cat}`.{common}.cohort_status_master",
        "treatment_arm":      f"`{cat}`.{common}.treatment_arm_master",
        "milestone_map":      f"`{cat}`.{common}.milestone_code_map",
        "country_region":     f"`{cat}`.{common}.country_region_mapping",
        "phase_rules":        f"`{cat}`.{common}.dim_participant_phase_rules",
        "cohort_overrides":   f"`{cat}`.{common}.dim_participant_cohort_overrides",
    }

    for key, fqn in meta_tables.items():
        try:
            meta[key] = spark.table(fqn).cache()
        except Exception as exc:
            logger.warning(
                "Gold Engine: could not load meta table %s (%s) - continuing",
                fqn,
                _compact_error_message(exc),
            )

    _META_DFS_CACHE = meta
    return _META_DFS_CACHE


# ── Build strategies ───────────────────────────────────────────────────────────

def _build_pyspark_model(conf: dict, row) -> int:
    """Dynamically import class_path and call model.build(spark, silver, meta) → DataFrame, then write."""
    class_path = row["class_path"]
    if not class_path:
        raise ValueError(
            f"build_strategy=PYSPARK_MODEL requires class_path for '{row['object_name']}'"
        )

    module_path, class_name = class_path.rsplit(".", 1)

    module = None
    tried_paths = [module_path]

    # Compatibility for class_path values like gold.models.* when running with
    # gold_engine on sys.path (Databricks standalone notebook mode).
    if module_path.startswith("gold.models."):
        tried_paths.append(module_path.replace("gold.models.", "models.", 1))

    if module_path.startswith("gold_engine.models."):
        tried_paths.append(module_path.replace("gold_engine.models.", "models.", 1))

    import_errors = []
    for candidate in dict.fromkeys(tried_paths):
        try:
            module = importlib.import_module(candidate)
            break
        except ModuleNotFoundError as exc:
            import_errors.append(f"{candidate}: {exc}")

    if module is None:
        raise ModuleNotFoundError(
            "Could not import model module for class_path="
            f"'{class_path}'. Tried: {', '.join(dict.fromkeys(tried_paths))}. "
            f"Errors: {' | '.join(import_errors)}"
        )

    model_cls = getattr(module, class_name)
    model = model_cls()

    # Current copied models are spark.table-driven and do not consume silver cache yet.
    # Keep this as an explicit dict for signature compatibility.
    silver = {}
    meta = _load_meta_dfs(conf)

    logger.info(f"[{row['object_name']}] Calling {class_path}.build(...)")

    # Choose build() call style by signature instead of catching TypeError.
    # This prevents accidental fallback when a modern build(spark, silver, meta)
    # raises TypeError from its internal logic.
    build_sig = inspect.signature(model.build)
    param_count = len(build_sig.parameters)

    if param_count >= 3:
        result_df = model.build(spark, silver, meta)
    elif param_count == 1:
        logger.warning(
            "[%s] Using legacy build(spark) signature",
            row["object_name"],
        )
        result_df = model.build(spark)
    else:
        raise TypeError(
            f"Unsupported build() signature for {class_path}: {build_sig}. "
            "Expected build(spark) or build(spark, silver, meta)."
        )

    if result_df is None:
        logger.info(f"[{row['object_name']}] Model returned None — skipping write.")
        return 0

    gold_tbl = _gold_table_fqn(conf, row["object_name"])
    write_mode = row["write_mode"] or "overwrite"

    writer = result_df.write.format("delta").mode(write_mode).option("overwriteSchema", "true")

    partition_cols = _parse_json_list(row["partition_cols"])
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.saveAsTable(gold_tbl)
    try:
        _apply_gold_tags(conf, row.asDict(recursive=True), gold_tbl, columns=result_df.columns)
    except Exception as exc:
        logger.warning("[%s] Failed applying Gold tags: %s", row["object_name"], _compact_error_message(exc))
    row_count = result_df.count()
    logger.info(f"[{row['object_name']}] Written {row_count} rows to {gold_tbl}")
    return row_count


def _build_full_refresh(conf: dict, row) -> int:
    """Execute sql_template SELECT and overwrite target Gold table."""
    sql_raw = row["sql_template"]
    if not sql_raw:
        raise ValueError(
            f"build_strategy=FULL_REFRESH requires sql_template for '{row['object_name']}'"
        )

    sql = _resolve_sql_tokens(sql_raw, conf)
    logger.info(f"[{row['object_name']}] Executing FULL_REFRESH SQL")
    result_df = spark.sql(sql)

    gold_tbl = _gold_table_fqn(conf, row["object_name"])
    write_mode = row["write_mode"] or "overwrite"

    writer = result_df.write.format("delta").mode(write_mode).option("overwriteSchema", "true")
    partition_cols = _parse_json_list(row["partition_cols"])
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.saveAsTable(gold_tbl)
    try:
        _apply_gold_tags(conf, row.asDict(recursive=True), gold_tbl, columns=result_df.columns)
    except Exception as exc:
        logger.warning("[%s] Failed applying Gold tags: %s", row["object_name"], _compact_error_message(exc))
    row_count = result_df.count()
    logger.info(f"[{row['object_name']}] Written {row_count} rows to {gold_tbl}")
    return row_count


def _build_view_ddl(conf: dict, row) -> int:
    """Execute sql_template as CREATE OR REPLACE VIEW DDL."""
    sql_raw = row["sql_template"]
    if not sql_raw:
        raise ValueError(
            f"build_strategy=VIEW_DDL requires sql_template for '{row['object_name']}'"
        )

    sql = _resolve_sql_tokens(sql_raw, conf)
    logger.info(f"[{row['object_name']}] Executing VIEW DDL")
    spark.sql(sql)
    logger.info(f"[{row['object_name']}] View created/replaced.")
    return 0


def _parse_json_list(value) -> list:
    if not value:
        return []
    try:
        result = json.loads(value)
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _parse_json_dict(value) -> dict:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _sql_escape(value) -> str:
    return str(value).replace("'", "''")


def _quote_principal_identifier(principal: str) -> str:
    return f"`{str(principal).replace('`', '``')}`"


def _get_unmask_grantees() -> list:
    raw = spark.conf.get("drugdev.PII_UNMASK_GROUP", "pii_unmasked_access")
    tokens = [t.strip() for t in str(raw).split(",") if t and t.strip()]
    if not tokens:
        tokens = ["pii_unmasked_access"]

    grantees = []
    for token in tokens:
        lower_token = token.lower()
        if (
            lower_token.startswith("group:")
            or lower_token.startswith("sp:")
            or lower_token.startswith("spn:")
            or lower_token.startswith("service_principal:")
            or lower_token.startswith("user:")
        ):
            value = token.split(":", 1)[1].strip()
        else:
            value = token
        if value:
            grantees.append(value)

    try:
        runtime_identity = spark.sql("SELECT current_user() AS current_user").collect()[0]["current_user"]
        if runtime_identity:
            grantees.append(runtime_identity)
    except Exception as exc:
        logger.warning("Unable to resolve current_user() for UNMASK grants: %s", _compact_error_message(exc))

    return list(dict.fromkeys(grantees))


def _grant_unmask_on_table(target_fqn: str) -> None:
    """Grant UNMASK on Gold table to configured identities and runtime principal."""
    for grantee in _get_unmask_grantees():
        try:
            principal_id = _quote_principal_identifier(grantee)
            spark.sql(f"GRANT UNMASK ON TABLE {target_fqn} TO {principal_id}")
            logger.info("Granted UNMASK on %s to %s", target_fqn, grantee)
        except Exception as exc:
            logger.warning(
                "Could not grant UNMASK on %s to %s: %s",
                target_fqn,
                grantee,
                _compact_error_message(exc),
            )


def _build_unmask_predicate() -> str:
    """
    Build SQL predicate for identities allowed to see unmasked PII.

    Accepted format in drugdev.PII_UNMASK_GROUP:
    - group names (default): "pii_unmasked_access,clinical_admins"
    - explicit group prefix: "group:clinical_admins"
    - service principals/users: "sp:app-id-or-name" or "user:alice@example.com"
    """
    raw = spark.conf.get("drugdev.PII_UNMASK_GROUP", "pii_unmasked_access")
    tokens = [t.strip() for t in str(raw).split(",") if t and t.strip()]
    if not tokens:
        tokens = ["pii_unmasked_access"]

    group_names = []
    principal_names = []
    for token in tokens:
        lower_token = token.lower()
        if lower_token.startswith("group:"):
            value = token.split(":", 1)[1].strip()
            if value:
                group_names.append(value)
        elif (
            lower_token.startswith("sp:")
            or lower_token.startswith("spn:")
            or lower_token.startswith("service_principal:")
            or lower_token.startswith("user:")
        ):
            value = token.split(":", 1)[1].strip()
            if value:
                principal_names.append(value)
        else:
            group_names.append(token)

    checks = [f"is_account_group_member('{_sql_escape(g)}')" for g in dict.fromkeys(group_names)]
    checks.extend(
        f"lower(current_user()) = lower('{_sql_escape(p)}')"
        for p in dict.fromkeys(principal_names)
    )
    if not checks:
        checks = ["is_account_group_member('pii_unmasked_access')"]
    return " OR ".join(checks)


def _get_column_type_map(target_fqn: str) -> dict:
    try:
        schema = spark.table(target_fqn).schema
        return {f.name.lower(): f.dataType.simpleString().lower() for f in schema.fields}
    except Exception as exc:
        logger.warning("Could not load schema for masking on %s: %s", target_fqn, _compact_error_message(exc))
        return {}


def _ensure_default_string_mask_function(conf: dict) -> str:
    fn_name = spark.conf.get("drugdev.PII_MASK_STRING_FUNCTION", "").strip()
    if fn_name:
        return fn_name

    unmask_predicate = _build_unmask_predicate()
    fqn = f"`{conf['METADATA_CATALOG']}`.{conf['METADATA_SCHEMA']}.mask_pii_string"
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {fqn}(val STRING)
        RETURNS STRING
        RETURN CASE
            WHEN ({unmask_predicate}) THEN val
            WHEN val IS NULL THEN NULL
            ELSE '***MASKED***'
        END
    """)
    return fqn


def _ensure_default_numeric_mask_function(conf: dict, dtype_l: str) -> str:
    unmask_predicate = _build_unmask_predicate()
    fn_meta = {
        "tinyint": ("mask_pii_tinyint", "TINYINT", "CAST(-1 AS TINYINT)"),
        "smallint": ("mask_pii_smallint", "SMALLINT", "CAST(-1 AS SMALLINT)"),
        "int": ("mask_pii_int", "INT", "CAST(-1 AS INT)"),
        "bigint": ("mask_pii_bigint", "BIGINT", "CAST(-1 AS BIGINT)"),
        "float": ("mask_pii_float", "FLOAT", "CAST(-1.0 AS FLOAT)"),
        "double": ("mask_pii_double", "DOUBLE", "CAST(-1.0 AS DOUBLE)"),
    }
    if dtype_l not in fn_meta:
        return ""

    fn_short, sql_type, masked_expr = fn_meta[dtype_l]
    fqn = f"`{conf['METADATA_CATALOG']}`.{conf['METADATA_SCHEMA']}.{fn_short}"
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {fqn}(val {sql_type})
        RETURNS {sql_type}
        RETURN CASE
            WHEN ({unmask_predicate}) THEN val
            WHEN val IS NULL THEN NULL
            ELSE {masked_expr}
        END
    """)
    return fqn


def _ensure_default_decimal_mask_function(conf: dict, dtype_l: str) -> str:
    m = re.match(r"^decimal\((\d+)\s*,\s*(\d+)\)$", dtype_l)
    if not m:
        return ""

    precision = int(m.group(1))
    scale = int(m.group(2))
    if precision < 1 or precision > 38 or scale < 0 or scale > precision:
        return ""

    unmask_predicate = _build_unmask_predicate()
    sql_type = f"DECIMAL({precision},{scale})"
    fn_short = f"mask_pii_decimal_{precision}_{scale}"
    fqn = f"`{conf['METADATA_CATALOG']}`.{conf['METADATA_SCHEMA']}.{fn_short}"
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {fqn}(val {sql_type})
        RETURNS {sql_type}
        RETURN CASE
            WHEN ({unmask_predicate}) THEN val
            WHEN val IS NULL THEN NULL
            ELSE CAST(0 AS {sql_type})
        END
    """)
    return fqn


def _ensure_default_date_mask_function(conf: dict) -> str:
    unmask_predicate = _build_unmask_predicate()
    fqn = f"`{conf['METADATA_CATALOG']}`.{conf['METADATA_SCHEMA']}.mask_pii_date"
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {fqn}(val DATE)
        RETURNS DATE
        RETURN CASE
            WHEN ({unmask_predicate}) THEN val
            WHEN val IS NULL THEN NULL
            ELSE DATE'1900-01-01'
        END
    """)
    return fqn


def _ensure_default_timestamp_mask_function(conf: dict) -> str:
    unmask_predicate = _build_unmask_predicate()
    fqn = f"`{conf['METADATA_CATALOG']}`.{conf['METADATA_SCHEMA']}.mask_pii_timestamp"
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {fqn}(val TIMESTAMP)
        RETURNS TIMESTAMP
        RETURN CASE
            WHEN ({unmask_predicate}) THEN val
            WHEN val IS NULL THEN NULL
            ELSE TIMESTAMP'1900-01-01 00:00:00'
        END
    """)
    return fqn


def _resolve_mask_function(conf: dict, configured_mask_fn: str, dtype: str) -> str:
    if configured_mask_fn:
        return configured_mask_fn
    dtype_l = (dtype or "").lower()
    if dtype_l == "string" or dtype_l.startswith("varchar") or dtype_l.startswith("char"):
        return _ensure_default_string_mask_function(conf)

    if dtype_l in {"tinyint", "smallint", "int", "bigint", "float", "double"}:
        return _ensure_default_numeric_mask_function(conf, dtype_l)

    if dtype_l.startswith("decimal("):
        return _ensure_default_decimal_mask_function(conf, dtype_l)

    if dtype_l == "date":
        return _ensure_default_date_mask_function(conf)

    if dtype_l == "timestamp":
        return _ensure_default_timestamp_mask_function(conf)

    return ""


def _apply_gold_tags(conf: dict, row, target_fqn: str, columns=None) -> None:
    """Apply Gold table and column tags from row.tags JSON."""
    _grant_unmask_on_table(target_fqn)

    tags = _parse_json_dict(row.get("tags"))
    if not tags:
        return

    table_tags = {}
    for key, val in tags.items():
        if key == "column_tags":
            continue
        if isinstance(val, (dict, list)) or val is None:
            continue
        key_s = str(key).strip()
        val_s = str(val).strip()
        if key_s and val_s:
            table_tags[key_s] = val_s

    if table_tags:
        tags_sql = ", ".join(
            f"'{_sql_escape(k)}' = '{_sql_escape(v)}'" for k, v in table_tags.items()
        )
        spark.sql(f"ALTER TABLE {target_fqn} SET TAGS ({tags_sql})")
        logger.info("[%s] Applied Gold table tags: %s", row["object_name"], sorted(table_tags.keys()))

    column_tags = tags.get("column_tags")
    if not isinstance(column_tags, dict) or not column_tags:
        return

    if columns is None:
        try:
            columns = spark.table(target_fqn).columns
        except Exception as exc:
            logger.warning(
                "[%s] Could not load columns for column tag application: %s",
                row["object_name"],
                _compact_error_message(exc),
            )
            return

    cols_lower = {c.lower() for c in columns}
    col_types = _get_column_type_map(target_fqn)
    for col_name, meta in column_tags.items():
        col = str(col_name).strip()
        if not col:
            continue
        if col.lower() not in cols_lower:
            logger.warning(
                "[%s] column_tags column not present in output table %s: %s",
                row["object_name"], target_fqn, col,
            )
            continue
        if not isinstance(meta, dict):
            continue

        col_pairs = []
        sensitivity = meta.get("sensitivity")
        pii_category = meta.get("pii_category")
        configured_mask_fn = str(meta.get("mask_function") or "").strip()
        if sensitivity is not None and str(sensitivity).strip():
            col_pairs.append(("sensitivity", str(sensitivity).strip().upper()))
        if pii_category is not None and str(pii_category).strip():
            col_pairs.append(("pii_category", str(pii_category).strip().upper()))
        if not col_pairs:
            continue

        col_tags_sql = ", ".join(
            f"'{_sql_escape(k)}' = '{_sql_escape(v)}'" for k, v in col_pairs
        )
        spark.sql(
            f"ALTER TABLE {target_fqn} "
            f"ALTER COLUMN `{col}` SET TAGS ({col_tags_sql})"
        )

        try:
            dtype = col_types.get(col.lower(), "")
            mask_fn = _resolve_mask_function(conf, configured_mask_fn, dtype)
            if mask_fn:
                spark.sql(
                    f"ALTER TABLE {target_fqn} "
                    f"ALTER COLUMN `{col}` SET MASK {mask_fn}"
                )
                logger.info("[%s] Applied PII mask for %s using %s", row["object_name"], col, mask_fn)
            else:
                logger.warning(
                    "[%s] Skipping PII mask for %s (unsupported type=%s and no mask_function override)",
                    row["object_name"],
                    col,
                    dtype,
                )
        except Exception as mask_exc:
            logger.warning(
                "[%s] Failed applying PII mask for %s: %s",
                row["object_name"],
                col,
                _compact_error_message(mask_exc),
            )

    logger.info("[%s] Applied Gold column tags for %d column(s)", row["object_name"], len(column_tags))


# ── Single object build ────────────────────────────────────────────────────────

def _build_object(conf: dict, row, run_id: str) -> dict:
    """Build a single Gold object. Returns status dict."""
    obj_name = row["object_name"]
    strategy = (row["build_strategy"] or "").upper()

    logger.info(f"[{obj_name}] Building. strategy={strategy}  order={row['execution_order']}")
    start_time = datetime.utcnow()

    try:
        if strategy == "PYSPARK_MODEL":
            rows_written = _build_pyspark_model(conf, row)
        elif strategy == "FULL_REFRESH":
            rows_written = _build_full_refresh(conf, row)
        elif strategy == "VIEW_DDL":
            rows_written = _build_view_ddl(conf, row)
        else:
            raise ValueError(
                f"Unknown build_strategy '{strategy}' for '{obj_name}'. "
                f"Supported: PYSPARK_MODEL, FULL_REFRESH, VIEW_DDL"
            )
        end_time = datetime.utcnow()
        _log_execution_metric(
            conf=conf,
            run_id=run_id,
            task_name=obj_name,
            status="SUCCESS",
            start_time=start_time,
            end_time=end_time,
            rows_written=rows_written,
        )
        return {"object": obj_name, "status": "success", "rows": rows_written}
    except Exception as exc:
        short_error = _compact_error_message(exc)
        logger.error("[%s] Failed: %s", obj_name, short_error)

        log_stacktrace = spark.conf.get("drugdev.GOLD_LOG_STACKTRACE", "").strip().lower()
        if log_stacktrace in {"1", "true", "yes", "y"}:
            logger.exception("[%s] Stacktrace", obj_name)

        end_time = datetime.utcnow()
        _log_execution_metric(
            conf=conf,
            run_id=run_id,
            task_name=obj_name,
            status="FAILED",
            start_time=start_time,
            end_time=end_time,
            rows_written=0,
            error_message=short_error,
            error_type=exc.__class__.__name__,
        )

        return {
            "object": obj_name,
            "status": "failed",
            "error": short_error,
            "error_type": exc.__class__.__name__,
        }


# ── Main ───────────────────────────────────────────────────────────────────────

def main(
    mode: str = "all",
    object_name: str = None,
    max_workers: int = 4,
    fail_fast: bool = False,
):
    """
    Args:
        mode        : "all" | "object"
        object_name : Required when mode="object"
        max_workers : Thread-pool workers for parallel object builds within each wave
        fail_fast   : If True, stop at first failed wave. Default False continues all waves.
    """
    conf = _get_conf()
    pipeline_run_id = spark.conf.get("drugdev.PIPELINE_RUN_ID", "").strip() or uuid.uuid4().hex

    if mode == "object" and not object_name:
        raise ValueError("object_name is required when mode='object'")

    objects = _fetch_objects(conf, object_name if mode == "object" else None)

    if not objects:
        logger.warning("No enabled Gold objects found for the given parameters.")
        return

    # Optional Spark conf override, while preserving explicit function arg.
    conf_fail_fast = spark.conf.get("drugdev.GOLD_FAIL_FAST", "").strip().lower()
    if conf_fail_fast in {"1", "true", "yes", "y"}:
        fail_fast = True
    elif conf_fail_fast in {"0", "false", "no", "n"}:
        fail_fast = False

    logger.info(
        f"Gold Engine: mode={mode}  objects={len(objects)}  fail_fast={fail_fast}"
    )

    all_results = []

    if mode == "object":
        all_results.append(_build_object(conf, objects[0], pipeline_run_id))
    else:
        # Wave-by-wave: each wave is a group of objects sharing the same execution_order.
        # Within a wave, objects are built in parallel.
        waves = _group_by_execution_order(objects)
        logger.info(f"Execution waves: {len(waves)}")

        for wave_num, wave in enumerate(waves, start=1):
            names = [r["object_name"] for r in wave]
            logger.info(f"Wave {wave_num}/{len(waves)}: {names}")

            wave_results = []

            if len(wave) == 1:
                result = _build_object(conf, wave[0], pipeline_run_id)
                wave_results.append(result)
                all_results.append(result)
            else:
                with ThreadPoolExecutor(
                    max_workers=min(max_workers, len(wave)),
                    thread_name_prefix=f"gold_engine_wave{wave_num}",
                ) as ex:
                    futures = {ex.submit(_build_object, conf, row, pipeline_run_id): row for row in wave}
                    for future in as_completed(futures):
                        result = future.result()
                        wave_results.append(result)
                        all_results.append(result)

            wave_failures = [r for r in wave_results if r.get("status") == "failed"]
            if wave_failures:
                logger.error(
                    "Wave %s had %s failure(s).",
                    wave_num,
                    len(wave_failures),
                )
                if fail_fast:
                    logger.error("Fail-fast enabled. Stopping subsequent waves.")
                    break
                logger.warning("Continuing to next wave because fail_fast=False.")

    # Summary
    success = [r for r in all_results if r.get("status") == "success"]
    failed  = [r for r in all_results if r.get("status") == "failed"]

    logger.info("=" * 60)
    logger.info(f"Gold Engine complete. Total={len(all_results)}")
    logger.info(f"  Success: {len(success)}")
    logger.info(f"  Failed : {len(failed)}")

    if failed:
        for f in failed:
            logger.error("  FAILED: %s - %s", f["object"], f.get("error"))

        grouped: dict[str, list[str]] = {}
        for f in failed:
            key = f.get("error") or "Unknown error"
            grouped.setdefault(key, []).append(f["object"])

        logger.error("Failure Summary (grouped by error):")
        sorted_groups = sorted(grouped.items(), key=lambda kv: (-len(kv[1]), kv[0]))
        for idx, (err_msg, objs) in enumerate(sorted_groups, start=1):
            preview = ", ".join(objs[:8])
            suffix = "" if len(objs) <= 8 else f", ... (+{len(objs) - 8} more)"
            logger.error("  %s) %s object(s): %s%s", idx, len(objs), preview, suffix)
            logger.error("     Error: %s", err_msg)

        raise RuntimeError(f"Gold Engine: {len(failed)} object(s) failed.")
