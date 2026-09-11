"""Silver metadata loader.

Supports two input modes:
1) Split YAML mode (preferred):
     - drugdev.YML_SILVER_VENDOR_CONFIG_PATH
     - drugdev.YML_SILVER_ENTITY_CONFIG_PATH
     The loader reads all ingestion datasets from dataset_registry and enriches
     them with Silver metadata from both YAMLs.

2) Legacy merged YAML mode (backward compatible):
     - drugdev.YML_SILVER_CONFIG_PATH
"""

import json
import logging
import re
import uuid
from datetime import datetime
from typing import Any

import yaml
from pyspark.dbutils import DBUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark   = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Spark conf ─────────────────────────────────────────────────────────────────
METADATA_CATALOG = spark.conf.get("drugdev.METADATA_CATALOG")
METADATA_SCHEMA  = spark.conf.get("drugdev.METADATA_SCHEMA")
REGISTRY_SCHEMA  = spark.conf.get("drugdev.REGISTRY_SCHEMA")
YML_CONFIG_PATH  = spark.conf.get("drugdev.YML_SILVER_CONFIG_PATH")
YML_VENDOR_PATH  = spark.conf.get("drugdev.YML_SILVER_VENDOR_CONFIG_PATH")
YML_ENTITY_PATH  = spark.conf.get("drugdev.YML_SILVER_ENTITY_CONFIG_PATH")
source_bucket    = spark.conf.get("drugdev.source_bucket")
environment      = spark.conf.get("drugdev.environment")
CATALOG          = spark.conf.get("drugdev.CATALOG")
SILVER_SCHEMA    = spark.conf.get("drugdev.SILVER_SCHEMA")

for key, val in {
    "drugdev.METADATA_CATALOG":       METADATA_CATALOG,
    "drugdev.METADATA_SCHEMA":        METADATA_SCHEMA,
    "drugdev.REGISTRY_SCHEMA":        REGISTRY_SCHEMA,
    "drugdev.CATALOG":                CATALOG,
    "drugdev.SILVER_SCHEMA":          SILVER_SCHEMA,
}.items():
    if not val:
        raise ValueError(f"Missing required Spark conf: {key}")

if not ((YML_VENDOR_PATH and YML_ENTITY_PATH) or YML_CONFIG_PATH):
    raise ValueError(
        "Provide either split Silver YAML confs "
        "(drugdev.YML_SILVER_VENDOR_CONFIG_PATH + drugdev.YML_SILVER_ENTITY_CONFIG_PATH) "
        "or legacy conf (drugdev.YML_SILVER_CONFIG_PATH)."
    )

logger.info("METADATA_CATALOG : %s", METADATA_CATALOG)
logger.info("METADATA_SCHEMA  : %s", METADATA_SCHEMA)
logger.info("CATALOG          : %s", CATALOG)
logger.info("SILVER_SCHEMA    : %s", SILVER_SCHEMA)
logger.info("Silver config mode: %s", "split" if (YML_VENDOR_PATH and YML_ENTITY_PATH) else "legacy")

# ── Table FQNs ─────────────────────────────────────────────────────────────────
SILVER_REGISTRY_TBL  = f"`{METADATA_CATALOG}`.{METADATA_SCHEMA}.drugdev_silver_registry"
DATASET_REGISTRY_TBL = f"`{METADATA_CATALOG}`.{REGISTRY_SCHEMA}.dataset_registry"
DATASET_TAGS_TBL     = f"`{METADATA_CATALOG}`.{REGISTRY_SCHEMA}.dataset_tags"

# ── DDL ────────────────────────────────────────────────────────────────────────

CREATE_REGISTRY_DDL = f"""
CREATE TABLE IF NOT EXISTS {SILVER_REGISTRY_TBL} (
    registry_id          STRING      COMMENT 'UUID primary key',
    domain               STRING      COMMENT 'Lowercase domain: irt, edc, ctms',
    entity               STRING      COMMENT 'Entity name without domain prefix: study_site',
    vendor               STRING      COMMENT 'Vendor slug: pra, medidata, 4g, etc.',
    study_id             STRING      COMMENT 'Study identifier: XL092-001',
    source_dataset_name  STRING      COMMENT 'Bronze table name matching ingestion dataset_name: 4g_xb002_101_subject_summary_report',
    silver_table_name    STRING      COMMENT 'Resolved Silver FQN: catalog.schema.ctms_study_site',
    table_sensitivity    STRING      COMMENT 'sensitivity level: public, internal, confidential, restricted',
    pii_columns          STRING      COMMENT 'JSON array of PII column names',
    scd_type             STRING      COMMENT 'scd2 or full_refresh',
    scd_business_keys    STRING      COMMENT 'JSON array of business key columns',
    canonical_cols       STRING      COMMENT 'JSON array of canonical column names',
    zorder_cols          STRING      COMMENT 'JSON array of ZORDER columns',
    mapping_rules        STRING      COMMENT 'JSON map: canonical_col -> vendor_col (Flow 1 direction)',
    transformation_logic STRING      COMMENT 'SQL expression block for selectExpr',
    cross_entity_sql     STRING      COMMENT 'Cross-entity enrichment SQL or null',
    validation_rules     STRING      COMMENT 'Validation rules from YAML (JSON-encoded)',
    is_active            BOOLEAN     COMMENT 'False = exclude from pipeline runs',
    environment          STRING      COMMENT 'dev / qa / prod',
    created_at           TIMESTAMP
)
USING DELTA
COMMENT 'Silver metadata registry — mirrors Flow 1 schema_registry.
         One row per (study, vendor, entity). Consumed by silver_transformer + silver_pipeline.
         source_dataset_name matches ingestion dataset_name for cross-layer joins.
         Ingestion-related columns (criticality, lifecycle_status, etc.) are in ingestion metadata tables.'
"""


# ── Config loader and merge helpers ───────────────────────────────────────────

def _load_yaml_with_substitutions(path: str) -> dict:
    """Load YAML and resolve ${VARIABLE} placeholders from Spark conf."""
    variables = {
        "METADATA_CATALOG": METADATA_CATALOG,
        "METADATA_SCHEMA":  METADATA_SCHEMA,
        "REGISTRY_SCHEMA":  REGISTRY_SCHEMA,
        "CATALOG":          CATALOG,
        "SILVER_SCHEMA":    SILVER_SCHEMA,
        "source_bucket":    source_bucket,
        "SOURCE_BUCKET":    source_bucket,
    }
    with open(path, "r") as f:
        text = f.read()
    for k, v in variables.items():
        if v:
            text = re.sub(rf"\$\{{\s*{k}\s*\}}", v, text)
    unresolved = re.findall(r"\$\{.*?\}", text)
    if unresolved:
        raise ValueError(f"Unresolved config placeholders: {unresolved}")
    return yaml.safe_load(text) or {}


def _clean(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _to_list(val: Any) -> list:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(v).strip() for v in val if str(v).strip()]
    text = str(val).strip()
    if not text:
        return []
    return [v.strip() for v in text.split(",") if v.strip()]


def _merge_transformation_logic(vendor_logic: Any, dataset_logic: Any) -> Any:
    if isinstance(vendor_logic, dict) and "standard" in vendor_logic:
        merged_vendor = dict(vendor_logic)
        dataset_text = _clean(dataset_logic)
        if dataset_text:
            standard_logic = _clean(merged_vendor.get("standard"))
            merged_vendor["standard"] = f"{standard_logic},\n{dataset_text}" if standard_logic else dataset_text
        return merged_vendor

    vendor_text = _clean(vendor_logic)
    dataset_text = _clean(dataset_logic)

    if not vendor_text:
        return dataset_logic
    if not dataset_text:
        return vendor_logic

    try:
        vendor_obj = json.loads(vendor_text)
    except Exception:
        vendor_obj = None

    if isinstance(vendor_obj, dict) and "standard" in vendor_obj:
        standard_logic = str(vendor_obj.get("standard", "")).strip()
        if standard_logic:
            vendor_obj["standard"] = f"{standard_logic},\n{dataset_text}"
        else:
            vendor_obj["standard"] = dataset_text
        return vendor_obj

    return f"{vendor_text},\n{dataset_text}"


def _derive_silver_table_name(dataset: dict) -> str | None:
    domain = _clean(dataset.get("domain")).lower()
    entity = _clean(dataset.get("entity")).lower()
    if not domain or not entity:
        return None
    return f"{CATALOG}.{SILVER_SCHEMA}.{domain}_{entity}"


def _normalize_vendor(vendor: Any) -> str:
    return _clean(vendor).lower()


def _normalize_entity(entity: Any) -> str:
    return _clean(entity).lower()


def _format_study_id(study_token: str) -> str | None:
    if not study_token:
        return None
    token = study_token.strip().lower()
    if token in {"global", "_global"}:
        return "_GLOBAL"
    if re.match(r"^[a-z]{2}\d{3}_\d{3}$", token):
        return f"{token[:5].upper()}-{token[6:]}"
    if re.match(r"^[a-z]{2}\d{3}-\d{3}$", token):
        return token.upper()
    return study_token.upper()


def _parse_dataset_identity(dataset_name: str) -> dict:
    name = _clean(dataset_name)
    parts = [p for p in name.split("_") if p]
    if len(parts) < 3:
        return {
            "vendor": None,
            "study_id": None,
            "entity": None,
        }

    # Global datasets use: <vendor>_global_<entity...>
    if len(parts) >= 3 and parts[1].lower() == "global":
        entity = "_".join(parts[2:]) if len(parts) > 2 else None
        return {
            "vendor": parts[0],
            "study_id": "_GLOBAL",
            "entity": entity,
        }

    vendor = parts[0]
    study_parts = parts[1:3]
    study_token = "_".join(study_parts)
    entity = "_".join(parts[3:]) if len(parts) > 3 else None
    return {
        "vendor": vendor,
        "study_id": _format_study_id(study_token),
        "entity": entity,
    }


def _get_ingestion_datasets(domain_name: str | None, data_product_name: str | None) -> list[dict]:
    where_parts = ["1 = 1"]
    if _clean(domain_name):
        safe_domain = _clean(domain_name).replace("'", "''")
        where_parts.append(f"lower(domain_name) = lower('{safe_domain}')")
    if _clean(data_product_name):
        safe_product = _clean(data_product_name).replace("'", "''")
        where_parts.append(f"lower(data_product_name) = lower('{safe_product}')")

    where_sql = " AND ".join(where_parts)
    rows = spark.sql(f"""
        SELECT
            r.dataset_name,
            r.is_active,
                        max(CASE WHEN lower(t.tag_key) = 'domain' THEN t.tag_value END) AS domain_tag,
                        max(CASE WHEN lower(t.tag_key) = 'domain' AND lower(trim(t.tag_value)) = 'common' THEN 1 ELSE 0 END) AS has_common_domain_tag
        FROM {DATASET_REGISTRY_TBL} r
        LEFT JOIN {DATASET_TAGS_TBL} t
          ON r.dataset_id = t.dataset_id
        WHERE {where_sql}
        GROUP BY r.dataset_name, r.is_active
    """).collect()

    datasets = []
    skipped_common_domain = 0
    for r in rows:
        dataset_name = r["dataset_name"]
        domain_tag = _clean(r["domain_tag"]).lower() or None
        has_common_domain_tag = int(r["has_common_domain_tag"] or 0) == 1
        if has_common_domain_tag:
            skipped_common_domain += 1
            logger.info("Skipping dataset with domain tag 'common': %s", dataset_name)
            continue

        identity = _parse_dataset_identity(dataset_name)
        datasets.append({
            "dataset_name": dataset_name,
            "is_active": r["is_active"],
            "domain": domain_tag,
            "vendor": _clean(identity.get("vendor")) or None,
            "entity": _clean(identity.get("entity")) or None,
            "study_id": _clean(identity.get("study_id")) or None,
        })

    if skipped_common_domain:
        logger.info("Skipped %d ingestion datasets with domain tag 'common'.", skipped_common_domain)

    return datasets


def _build_merged_config_from_split_yaml() -> dict:
    vendor_cfg = _load_yaml_with_substitutions(YML_VENDOR_PATH)
    entity_cfg = _load_yaml_with_substitutions(YML_ENTITY_PATH)

    domain_name = entity_cfg.get("domain_name") or vendor_cfg.get("domain_name") or "DrugDev"
    data_product_name = entity_cfg.get("data_product_name") or vendor_cfg.get("data_product_name") or "DrugDev"

    ingestion_datasets = _get_ingestion_datasets(domain_name, data_product_name)
    logger.info("Ingestion datasets found: %d", len(ingestion_datasets))

    vendor_index: dict[tuple[str, str], dict] = {}
    for v in vendor_cfg.get("vendor_entities", []):
        key = (_normalize_vendor(v.get("vendor")), _normalize_entity(v.get("entity")))
        if key[0] and key[1]:
            vendor_index[key] = v

    entity_rows = entity_cfg.get("datasets", [])
    by_source = {_clean(d.get("source_dataset_name")): d for d in entity_rows if _clean(d.get("source_dataset_name"))}
    by_dataset = {_clean(d.get("dataset_name")): d for d in entity_rows if _clean(d.get("dataset_name"))}

    merged_datasets: list[dict] = []
    for ds in ingestion_datasets:
        dataset_name = _clean(ds.get("dataset_name"))
        entity_row = by_source.get(dataset_name) or by_dataset.get(dataset_name)
        vendor = _normalize_vendor(ds.get("vendor"))
        entity = _normalize_entity(ds.get("entity"))
        domain = _clean(ds.get("domain")).lower() or _clean((entity_row or {}).get("domain")).lower() or None
        study_id = _clean(ds.get("study_id")) or None

        if not vendor or not entity:
            logger.warning("Skipping dataset with unparsable identity: %s", dataset_name)
            continue

        vendor_defaults = vendor_index.get((vendor, entity), {})

        overrides = (entity_row or {}).get("overrides") or {}

        scd_business_keys = _to_list(overrides.get("scd_business_keys")) or _to_list(vendor_defaults.get("scd_business_keys"))
        canonical_cols = _to_list(overrides.get("canonical_cols")) or _to_list(vendor_defaults.get("canonical_cols"))
        zorder_cols = _to_list(overrides.get("zorder_cols")) or _to_list(vendor_defaults.get("zorder_cols"))

        override_table_sensitivity = _clean(overrides.get("table_sensitivity"))
        table_sensitivity = override_table_sensitivity or _clean(vendor_defaults.get("table_sensitivity")) or "INTERNAL"

        override_pii_cols = overrides.get("pii_columns")
        pii_columns = override_pii_cols if isinstance(override_pii_cols, list) and override_pii_cols else (vendor_defaults.get("pii_columns") or [])

        transformation_logic = _merge_transformation_logic(
            vendor_defaults.get("transformation_logic"),
            overrides.get("transformation_logic"),
        )

        cross_entity_sql = overrides.get("cross_entity_sql") or vendor_defaults.get("cross_entity_sql")
        validation_rules = (entity_row or {}).get("validation_rules") or vendor_defaults.get("validation_rules")

        mapping_rules = (entity_row or {}).get("mapping_rules") or {}
        full_mapping_rules = {}
        for col in canonical_cols:
            full_mapping_rules[col] = mapping_rules.get(col, col)
        for col, src in mapping_rules.items():
            if col not in full_mapping_rules:
                full_mapping_rules[col] = src

        merged = {
            "dataset_name": _clean((entity_row or {}).get("dataset_name")) or dataset_name,
            "source_dataset_name": _clean((entity_row or {}).get("source_dataset_name")) or dataset_name,
            "silver_table_name": _derive_silver_table_name({"domain": domain, "entity": entity}),
            "vendor": vendor.upper(),
            "domain": domain,
            "entity": entity,
            "study_id": study_id,
            "is_active": bool(ds.get("is_active", True)),
            "scd_type": "Overwrite",
            "table_sensitivity": table_sensitivity,
            "pii_columns": pii_columns,
            "scd_business_keys": scd_business_keys,
            "canonical_cols": canonical_cols,
            "zorder_cols": zorder_cols,
            "mapping_rules": full_mapping_rules,
            "transformation_logic": transformation_logic,
            "cross_entity_sql": cross_entity_sql,
            "validation_rules": validation_rules,
        }
        merged_datasets.append(merged)

    extra_entity_rows = [
        _clean(d.get("dataset_name"))
        for d in entity_rows
        if _clean(d.get("source_dataset_name")) not in {x["dataset_name"] for x in ingestion_datasets}
        and _clean(d.get("dataset_name")) not in {x["dataset_name"] for x in ingestion_datasets}
    ]
    if extra_entity_rows:
        logger.warning(
            "Entity YAML contains %d rows not present in ingestion registry. Example: %s",
            len(extra_entity_rows),
            ", ".join(extra_entity_rows[:5]),
        )

    return {
        "domain_name": domain_name,
        "data_product_name": data_product_name,
        "datasets": merged_datasets,
    }


def load_config() -> dict:
    if YML_VENDOR_PATH and YML_ENTITY_PATH:
        return _build_merged_config_from_split_yaml()
    logger.info("Using legacy merged Silver config: %s", YML_CONFIG_PATH)
    return _load_yaml_with_substitutions(YML_CONFIG_PATH)


# ── dataset_id lookup (optional FK to ingestion framework) ────────────────────

def _get_dataset_id_map(config: dict) -> dict:
    source_names = list({_clean(ds.get("source_dataset_name")) for ds in config.get("datasets", []) if _clean(ds.get("source_dataset_name"))})
    if not source_names:
        return {}
    names_sql = ", ".join(f"'{n}'" for n in source_names)
    df = spark.sql(f"""
        SELECT dataset_id, dataset_name
        FROM {DATASET_REGISTRY_TBL}
        WHERE dataset_name IN ({names_sql})
    """)
    return {row["dataset_name"]: row["dataset_id"] for row in df.collect()}


# ── Create / ensure table ──────────────────────────────────────────────────────

def ensure_registry_table():
    """Create drugdev_silver_registry if it does not already exist."""
    spark.sql(CREATE_REGISTRY_DDL)
    logger.info("drugdev_silver_registry ready: %s", SILVER_REGISTRY_TBL)


# ── Main upsert ────────────────────────────────────────────────────────────────

def load_silver_registry(config: dict, dataset_id_map: dict):
    """
    Upsert drugdev_silver_registry — one row per Silver dataset
    (each row = one study/vendor/entity combination).

    Merge key: (source_dataset_name, environment)
    All fields are refreshed on MATCHED.
    """
    existing_map = {
        (row["source_dataset_name"], row["environment"]): row["registry_id"]
        for row in spark.sql(f"""
            SELECT registry_id, source_dataset_name, environment
            FROM {SILVER_REGISTRY_TBL}
        """).collect()
    }

    rows = []
    for dataset in config.get("datasets", []):
        src_name = _clean(dataset.get("source_dataset_name"))
        if not src_name:
            continue
        # environment = dataset.get("environment")
        key = (src_name, environment)

        registry_id = existing_map.get(key) or str(uuid.uuid4())

        # mapping_rules from YAML is {canonical_col: vendor_col} (Flow 1 direction).
        # Build full mapping: for each canonical_col, use YAML rename or fall back to self-mapping.
        yaml_mapping = dataset.get("mapping_rules") or {}  # {canonical_col: vendor_col}
        mapping_rules = {}
        # logger.info("Processing dataset: %s  canonical_cols=%s  mapping_rules=%s", src_name, dataset.get("canonical_cols"), yaml_mapping)
        for canonical_col in dataset.get("canonical_cols") or []:
            if canonical_col in yaml_mapping:
                mapping_rules[canonical_col] = yaml_mapping[canonical_col]
            else:
                mapping_rules[canonical_col] = canonical_col
        for col in yaml_mapping or []:
            if col not in mapping_rules:
                mapping_rules[col] = col
        # logger.info("validation_rules=%s", dataset.get("validation_rules"))
        rows.append(Row(
            registry_id         = registry_id,
            domain              = dataset.get("domain"),
            entity              = dataset.get("entity"),
            vendor              = dataset.get("vendor"),
            study_id            = dataset.get("study_id"),
            source_dataset_name = src_name,
            silver_table_name   = dataset.get("silver_table_name"),
            table_sensitivity   = dataset.get("table_sensitivity"),
            pii_columns         = json.dumps(dataset.get("pii_columns") or []),
            scd_type            = dataset.get("scd_type", "scd2"),
            scd_business_keys   = json.dumps(dataset.get("scd_business_keys") or []),
            canonical_cols      = json.dumps(dataset.get("canonical_cols") or []),
            zorder_cols         = json.dumps(dataset.get("zorder_cols") or []),
            mapping_rules       = json.dumps(mapping_rules),
            transformation_logic= dataset.get("transformation_logic"),
            cross_entity_sql    = dataset.get("cross_entity_sql"),
            validation_rules     = json.dumps(dataset.get("validation_rules", {})),
            is_active           = bool(dataset.get("is_active", True)),
            environment         = environment,
            created_at          = datetime.now(),
        ))

    if not rows:
        logger.info("No datasets to process.")
        return

    schema = StructType([
        StructField("registry_id",          StringType(),    True),
        StructField("domain",               StringType(),    True),
        StructField("entity",               StringType(),    True),
        StructField("vendor",               StringType(),    True),
        StructField("study_id",             StringType(),    True),
        StructField("source_dataset_name",  StringType(),    True),
        StructField("silver_table_name",    StringType(),    True),
        StructField("table_sensitivity",    StringType(),    True),
        StructField("pii_columns",          StringType(),    True),
        StructField("scd_type",             StringType(),    True),
        StructField("scd_business_keys",    StringType(),    True),
        StructField("canonical_cols",       StringType(),    True),
        StructField("zorder_cols",          StringType(),    True),
        StructField("mapping_rules",        StringType(),    True),
        StructField("transformation_logic", StringType(),    True),
        StructField("cross_entity_sql",     StringType(),    True),
        StructField("validation_rules",     StringType(),    True),
        StructField("is_active",            BooleanType(),   True),
        StructField("environment",          StringType(),    True),
        StructField("created_at",           TimestampType(), True),
    ])

    source_df = spark.createDataFrame(rows, schema=schema)
    source_df = source_df.dropDuplicates(["registry_id"])
    source_df.createOrReplaceTempView("_silver_registry_source")

    spark.sql(f"""
        MERGE INTO {SILVER_REGISTRY_TBL} t
        USING _silver_registry_source s
        ON t.source_dataset_name = s.source_dataset_name
           AND t.environment = s.environment

        WHEN MATCHED THEN UPDATE SET
            t.domain               = s.domain,
            t.entity               = s.entity,
            t.vendor               = s.vendor,
            t.study_id             = s.study_id,
            t.silver_table_name    = s.silver_table_name,
            t.table_sensitivity    = s.table_sensitivity,
            t.pii_columns          = s.pii_columns,
            t.scd_type             = s.scd_type,
            t.scd_business_keys    = s.scd_business_keys,
            t.canonical_cols       = s.canonical_cols,
            t.zorder_cols          = s.zorder_cols,
            t.mapping_rules        = s.mapping_rules,
            t.transformation_logic = s.transformation_logic,
            t.cross_entity_sql     = s.cross_entity_sql,
            t.validation_rules     = s.validation_rules,
            t.is_active            = s.is_active

        WHEN NOT MATCHED THEN INSERT (
            registry_id, domain, entity, vendor, study_id,
            source_dataset_name, silver_table_name, table_sensitivity, pii_columns, scd_type,
            scd_business_keys, canonical_cols, zorder_cols,
            mapping_rules, transformation_logic, cross_entity_sql, validation_rules,
            is_active, environment, created_at
        )
        VALUES (
            s.registry_id, s.domain, s.entity, s.vendor, s.study_id,
            s.source_dataset_name, s.silver_table_name, s.table_sensitivity, s.pii_columns, s.scd_type,
            s.scd_business_keys, s.canonical_cols, s.zorder_cols,
            s.mapping_rules, s.transformation_logic, s.cross_entity_sql, s.validation_rules,
            s.is_active, s.environment, s.created_at
        )
    """)

    logger.info("drugdev_silver_registry upsert complete. Rows: %d", len(rows))

    # Summary — entities loaded
    domain_entity_counts: dict = {}
    for row in rows:
        k = f"{row.domain}/{row.entity}"
        domain_entity_counts[k] = domain_entity_counts.get(k, 0) + 1
    logger.info("Entities loaded: %d  (study rows: %d)", len(domain_entity_counts), len(rows))
    for k, cnt in sorted(domain_entity_counts.items()):
        logger.info("  %s: %d study/vendor rows", k, cnt)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    config = load_config()
    logger.info("Config loaded. Datasets: %d", len(config.get("datasets", [])))

    dataset_id_map = _get_dataset_id_map(config)
    logger.info("Resolved %d dataset_ids from dataset_registry.", len(dataset_id_map))

    ensure_registry_table()
    load_silver_registry(config, dataset_id_map)

    logger.info("Silver metadata loaded successfully into drugdev_silver_registry.")


if __name__ == "__main__":
    main()
