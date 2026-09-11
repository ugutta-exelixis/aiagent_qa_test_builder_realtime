"""
Silver YAML Generator
Generates drugdev_silver_config.yaml from TWO Excel files:

  silver_vendor_catalog.xlsx   — vendor/entity defaults (one row per vendor-entity)
    Sheets: Vendors, Entities, CanonicalCols, ValidationRules

  silver_entity_level.xlsx     — per-dataset study config (one row per Silver dataset)
    Sheets: Datasets, ColumnMappings, ValidationRules

Merge logic (entity-level always wins over vendor defaults):
  • canonical_cols        → entity override > vendor CanonicalCols sheet
  • scd_business_keys     → entity override > vendor Entities sheet
  • zorder_cols           → entity override > vendor Entities sheet
  • transformation_logic  → entity override > vendor Entities sheet
  • cross_entity_sql      → entity override > vendor Entities sheet
  • validation_rules      → entity ValidationRules > vendor ValidationRules
  • mapping_rules         → from entity ColumnMappings sheet (bronze_col → canonical_col)
  • column_types          → from entity ColumnMappings sheet (canonical_col → target_type)

Usage
-----
  # Create vendor catalog template:
  python silver_yaml_generator.py --create-vendor-template docs/silver_vendor_catalog.xlsx

  # Create entity-level template:
  python silver_yaml_generator.py --create-entity-template docs/silver_entity_level.xlsx

  # Generate YAML from both filled-in Excels:
  python silver_yaml_generator.py \\
      --vendor-catalog docs/silver_vendor_catalog.xlsx \\
      --entity-level   docs/silver_entity_level.xlsx \\
      --output         databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_config.yaml
"""

import argparse
import json
import sys
import ast
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip()


def _split_csv(val) -> list:
    s = _clean(val)
    return [v.strip() for v in s.split(",") if v.strip()] if s else []


def _parse_rule_field(field_val, check_val) -> Any:
    """
    Parse ValidationRules.field with special handling for compound_unique.

    For compound_unique, return a list of column names. For all other checks,
    keep the historical scalar string behavior.
    """
    check = _clean(check_val).lower()
    raw = _clean(field_val)

    if check != "compound_unique":
        return raw

    if not raw:
        return []

    text = raw
    if text.startswith("'") and text.endswith("'"):
        text = text[1:-1]
    text = text.replace("''", "'")

    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(text)
            if isinstance(parsed, list):
                return [str(c).strip() for c in parsed if str(c).strip()]
            if isinstance(parsed, str) and parsed.strip():
                return [parsed.strip()]
        except Exception:
            continue

    if "," in text:
        return [c.strip().strip("'\"") for c in text.split(",") if c.strip().strip("'\"")]

    return [text.strip("'\"")]


def _parse_pii_columns(val) -> list:
    """
    Parse pii_columns from Excel.

    Supported formats:
      1) Legacy CSV
         subject, treatment_arm

      2) JSON list of strings
         ["subject", "treatment_arm"]

      3) JSON list of objects (preferred for column-level tag metadata)
         [{"column":"subject","pii_category":"SUBJECT_IDENTIFIER","sensitivity":"RESTRICTED"}]
    """
    s = _clean(val)
    if not s:
        return []

    if s.startswith("[") or s.startswith("{"):
        try:
            parsed = json.loads(s)
            if isinstance(parsed, dict):
                parsed = [parsed]
            if isinstance(parsed, list):
                normalized: list = []
                for item in parsed:
                    if isinstance(item, dict):
                        col_name = _clean(item.get("column", ""))
                        if not col_name:
                            continue
                        entry: dict[str, str] = {"column": col_name}
                        pii_category = _clean(item.get("pii_category", ""))
                        sensitivity = _clean(item.get("sensitivity", ""))
                        if pii_category:
                            entry["pii_category"] = pii_category
                        if sensitivity:
                            entry["sensitivity"] = sensitivity.upper()
                        normalized.append(entry)
                    else:
                        text_item = _clean(item)
                        if text_item:
                            normalized.append(text_item)
                return normalized
        except Exception:
            # Fall back to legacy CSV parsing.
            pass

    return _split_csv(s)


def _to_bool(val, default=False) -> bool:
    s = _clean(val).lower()
    if s in ("true", "yes", "1", "y"):
        return True
    if s in ("false", "no", "0", "n"):
        return False
    return default


class _LiteralStr(str):
    pass


def _literal_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(_LiteralStr, _literal_representer)


# ---------------------------------------------------------------------------
# Template: silver_vendor_catalog.xlsx
# ---------------------------------------------------------------------------

def create_vendor_template(output_path: str) -> None:
    """
    Create a blank silver_vendor_catalog.xlsx with 4 sheets.
    Mirrors the structure of Flow 1's vendor catalog YAMLs (pra.yaml, iqvia.yaml, etc.)

    Sheets:
      Vendors        — one row per vendor (delivery metadata)
      Entities       — one row per vendor-entity (schema + SQL baseline)
      CanonicalCols  — one row per canonical column per vendor-entity (in schema order)
      ValidationRules— one row per DQ rule per vendor-entity
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out, engine="openpyxl") as writer:

        # ── Vendors ──────────────────────────────────────────────────────────
        vendors_rows = [
            ("PRA",     "ctms", "DATE_IN_FOLDER",            "DIGEST",    "daily",              r"(\d{8})",            "yyyyMMdd",    "PRA_EXL_{entity_key}_{study}_{date}_{time}.{ext}"),
            ("IQVIA",   "ctms", "STUDY_AND_DATE_IN_FILENAME","DIGEST",    "daily except Sunday", r"(\d{4}-\d{2}-\d{2})","yyyy-MM-dd",  "Q_EXE_{entity_title}_{date}_{time}.{ext}"),
            ("PAREXEL", "ctms", "DATE_IN_FOLDER",            "DIGEST",    "daily",              r"(\d{8})",            "yyyyMMdd",    "PAREXEL_{entity_key}_{date}.{ext}"),
            ("4G",      "irt",  "DATE_IN_FOLDER",            "DIGEST",    "daily",              r"(\d{8})",            "yyyyMMdd",    "{entity_key}_{date}.{ext}"),
            ("CENDUIT", "irt",  "DATE_IN_FOLDER",            "DIGEST",    "daily",              r"(\d{8})",            "yyyyMMdd",    "CENDUIT_{entity_key}_{date}.{ext}"),
            ("MEDIDATA","edc",  "STUDY_AND_DATE_IN_FILENAME","DIGEST",    "daily",              r"(\d{4}-\d{2}-\d{2})","yyyy-MM-dd",  "{entity_title}_{date}.{ext}"),
        ]
        pd.DataFrame(
            vendors_rows,
            columns=[
                "vendor_name", "domain", "path_type", "delivery_method",
                "delivery_schedule", "date_regex", "date_format",
                "file_name_pattern_template",
            ],
        ).to_excel(writer, sheet_name="Vendors", index=False)

        # ── Entities ─────────────────────────────────────────────────────────
        entities_rows = [
            (
                "PRA", "study",
                "study_id",        # scd_business_keys
                "study_id",        # zorder_cols
                "INTERNAL", "",    # table_sensitivity, pii_columns
                # transformation_logic
                "cast(study_protocol_number as string) as study_id,\n"
                "cast(study_title as string) as study_title,\n"
                "UPPER(status) as status,\n"
                "md5(study_protocol_number) as hash_key",
                "",               # cross_entity_sql
            ),
            (
                "PRA", "study_site",
                "study_id, study_country_id, account_id, site_number, parent_site",
                "study_id, study_site_id, site_number",
                "INTERNAL", "",
                "cast(study_protocol_number as string) as study_id,\n"
                "cast(study_site_id as string) as study_site_id,\n"
                "cast(study_country_id as string) as study_country_id,\n"
                "cast(account_id as string) as account_id,\n"
                "cast(site_number as string) as site_number,\n"
                "md5(study_protocol_number || coalesce(cast(study_country_id as string),'') || coalesce(cast(site_number as string),'') || coalesce(account_id,'')) as hash_key",
                "SELECT /*+ BROADCAST(crm) */ DISTINCT s.* EXCEPT (icn_region),\n"
                "  CASE WHEN crm.region = 'NA' THEN 'North America'\n"
                "       WHEN crm.region = 'APAC' THEN 'Asia/Pacific'\n"
                "       WHEN crm.region = 'LATAM' THEN 'Latin America'\n"
                "       WHEN crm.region = 'EMEA' THEN 'Europe/Africa'\n"
                "       ELSE crm.region END AS icn_region\n"
                "FROM {staging_table} s\n"
                "LEFT JOIN {bronze:study_country} sc ON s.study_country_id = sc.study_country_id\n"
                "LEFT JOIN {common:country_region_mapping} crm\n"
                "    ON UPPER(sc.country_name) = crm.country",
            ),
        ]
        pd.DataFrame(
            entities_rows,
            columns=[
                "vendor_name", "entity_name",
                "scd_business_keys", "zorder_cols",
                "table_sensitivity", "pii_columns", "transformation_logic", "cross_entity_sql",
            ],
        ).to_excel(writer, sheet_name="Entities", index=False)

        # ── CanonicalCols ─────────────────────────────────────────────────────
        canon_rows = [
            ("PRA", "study",      "study_id",    1, "string"),
            ("PRA", "study",      "study_title", 2, "string"),
            ("PRA", "study",      "status",      3, "string"),
            ("PRA", "study",      "hash_key",    4, "string"),
            ("PRA", "study_site", "study_id",        1, "string"),
            ("PRA", "study_site", "study_site_id",   2, "string"),
            ("PRA", "study_site", "study_country_id",3, "string"),
            ("PRA", "study_site", "account_id",      4, "string"),
            ("PRA", "study_site", "site_number",     5, "string"),
            ("PRA", "study_site", "icn_region",      6, "string"),
            ("PRA", "study_site", "site_type",       7, "string"),
            ("PRA", "study_site", "study_site_status",8,"string"),
            ("PRA", "study_site", "parent_site",     9, "string"),
            ("PRA", "study_site", "hash_key",       10, "string"),
        ]
        pd.DataFrame(
            canon_rows,
            columns=["vendor_name", "entity_name", "canonical_col", "col_order", "target_type"],
        ).to_excel(writer, sheet_name="CanonicalCols", index=False)

        # ── ValidationRules ───────────────────────────────────────────────────
        val_rows = [
            ("PRA", "study",      "CTMS-PRA-STU-NNL-001", "study_id",      "not_null",    "",                                "CRITICAL", "quarantine", "study_id must never be null"),
            ("PRA", "study",      "CTMS-PRA-STU-RGX-001", "study_id",      "regex_match", "^[A-Z]{2}[0-9]{3}-[0-9]{3}$",   "HIGH",     "quarantine", "study_id must match XX999-999"),
            ("PRA", "study_site", "CTMS-PRA-SS-NNL-001",  "study_id",      "not_null",    "",                                "CRITICAL", "quarantine", "study_id must never be null"),
            ("PRA", "study_site", "CTMS-PRA-SS-NNL-002",  "study_site_id", "not_null",    "",                                "CRITICAL", "quarantine", "study_site_id must never be null"),
        ]
        pd.DataFrame(
            val_rows,
            columns=[
                "vendor_name", "entity_name", "rule_id", "field", "check",
                "expression", "severity", "mode", "description",
            ],
        ).to_excel(writer, sheet_name="ValidationRules", index=False)

    print(f"✓ Silver vendor catalog template created: {out}")
    print("  Sheets: Vendors, Entities, CanonicalCols, ValidationRules")
    print("  Fill in ALL vendors (PRA, IQVIA, PAREXEL, 4G, CENDUIT, MEDIDATA, etc.)")
    print("  Then run: python silver_yaml_generator.py --vendor-catalog <this_file> "
          "--entity-level silver_entity_level.xlsx --output drugdev_silver_config.yaml")


# ---------------------------------------------------------------------------
# Template: silver_entity_level.xlsx
# ---------------------------------------------------------------------------

def create_entity_template(output_path: str) -> None:
    """
    Create a blank silver_entity_level.xlsx with 3 sheets.
    Mirrors the ingestion Excel (one row per dataset) + override columns.

    Sheets:
      Datasets       — one row per Silver dataset (dataset_name is unique key)
      ColumnMappings — one row per bronze_col → canonical_col rename per dataset
      ValidationRules— one row per DQ rule per dataset (overrides vendor rules if any rows present)
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out, engine="openpyxl") as writer:

        # ── Datasets ──────────────────────────────────────────────────────────
        # Two example rows: one IRT (no overrides), one CTMS (with SQL overrides)
        datasets_rows = [
            {
                "dataset_name":         "xb002_101_4g_irt_subject_summary",
                "vendor_name":          "4G",
                "entity_name":          "subject_summary_report",
                "study_id":             "XB002-101",
                "domain":               "irt",
                "source_dataset_name":  "xb002_101_4g_clinical_subject_summary_report",
                "silver_table_name":    "${CATALOG}.silver_drugdev.xb002_101_4g_irt_subject_summary",
                "dataset_version":      "v1",
                "criticality":          "high",
                "data_classification":  "internal",
                "contains_pii":         "false",
                "table_sensitivity":    "INTERNAL",
                "lifecycle_status":     "active",
                "is_active":            "true",
                "frequency":            "daily_with_datefolder",
                "scd_type":             "scd2",
                # Override columns — leave blank to inherit from vendor catalog
                "scd_business_keys":    "",
                "canonical_cols":       "",
                "zorder_cols":          "",
                "partition_cols":       "",
                "watermark_column":     "",
                "transformation_logic": "",
                "cross_entity_sql":     "",
                # DQ SLA
                "dq_enabled":           "true",
                "fail_action":          "fail_pipeline",
                "alert_channel":        "email",
                "escalation_contact":   "spullikandla@exelixis.com",
            },
            {
                "dataset_name":         "xl092_304_pra_ctms_study_site",
                "vendor_name":          "PRA",
                "entity_name":          "study_site",
                "study_id":             "XL092-304",
                "domain":               "ctms",
                "source_dataset_name":  "xl092_304_pra_ctms_study_site",
                "silver_table_name":    "${CATALOG}.silver_drugdev.xl092_304_pra_ctms_study_site",
                "dataset_version":      "v1",
                "criticality":          "high",
                "data_classification":  "internal",
                "contains_pii":         "false",
                "table_sensitivity":    "INTERNAL",
                "lifecycle_status":     "active",
                "is_active":            "true",
                "frequency":            "daily",
                "scd_type":             "scd2",
                # Overrides — filled means "override vendor default"
                "scd_business_keys":    "",
                "canonical_cols":       "",
                "zorder_cols":          "",
                "partition_cols":       "",
                "watermark_column":     "",
                # Study-specific SQL overrides (blank = use vendor baseline from silver_vendor_catalog.xlsx)
                "transformation_logic": "",
                "cross_entity_sql":
                    "with site_actual as (\n"
                    "  select distinct s.* EXCEPT (s.study_country_id, s.account_id, s.icn_region, s.hash_key),\n"
                    "  cast(crm.study_country_id AS string) as study_country_id,\n"
                    "  cast(sa.account_source_id AS string) as account_id,\n"
                    "  case when crm.region = 'NA' then 'North America'\n"
                    "       when crm.region = 'APAC' then 'Asia/Pacific'\n"
                    "       else crm.region END AS icn_region,\n"
                    "  md5(concat(coalesce(s.study_id,''),coalesce(s.site_number,''))) as hash_key\n"
                    "  FROM {staging_table} s\n"
                    "  LEFT JOIN {bronze:account_association} sa ON s.study_site_id = sa.study_site_source_id\n"
                    "  LEFT JOIN {bronze:study_country} sc ON s.study_country_id = sc.study_country_source_id\n"
                    "  LEFT JOIN {common:country_region_mapping} crm ON UPPER(sc.study_region) = crm.country)\n"
                    "select * from site_actual",
                "dq_enabled":           "true",
                "fail_action":          "fail_pipeline",
                "alert_channel":        "email",
                "escalation_contact":   "spullikandla@exelixis.com",
            },
        ]
        pd.DataFrame(datasets_rows).to_excel(writer, sheet_name="Datasets", index=False)

        # ── ColumnMappings ────────────────────────────────────────────────────
        mapping_rows = [
            ("xb002_101_4g_irt_subject_summary", "Subject",   "subject_id", "string"),
            ("xb002_101_4g_irt_subject_summary", "SiteId",    "site_id",    "string"),
            ("xb002_101_4g_irt_subject_summary", "StudyId",   "study_id",   "string"),
            ("xl092_304_pra_ctms_study_site",    "pi_contact_id", "primary_investigator_source_id", "string"),
            ("xl092_304_pra_ctms_study_site",    "study_site_status", "site_status", "string"),
        ]
        pd.DataFrame(
            mapping_rows,
            columns=["dataset_name", "bronze_col", "canonical_col", "target_type"],
        ).to_excel(writer, sheet_name="ColumnMappings", index=False)

        # ── ValidationRules ───────────────────────────────────────────────────
        # Rows here REPLACE vendor-level rules for that dataset.
        # If no rows for a dataset, vendor rules from silver_vendor_catalog.xlsx are used.
        val_rows = [
            ("xb002_101_4g_irt_subject_summary", "DD-IRT-SUBJ-NNL-001", "subject_id", "not_null",        "",     "CRITICAL", "quarantine", "subject_id must never be null"),
            ("xb002_101_4g_irt_subject_summary", "DD-IRT-SUBJ-NNL-002", "study_id",   "not_null",        "",     "CRITICAL", "quarantine", "study_id must never be null"),
            ("xb002_101_4g_irt_subject_summary", "DD-IRT-SUBJ-UNQ-001", '["subject_id","study_id"]', "compound_unique", "", "HIGH", "quarantine", "subject_id unique per study_id"),
        ]
        pd.DataFrame(
            val_rows,
            columns=["dataset_name", "rule_id", "field", "check", "expression",
                     "severity", "mode", "description"],
        ).to_excel(writer, sheet_name="ValidationRules", index=False)

    print(f"✓ Silver entity-level template created: {out}")
    print("  Sheets: Datasets, ColumnMappings, ValidationRules")
    print("  - One row per Silver dataset in Datasets sheet")
    print("  - Leave scd_business_keys / canonical_cols / transformation_logic / cross_entity_sql blank")
    print("    to inherit defaults from silver_vendor_catalog.xlsx")
    print("  - ColumnMappings: only renames needed (bronze_col → canonical_col);")
    print("    passthrough columns (same name in Bronze & Silver) are auto-handled")
    print("  - ValidationRules: if rows exist for a dataset they REPLACE vendor rules;")
    print("    leave empty for a dataset to inherit vendor-level rules")


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _load_vendor_catalog(path: str) -> tuple[dict, dict, dict]:
    """
    Returns:
      entities   : {(vendor_name, entity_name): entity_dict}
      canon_cols : {(vendor_name, entity_name): [canonical_col, ...]}   (ordered)
      val_rules  : {(vendor_name, entity_name): [rule_dict, ...]}
    """
    xl = pd.ExcelFile(path)
    entities: dict = {}
    canon_cols: dict = {}
    val_rules: dict = {}

    # Entities sheet
    ent_df = pd.read_excel(xl, sheet_name="Entities")
    for _, row in ent_df.iterrows():
        vn = _clean(row.get("vendor_name", ""))
        en = _clean(row.get("entity_name", ""))
        if not vn or not en:
            continue
        key = (vn, en)
        entities[key] = {
            "scd_business_keys":     _split_csv(row.get("scd_business_keys", "")),
            "zorder_cols":           _split_csv(row.get("zorder_cols", "")),
            "table_sensitivity":     _clean(row.get("table_sensitivity", "INTERNAL")),
            "pii_columns":           _parse_pii_columns(row.get("pii_columns", "")),
            "transformation_logic":  _clean(row.get("transformation_logic", "")) or None,
            "cross_entity_sql":      _clean(row.get("cross_entity_sql", "")) or None,
        }

    # CanonicalCols sheet
    cc_df = pd.read_excel(xl, sheet_name="CanonicalCols")
    col_buf: dict[tuple, list] = {}
    for _, row in cc_df.iterrows():
        vn = _clean(row.get("vendor_name", ""))
        en = _clean(row.get("entity_name", ""))
        col = _clean(row.get("canonical_col", ""))
        if not vn or not en or not col:
            continue
        key = (vn, en)
        order = int(row.get("col_order", 999) or 999)
        target_type = _clean(row.get("target_type", "string")) or "string"
        col_buf.setdefault(key, []).append((order, col, target_type))
    for key, cols in col_buf.items():
        cols.sort(key=lambda x: x[0])
        canon_cols[key] = [(c, t) for _, c, t in cols]

    # ValidationRules sheet
    vr_df = pd.read_excel(xl, sheet_name="ValidationRules")
    for _, row in vr_df.iterrows():
        vn = _clean(row.get("vendor_name", ""))
        en = _clean(row.get("entity_name", ""))
        rule_id = _clean(row.get("rule_id", ""))
        if not vn or not en or not rule_id:
            continue
        key = (vn, en)
        rule: dict = {
            "rule_id":  rule_id,
            "check":    _clean(row.get("check", "")),
            "severity": _clean(row.get("severity", "HIGH")),
            "mode":     _clean(row.get("mode", "quarantine")),
        }
        rule["field"] = _parse_rule_field(row.get("field", ""), rule["check"])
        expr = _clean(row.get("expression", ""))
        if expr:
            rule["expression"] = expr
        desc = _clean(row.get("description", ""))
        if desc:
            rule["description"] = desc
        val_rules.setdefault(key, []).append(rule)

    return entities, canon_cols, val_rules


def _load_entity_level(path: str) -> tuple[list, dict, dict, dict]:
    """
    Returns:
      datasets       : [row_dict, ...]  (from Datasets sheet)
      mapping_rules  : {dataset_name: {bronze_col: canonical_col}}
      column_types   : {dataset_name: {canonical_col: target_type}}
      entity_val_rules: {dataset_name: [rule_dict, ...]}
    """
    xl = pd.ExcelFile(path)

    # ColumnMappings
    mapping_rules: dict[str, dict] = {}
    column_types: dict[str, dict] = {}
    cm_df = pd.read_excel(xl, sheet_name="ColumnMappings")
    for _, row in cm_df.iterrows():
        ds = _clean(row.get("dataset_name", ""))
        bc = _clean(row.get("bronze_col", ""))
        cc = _clean(row.get("canonical_col", ""))
        if not ds or not bc or not cc:
            continue
        mapping_rules.setdefault(ds, {})[cc] = bc
        tt = _clean(row.get("target_type", "string")) or "string"
        column_types.setdefault(ds, {})[cc] = tt

    # ValidationRules are intentionally ignored for split entity YAML.
    # Silver validation rules are sourced from vendor catalog defaults.
    entity_val_rules: dict[str, list] = {}

    # Datasets
    datasets = []
    ds_df = pd.read_excel(xl, sheet_name="Datasets")
    for _, row in ds_df.iterrows():
        ds = _clean(row.get("dataset_name", ""))
        if not ds:
            continue
        datasets.append({k: row.get(k) for k in ds_df.columns})

    return datasets, mapping_rules, column_types, entity_val_rules


def _validate_entity_alignment(
    datasets: list,
    mapping_rules: dict,
    entity_val_rules: dict,
    strict: bool = False,
) -> dict:
    """Validate dataset_name alignment across entity-level sheets."""
    dataset_names = {
        _clean(row.get("dataset_name", ""))
        for row in datasets
        if _clean(row.get("dataset_name", ""))
    }
    mapping_names = {k for k in mapping_rules.keys() if _clean(k)}
    validation_names = {k for k in entity_val_rules.keys() if _clean(k)}

    only_in_mappings = sorted(mapping_names - dataset_names)
    only_in_datasets = sorted(dataset_names - mapping_names)
    only_in_validation = sorted(validation_names - dataset_names)

    print("\nEntity sheet alignment summary")
    print(f"  Datasets rows         : {len(dataset_names)}")
    print(f"  ColumnMappings sets   : {len(mapping_names)}")
    print(f"  ValidationRules sets  : {len(validation_names)} (ignored for split entity YAML)")
    print(f"  Mapping only          : {len(only_in_mappings)}")
    print(f"  Datasets only         : {len(only_in_datasets)}")
    print(f"  Validation only       : {len(only_in_validation)}")

    if only_in_mappings:
        print("  Example mapping-only datasets:", ", ".join(only_in_mappings[:5]))
    if only_in_validation:
        print("  Example validation-only datasets:", ", ".join(only_in_validation[:5]))

    if strict and only_in_mappings:
        raise ValueError(
            "Strict validation failed: found dataset_name values in ColumnMappings "
            "that are missing from Datasets sheet."
        )

    return {
        "dataset_names": dataset_names,
        "mapping_names": mapping_names,
        "validation_names": validation_names,
        "only_in_mappings": only_in_mappings,
        "only_in_datasets": only_in_datasets,
        "only_in_validation": only_in_validation,
    }


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

def generate_yaml(
    vendor_catalog_path: str,
    entity_level_path: str,
    output_path: str,
    domain_overrides: dict | None = None,
) -> None:
    """
    Merge vendor catalog + entity-level Excel → drugdev_silver_config.yaml.
    Entity-level values always WIN over vendor defaults.
    """
    print(f"Loading vendor catalog : {vendor_catalog_path}")
    vendor_entities, vendor_canon_cols, vendor_val_rules = _load_vendor_catalog(vendor_catalog_path)

    print(f"Loading entity level   : {entity_level_path}")
    datasets, mapping_rules, column_types, entity_val_rules = _load_entity_level(entity_level_path)

    domain_config = domain_overrides or {
        "domain_name": "DrugDev",
        "data_product_name": "DrugDev",
        "owner_team": "clinical_data_engineering",
    }

    output_datasets = []

    for row in datasets:
        ds_name   = _clean(row.get("dataset_name", ""))
        vn        = _clean(row.get("vendor_name", ""))
        en        = _clean(row.get("entity_name", ""))
        vendor_key = (vn, en)
        vendor_ent = vendor_entities.get(vendor_key, {})

        # ── Merge logic: entity-level override wins ───────────────────────────

        # scd_business_keys
        bk =  vendor_ent.get("scd_business_keys", [])

        # canonical_cols: entity override (csv) > vendor CanonicalCols sheet
        raw_canon = _split_csv(row.get("canonical_cols"))
        if raw_canon:
            canonical_cols = raw_canon
            # column_types from ColumnMappings sheet already built
        else:
            vendor_cc = vendor_canon_cols.get(vendor_key, [])
            canonical_cols = [c for c, _ in vendor_cc]
            # Supplement column_types with vendor defaults (entity mappings win)
            vendor_types = {c: t for c, t in vendor_cc}
            merged_types = {**vendor_types, **column_types.get(ds_name, {})}
            column_types[ds_name] = merged_types

        # zorder_cols
        zorder = vendor_ent.get("zorder_cols", [])

        # table sensitivity
        table_sensitivity = vendor_ent.get("table_sensitivity", "INTERNAL")

        # pii_columns
        pii_columns = vendor_ent.get("pii_columns", [])

        # transformation_logic: entity > vendor        

        import json

        vendor_logic = _clean(vendor_ent.get("transformation_logic"))
        dataset_logic = _clean(row.get("transformation_logic"))

        tl = None

        try:
            vendor_obj = json.loads(vendor_logic) if vendor_logic else None

            # Case 1: vendor transformation_logic is JSON with standard/post_processing
            if isinstance(vendor_obj, dict) and "standard" in vendor_obj:
                standard_logic = vendor_obj.get("standard", "")

                if dataset_logic:
                    standard_logic = f"{standard_logic},\n{dataset_logic}"

                vendor_obj["standard"] = standard_logic
                tl = vendor_obj

            else:
                raise ValueError("Not transformation JSON")

        except Exception:
            # Existing Case 2 logic
            if vendor_logic and dataset_logic:
                tl = f"{vendor_logic},\n{dataset_logic}"
            elif dataset_logic:
                tl = dataset_logic
            else:
                tl = vendor_logic

        # cross_entity_sql: entity > vendor
        cs = _clean(row.get("cross_entity_sql")) or vendor_ent.get("cross_entity_sql") or ""

        # vendor-level validation rules
        validation_rules = vendor_val_rules.get((vn, en), [])

        # ── Build output entry ────────────────────────────────────────────────
        entry: dict[str, Any] = {
            "dataset_name":       ds_name,
            "source_dataset_name":_clean(row.get("source_dataset_name")),
            "silver_table_name":  _clean(row.get("silver_table_name")),
            "vendor":             vn,
            "domain":             _clean(row.get("domain")),
            "entity":             en,
            "study_id":           _clean(row.get("study_id")) or None,
            "is_active":          _to_bool(row.get("is_active"), True),
            "scd_type":           _clean(row.get("scd_type")) or "scd2",
            "table_sensitivity":  table_sensitivity,
            "pii_columns":        pii_columns,
            "scd_business_keys":  bk,
            "canonical_cols":     canonical_cols,
            "zorder_cols":        zorder,
            # mapping_rules stored as {canonical_col: vendor_col} (Flow 1 direction)
            # Excel ColumnMappings sheet has {bronze_col: canonical_col} → flip here
            "mapping_rules":      {cc: bc for bc, cc in mapping_rules.get(ds_name, {}).items()},
            "transformation_logic": (
    {
        k: _LiteralStr(
            v + ("\n" if v and not v.endswith("\n") else "")
        )
        for k, v in tl.items()
    }
    if isinstance(tl, dict)
    else (
        _LiteralStr(
            tl + ("\n" if tl and not tl.endswith("\n") else "")
        )
        if tl
        else None
    )
),
            "cross_entity_sql":   _LiteralStr(cs + ("\n" if cs and not cs.endswith("\n") else "")) if cs else None,
            "validation_rules": {"rules": validation_rules} if validation_rules else None,
        }

        output_datasets.append(entry)

    config = {**domain_config, "datasets": output_datasets}

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    class _NoAliasDumper(yaml.Dumper):
        def ignore_aliases(self, data):
            return True

    with open(out, "w", encoding="utf-8") as f:
        yaml.dump(config, f, Dumper=_NoAliasDumper, default_flow_style=False,
                  sort_keys=False, allow_unicode=True, width=120)

    print(f"\nSilver config YAML generated: {out}")
    print(f"  Datasets: {len(output_datasets)}")


def generate_vendor_yaml(
    vendor_catalog_path: str,
    output_path: str,
    domain_overrides: dict | None = None,
) -> None:
    """Generate vendor-level Silver YAML (defaults by vendor/entity)."""
    vendor_entities, vendor_canon_cols, vendor_val_rules = _load_vendor_catalog(vendor_catalog_path)

    domain_config = domain_overrides or {
        "domain_name": "DrugDev",
        "data_product_name": "DrugDev",
        "owner_team": "clinical_data_engineering",
    }

    entries = []
    for (vendor_name, entity_name), defaults in sorted(vendor_entities.items(), key=lambda x: (x[0][0], x[0][1])):
        vendor_cc = vendor_canon_cols.get((vendor_name, entity_name), [])
        canonical_cols = [c for c, _ in vendor_cc]
        column_types = {c: t for c, t in vendor_cc}

        entry: dict[str, Any] = {
            "vendor": vendor_name,
            "entity": entity_name,
            "scd_business_keys": defaults.get("scd_business_keys", []),
            "canonical_cols": canonical_cols,
            "column_types": column_types,
            "zorder_cols": defaults.get("zorder_cols", []),
            "table_sensitivity": defaults.get("table_sensitivity", "INTERNAL"),
            "pii_columns": defaults.get("pii_columns", []),
            "transformation_logic": (
                _LiteralStr(defaults["transformation_logic"] + ("\n" if defaults.get("transformation_logic") and not str(defaults["transformation_logic"]).endswith("\n") else ""))
                if defaults.get("transformation_logic")
                else None
            ),
            "cross_entity_sql": (
                _LiteralStr(defaults["cross_entity_sql"] + ("\n" if defaults.get("cross_entity_sql") and not str(defaults["cross_entity_sql"]).endswith("\n") else ""))
                if defaults.get("cross_entity_sql")
                else None
            ),
            "validation_rules": {"rules": vendor_val_rules.get((vendor_name, entity_name), [])} if vendor_val_rules.get((vendor_name, entity_name)) else None,
        }
        entries.append(entry)

    payload = {**domain_config, "vendor_entities": entries}

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    class _NoAliasDumper(yaml.Dumper):
        def ignore_aliases(self, data):
            return True

    with open(out, "w", encoding="utf-8") as f:
        yaml.dump(payload, f, Dumper=_NoAliasDumper, default_flow_style=False,
                  sort_keys=False, allow_unicode=True, width=120)

    print(f"\nSilver vendor YAML generated: {out}")
    print(f"  Vendor/entity defaults: {len(entries)}")


def generate_entity_yaml(
    entity_level_path: str,
    output_path: str,
    domain_overrides: dict | None = None,
    strict_validation: bool = False,
) -> None:
    """Generate entity-level Silver YAML with overrides/renames only."""
    datasets, mapping_rules, column_types, entity_val_rules = _load_entity_level(entity_level_path)

    alignment = _validate_entity_alignment(
        datasets=datasets,
        mapping_rules=mapping_rules,
        entity_val_rules=entity_val_rules,
        strict=strict_validation,
    )

    domain_config = domain_overrides or {
        "domain_name": "DrugDev",
        "data_product_name": "DrugDev",
        "owner_team": "clinical_data_engineering",
    }

    dataset_row_by_name = {
        _clean(row.get("dataset_name", "")): row
        for row in datasets
        if _clean(row.get("dataset_name", ""))
    }
    all_dataset_names = sorted(
        alignment["dataset_names"] | alignment["mapping_names"]
    )

    output_datasets: list[dict[str, Any]] = []
    for ds_name in all_dataset_names:
        row = dataset_row_by_name.get(ds_name, {})

        overrides = {
            "scd_business_keys": _split_csv(row.get("scd_business_keys")),
            "canonical_cols": _split_csv(row.get("canonical_cols")),
            "zorder_cols": _split_csv(row.get("zorder_cols")),
            "partition_cols": _split_csv(row.get("partition_cols")),
            "watermark_column": _clean(row.get("watermark_column")) or None,
            "table_sensitivity": _clean(row.get("table_sensitivity")) or None,
            "pii_columns": _parse_pii_columns(row.get("pii_columns")),
            "transformation_logic": _clean(row.get("transformation_logic")) or None,
            "cross_entity_sql": _clean(row.get("cross_entity_sql")) or None,
        }

        mapped_rules = {cc: bc for bc, cc in mapping_rules.get(ds_name, {}).items()}
        mapped_types = column_types.get(ds_name, {})
        compact_overrides = {
            "transformation_logic": (
                _LiteralStr(overrides["transformation_logic"] + ("\n" if overrides["transformation_logic"] and not str(overrides["transformation_logic"]).endswith("\n") else ""))
                if overrides["transformation_logic"]
                else None
            ),
            "cross_entity_sql": (
                _LiteralStr(overrides["cross_entity_sql"] + ("\n" if overrides["cross_entity_sql"] and not str(overrides["cross_entity_sql"]).endswith("\n") else ""))
                if overrides["cross_entity_sql"]
                else None
            ),
        }

        entry: dict[str, Any] = {
            "dataset_name": ds_name,
            "source_dataset_name": _clean(row.get("source_dataset_name")) or ds_name,
            "mapping_rules": mapped_rules,
            "overrides": compact_overrides,
        }
        output_datasets.append(entry)

    payload = {**domain_config, "datasets": output_datasets}

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    class _NoAliasDumper(yaml.Dumper):
        def ignore_aliases(self, data):
            return True

    with open(out, "w", encoding="utf-8") as f:
        yaml.dump(payload, f, Dumper=_NoAliasDumper, default_flow_style=False,
                  sort_keys=False, allow_unicode=True, width=120)

    print(f"\nSilver entity YAML generated: {out}")
    print(f"  Dataset rows: {len(output_datasets)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
    description="Silver YAML Generator — generates vendor/entity YAMLs (and optional legacy merged YAML) from two Excels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create vendor catalog template (fill in PRA, IQVIA, 4G, CENDUIT, MEDIDATA, etc.):
  python silver_yaml_generator.py --create-vendor-template docs/silver_vendor_catalog.xlsx

  # Create entity-level template (fill in one row per Silver dataset):
  python silver_yaml_generator.py --create-entity-template docs/silver_entity_level.xlsx

    # Generate split Silver YAMLs (vendor + entity):
    python silver_yaml_generator.py \
            --vendor-catalog docs/silver_vendor_catalog.xlsx \
            --entity-level   docs/silver_entity_level.xlsx \
            --vendor-output  databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_vendor_catalog.yaml \
            --entity-output  databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_entity_level.yaml

    # Optional: also generate legacy merged config for backward compatibility:
  python silver_yaml_generator.py \\
      --vendor-catalog docs/silver_vendor_catalog.xlsx \\
      --entity-level   docs/silver_entity_level.xlsx \\
      --output         databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_config.yaml
        """,
    )
    parser.add_argument("--create-vendor-template", metavar="OUTPUT_XLSX",
                        help="Create blank silver_vendor_catalog.xlsx template")
    parser.add_argument("--create-entity-template", metavar="OUTPUT_XLSX",
                        help="Create blank silver_entity_level.xlsx template")
    parser.add_argument("--vendor-catalog", metavar="VENDOR_XLSX",
                        help="Filled-in silver_vendor_catalog.xlsx")
    parser.add_argument("--entity-level", metavar="ENTITY_XLSX",
                        help="Filled-in silver_entity_level.xlsx")
    parser.add_argument("--vendor-output", metavar="VENDOR_YAML",
                        default="databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_vendor_catalog.yaml",
                        help="Output vendor-level YAML path")
    parser.add_argument("--entity-output", metavar="ENTITY_YAML",
                        default="databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_entity_level.yaml",
                        help="Output entity-level YAML path")
    parser.add_argument("--output", metavar="OUTPUT_YAML",
                        default=None,
                        help="Optional legacy merged output YAML path")
    parser.add_argument("--strict-entity-validation", action="store_true",
                        help="Fail generation when dataset_name appears in ColumnMappings/ValidationRules but not in Datasets")

    args = parser.parse_args()

    if args.create_vendor_template:
        create_vendor_template(args.create_vendor_template)
    elif args.create_entity_template:
        create_entity_template(args.create_entity_template)
    elif args.vendor_catalog and args.entity_level:
        generate_vendor_yaml(args.vendor_catalog, args.vendor_output)
        generate_entity_yaml(args.entity_level, args.entity_output, strict_validation=args.strict_entity_validation)
        if args.output:
            generate_yaml(args.vendor_catalog, args.entity_level, args.output)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

