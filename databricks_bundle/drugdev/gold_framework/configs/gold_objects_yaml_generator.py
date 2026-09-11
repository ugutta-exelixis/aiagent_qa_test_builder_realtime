import argparse
import json

import pandas as pd
import yaml


REQUIRED_COLUMNS = [
    "name",
    "class_path",
    "type",
    "strategy",
    "write_mode",
    "order",
    "dependencies",
    "is_enabled",
    "partition_cols",
    "tags",
    "column_tags",
]


def _split_csv(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    sval = str(value).strip()
    if not sval or sval.lower() in {"nan", "none", "null"}:
        return []
    return [v.strip() for v in sval.split(",") if v.strip()]


def _to_int(value, default=99):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        return int(value)
    except Exception:
        return default


def _to_bool(value, default=True):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    sval = str(value).strip().lower()
    if sval in {"true", "1", "yes", "y"}:
        return True
    if sval in {"false", "0", "no", "n"}:
        return False
    return default


def _safe_tags(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    sval = str(value).strip()
    if not sval:
        return {}
    try:
        parsed = json.loads(sval)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _safe_column_tags(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    sval = str(value).strip()
    if not sval or sval.lower() in {"nan", "none", "null"}:
        return {}
    try:
        parsed = json.loads(sval)
    except json.JSONDecodeError:
        return {}

    if not isinstance(parsed, dict):
        return {}

    normalized = {}
    for col_name, meta in parsed.items():
        col_key = str(col_name).strip()
        if not col_key:
            continue
        if isinstance(meta, dict):
            clean_meta = {}
            sensitivity = meta.get("sensitivity")
            pii_category = meta.get("pii_category")
            if sensitivity is not None and str(sensitivity).strip():
                clean_meta["sensitivity"] = str(sensitivity).strip().upper()
            if pii_category is not None and str(pii_category).strip():
                clean_meta["pii_category"] = str(pii_category).strip().upper()
            if clean_meta:
                normalized[col_key] = clean_meta
    return normalized


def excel_to_yaml(input_excel: str, output_yaml: str) -> None:
    df = pd.read_excel(input_excel, sheet_name="GoldObjects")

    # Backward-compatible: allow older Excel files without column_tags column.
    if "column_tags" not in df.columns:
        df["column_tags"] = ""

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in GoldObjects sheet: {missing}")

    objects = []
    for _, row in df.iterrows():
        name = str(row.get("name", "")).strip()
        if not name:
            continue

        obj = {
            "name": name,
            "class_path": str(row.get("class_path", "")).strip(),
            "type": str(row.get("type", "")).strip(),
            "strategy": str(row.get("strategy", "")).strip(),
            "write_mode": str(row.get("write_mode", "overwrite")).strip() or "overwrite",
            "order": _to_int(row.get("order"), 99),
            "dependencies": _split_csv(row.get("dependencies")),
            "is_enabled": _to_bool(row.get("is_enabled"), True),
        }

        partition_cols = _split_csv(row.get("partition_cols"))
        if partition_cols:
            obj["partition_cols"] = partition_cols

        tags = _safe_tags(row.get("tags"))
        column_tags = _safe_column_tags(row.get("column_tags"))
        if column_tags:
            tags = dict(tags or {})
            tags["column_tags"] = column_tags

        if tags:
            obj["tags"] = tags

        objects.append(obj)

    config = {"objects": objects}

    with open(output_yaml, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"YAML created successfully: {output_yaml}")
    print(f"Objects: {len(objects)}")


def main():
    parser = argparse.ArgumentParser(description="Generate gold_objects.yaml from GoldObjects Excel")
    parser.add_argument("--input", "-i", required=True, help="Input Excel path")
    parser.add_argument("--output", "-o", required=True, help="Output YAML path")
    args = parser.parse_args()

    excel_to_yaml(args.input, args.output)


if __name__ == "__main__":
    main()
