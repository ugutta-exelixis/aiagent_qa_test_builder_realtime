"""
Gold Metadata Loader
Reads drugdev_gold_config.yaml and upserts:
  - <METADATA_CATALOG>.<REGISTRY_SCHEMA>.gold_object_registry

Spark conf parameters required:
  drugdev.METADATA_CATALOG       Unity Catalog for metadata tables
  drugdev.METADATA_SCHEMA        Schema holding DQ/ingestion metadata
  drugdev.REGISTRY_SCHEMA        Schema holding gold_object_registry
  drugdev.YML_GOLD_CONFIG_PATH   DBFS/workspace path to drugdev_gold_config.yaml
  drugdev.source_bucket          Source S3 bucket (for variable substitution)
  drugdev.environment            Runtime environment label (dev/qa/prod)
  drugdev.CATALOG                Unity Catalog for Silver/Gold data tables
  drugdev.SILVER_SCHEMA          Silver schema inside CATALOG
  drugdev.GOLD_SCHEMA            Gold schema inside CATALOG
"""

import json
import logging
import re
import uuid
from datetime import datetime

import yaml
from pyspark.dbutils import DBUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Spark conf ─────────────────────────────────────────────────────────────────
METADATA_CATALOG = spark.conf.get("drugdev.METADATA_CATALOG")
METADATA_SCHEMA  = spark.conf.get("drugdev.METADATA_SCHEMA")
REGISTRY_SCHEMA  = spark.conf.get("drugdev.REGISTRY_SCHEMA")
YML_CONFIG_PATH  = spark.conf.get("drugdev.YML_GOLD_CONFIG_PATH")
source_bucket    = spark.conf.get("drugdev.source_bucket")
environment      = spark.conf.get("drugdev.environment")
CATALOG          = spark.conf.get("drugdev.CATALOG")
SILVER_SCHEMA    = spark.conf.get("drugdev.SILVER_SCHEMA")
GOLD_SCHEMA      = spark.conf.get("drugdev.GOLD_SCHEMA")

for key, val in {
    "drugdev.METADATA_CATALOG": METADATA_CATALOG,
    "drugdev.METADATA_SCHEMA": METADATA_SCHEMA,
    "drugdev.REGISTRY_SCHEMA": REGISTRY_SCHEMA,
    "drugdev.YML_GOLD_CONFIG_PATH": YML_CONFIG_PATH,
    "drugdev.CATALOG": CATALOG,
    "drugdev.SILVER_SCHEMA": SILVER_SCHEMA,
    "drugdev.GOLD_SCHEMA": GOLD_SCHEMA,
}.items():
    if not val:
        raise ValueError(f"Missing required Spark conf: {key}")

logger.info(f"METADATA_CATALOG : {METADATA_CATALOG}")
logger.info(f"REGISTRY_SCHEMA  : {REGISTRY_SCHEMA}")
logger.info(f"CATALOG          : {CATALOG}")
logger.info(f"GOLD_SCHEMA      : {GOLD_SCHEMA}")

# ── Table FQN ──────────────────────────────────────────────────────────────────
GOLD_OBJECT_REGISTRY_TBL = f"`{METADATA_CATALOG}`.{REGISTRY_SCHEMA}.gold_object_registry"


# ── Config loader ──────────────────────────────────────────────────────────────

def load_config():
    variables = {
        "METADATA_CATALOG": METADATA_CATALOG,
        "METADATA_SCHEMA": METADATA_SCHEMA,
        "REGISTRY_SCHEMA": REGISTRY_SCHEMA,
        "CATALOG": CATALOG,
        "SILVER_SCHEMA": SILVER_SCHEMA,
        "GOLD_SCHEMA": GOLD_SCHEMA,
        "silver_schema": f"`{CATALOG}`.{SILVER_SCHEMA}",
        "gold_schema": f"`{CATALOG}`.{GOLD_SCHEMA}",
        "catalog": f"`{CATALOG}`",
        "source_bucket": source_bucket,
        "SOURCE_BUCKET": source_bucket,
    }
    with open(YML_CONFIG_PATH, "r") as f:
        text = f.read()
    for k, v in variables.items():
        if v:
            text = re.sub(rf"\$\{{\s*{k}\s*\}}", v, text)
    unresolved = re.findall(r"\$\{.*?\}", text)
    if unresolved:
        raise ValueError(f"Unresolved config placeholders: {unresolved}")
    return yaml.safe_load(text)


# ── gold_object_registry ───────────────────────────────────────────────────────

def load_gold_object_registry(config):
    """Upsert gold_object_registry — one row per Gold object."""

    existing_df = spark.sql(f"""
        SELECT object_id, object_name
        FROM {GOLD_OBJECT_REGISTRY_TBL}
    """)
    existing_map = {row["object_name"]: row["object_id"] for row in existing_df.collect()}

    rows = []

    for obj in config.get("objects", []):
        obj_name = obj.get("object_name")
        if not obj_name:
            obj_name = obj.get("name")
        if not obj_name:
            logger.warning("Skipping object with no object_name.")
            continue

        if obj_name in existing_map:
            object_id = existing_map[obj_name]
            logger.info(f"Reusing object_id for: {obj_name}")
        else:
            object_id = str(uuid.uuid4())
            logger.info(f"New object_id created for: {obj_name}")

        # Dependencies → JSON array string
        deps = obj.get("dependencies") or []
        dependencies_json = json.dumps(deps) if deps else "[]"

        # Partition cols → JSON array string
        parts = obj.get("partition_cols") or []
        partition_cols_json = json.dumps(parts) if parts else None

        # Tags can be either:
        #   1) dict style: {"mart": "dim_study.sql"}
        #   2) list style: [{"key": "mart", "value": "dim_study.sql"}]
        tags_raw = obj.get("tags")
        tags_dict = {}
        if isinstance(tags_raw, dict):
            tags_dict = {str(k): v for k, v in tags_raw.items()}
        elif isinstance(tags_raw, list):
            tags_dict = {
                t.get("key"): t.get("value")
                for t in tags_raw
                if isinstance(t, dict) and t.get("key") is not None
            }
        tags_json = json.dumps(tags_dict) if tags_dict else None

        rows.append(Row(
            object_id=object_id,
            object_name=obj_name,
            object_type=obj.get("object_type") or obj.get("type"),
            build_strategy=obj.get("build_strategy") or obj.get("strategy"),
            class_path=obj.get("class_path"),
            write_mode=obj.get("write_mode", "overwrite"),
            execution_order=int(obj.get("execution_order", obj.get("order", 99))) if obj.get("execution_order", obj.get("order")) is not None else 99,
            dependencies=dependencies_json,
            sql_template=obj.get("sql_template"),
            partition_cols=partition_cols_json,
            is_enabled=bool(obj.get("is_enabled", True)),
            tags=tags_json,
            domain_name=config.get("domain_name"),
            data_product_name=config.get("data_product_name"),
            created_at=datetime.now(),
        ))

    if not rows:
        logger.info("No Gold objects to process.")
        return

    schema = StructType([
        StructField("object_id", StringType(), True),
        StructField("object_name", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("build_strategy", StringType(), True),
        StructField("class_path", StringType(), True),
        StructField("write_mode", StringType(), True),
        StructField("execution_order", IntegerType(), True),
        StructField("dependencies", StringType(), True),
        StructField("sql_template", StringType(), True),
        StructField("partition_cols", StringType(), True),
        StructField("is_enabled", BooleanType(), True),
        StructField("tags", StringType(), True),
        StructField("domain_name", StringType(), True),
        StructField("data_product_name", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ])

    source_df = spark.createDataFrame(rows, schema=schema)
    source_df = source_df.dropDuplicates(["object_id"])
    source_df.createOrReplaceTempView("gold_object_registry_source")

    spark.sql(f"""
        MERGE INTO {GOLD_OBJECT_REGISTRY_TBL} t
        USING gold_object_registry_source s
        ON t.object_id = s.object_id

        WHEN MATCHED THEN UPDATE SET
            t.object_name       = s.object_name,
            t.object_type       = s.object_type,
            t.build_strategy    = s.build_strategy,
            t.class_path        = s.class_path,
            t.write_mode        = s.write_mode,
            t.execution_order   = s.execution_order,
            t.dependencies      = s.dependencies,
            t.sql_template      = s.sql_template,
            t.partition_cols    = s.partition_cols,
            t.is_enabled        = s.is_enabled,
            t.tags              = s.tags,
            t.domain_name       = s.domain_name,
            t.data_product_name = s.data_product_name

        WHEN NOT MATCHED THEN INSERT (
            object_id, object_name, object_type, build_strategy, class_path,
            write_mode, execution_order, dependencies, sql_template, partition_cols,
            is_enabled, tags, domain_name, data_product_name, created_at
        )
        VALUES (
            s.object_id, s.object_name, s.object_type, s.build_strategy, s.class_path,
            s.write_mode, s.execution_order, s.dependencies, s.sql_template, s.partition_cols,
            s.is_enabled, s.tags, s.domain_name, s.data_product_name, s.created_at
        )
    """)
    logger.info(f"gold_object_registry upsert complete. Rows: {len(rows)}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    config = load_config()
    logger.info(f"Config loaded. Objects: {len(config.get('objects', []))}")

    load_gold_object_registry(config)
    logger.info("Gold metadata loaded successfully.")


if __name__ == "__main__":
    main()
