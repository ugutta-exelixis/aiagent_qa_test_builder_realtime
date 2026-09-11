from pyspark.sql import SparkSession
from datetime import datetime
import yaml
import uuid
import logging
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils
import re
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

dbutils = DBUtils(spark)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# variables (from Spark conf)
METADATA_CATALOG = spark.conf.get("drugdev.METADATA_CATALOG")
METADATA_SCHEMA  = spark.conf.get("drugdev.METADATA_SCHEMA")
REGISTRY_SCHEMA  = spark.conf.get("drugdev.REGISTRY_SCHEMA")
YML_CONFIG_PATH  = spark.conf.get("drugdev.YML_CONFIG_PATH")
source_bucket = spark.conf.get("drugdev.source_bucket")
environment = spark.conf.get("drugdev.environment")

logger.info(f"Metadata Catalog: {METADATA_CATALOG}")
logger.info(f"Metadata Schema: {METADATA_SCHEMA}")
logger.info(f"Source bucket: {source_bucket}")
logger.info(f"Env: {environment}")

if not METADATA_CATALOG or not METADATA_SCHEMA or not REGISTRY_SCHEMA:
    raise ValueError(
        "Missing one or more required spark confs: "
        "drugdev.METADATA_CATALOG, drugdev.METADATA_SCHEMA, drugdev.REGISTRY_SCHEMA"
    )


if not YML_CONFIG_PATH:
    raise ValueError("Missing required spark conf: drugdev.YML_CONFIG_PATH")

# dynamic table names
DATASET_REGISTRY_TBL   = f"`{METADATA_CATALOG}`.{REGISTRY_SCHEMA}.dataset_registry"
INGESTION_CONFIG_TBL   = f"`{METADATA_CATALOG}`.{METADATA_SCHEMA}.ingestion_config"
DATASET_TAGS_TBL       = f"`{METADATA_CATALOG}`.{REGISTRY_SCHEMA}.dataset_tags"
DATASET_DEPS_TBL       = f"`{METADATA_CATALOG}`.{REGISTRY_SCHEMA}.dataset_dependencies"
DQ_SLA_CONFIG_TBL      = f"`{METADATA_CATALOG}`.{METADATA_SCHEMA}.dq_sla_config"

def load_config(environment):
    config_path = spark.conf.get("drugdev.YML_CONFIG_PATH")

    variables = {
        "METADATA_CATALOG": spark.conf.get("drugdev.METADATA_CATALOG"),
        "METADATA_SCHEMA": spark.conf.get("drugdev.METADATA_SCHEMA"),
        "REGISTRY_SCHEMA": spark.conf.get("drugdev.REGISTRY_SCHEMA"),
        "source_bucket": spark.conf.get("drugdev.source_bucket"),
        "SOURCE_BUCKET": spark.conf.get("drugdev.source_bucket"),
        # "environment": environment
    }

    with open(config_path, "r") as f:
        text = f.read()
    
    for k, v in variables.items():
        if v is None or v == "":
            continue
        text = re.sub(rf"\$\{{\s*{k}\s*\}}", v, text)
    # logger.info("FINAL YAML:\n" + text)
    
    unresolved = re.findall(r"\$\{.*?\}", text)
    # logger.info(f"Unresolved placeholders: {re.findall(r'\$\{.*?\}', text)}")
    
    if unresolved:
        raise ValueError(
            f"Unresolved config placeholders found: {unresolved}"
        )

    return yaml.safe_load(text)

# Upsert Dataset Registry
def load_dataset_registry(config):
    dataset_id_map = {}

    # Step 1: Read existing datasets once (avoid N queries)
    existing_df = spark.sql(f"""
        SELECT dataset_id, domain_name, data_product_name, dataset_name
        FROM {DATASET_REGISTRY_TBL}
        WHERE domain_name = '{config["domain_name"]}'
          AND data_product_name = '{config["data_product_name"]}'
    """)

    existing_map = {
        (row["dataset_name"]): row["dataset_id"]
        for row in existing_df.collect()
    }

    rows = []

    # Step 2: Prepare data safely
    for dataset in config["datasets"]:

        dataset_name = dataset["dataset_name"]

        if dataset_name in existing_map:
            dataset_id = existing_map[dataset_name]
            logger.info(f"Dataset exists. Reusing dataset_id for {dataset_name}")
        else:
            dataset_id = str(uuid.uuid4())
            logger.info(f"New dataset detected. Creating dataset_id for {dataset_name}")

        dataset_id_map[dataset_name] = dataset_id

        rows.append(Row(
            dataset_id=dataset_id,
            domain_name=config["domain_name"],
            data_product_name=config["data_product_name"],
            dataset_name=dataset_name,
            dataset_version=dataset.get("dataset_version"),
            owner_team=config["owner_team"],
            owner_email=config["owner_email"],
            criticality=dataset["criticality"],
            contains_pii=bool(dataset["contains_pii"]),
            data_classification=dataset["data_classification"],
            lifecycle_status=dataset["lifecycle_status"],
            retention_days=int(dataset["retention_days"]) if dataset.get("retention_days") is not None else None,
            created_at=datetime.now(),
            is_active=bool(dataset["is_active"]),
            frequency=dataset["frequency"]
        ))

    if not rows:
        logger.info("No datasets to process.")
        return dataset_id_map

    # Step 3: Create DataFrame (schema inferred correctly)
    source_df = spark.createDataFrame(rows)

    # Step 4: Add created_at column properly
    # source_df = source_df.withColumn("created_at", F.current_timestamp())

    # Step 5: Create temp view
    source_df.createOrReplaceTempView("dataset_registry_source")

    # Step 6: MERGE (clean and safe)
    spark.sql(f"""
        MERGE INTO {DATASET_REGISTRY_TBL} t
        USING dataset_registry_source s
        ON t.dataset_id = s.dataset_id
        WHEN MATCHED THEN UPDATE SET
            t.domain_name = s.domain_name,
            t.data_product_name = s.data_product_name,
            t.dataset_name = s.dataset_name,
            t.dataset_version = s.dataset_version,
            t.owner_team = s.owner_team,
            t.owner_email = s.owner_email,
            t.criticality = s.criticality,
            t.contains_pii = s.contains_pii,
            t.data_classification = s.data_classification,
            t.lifecycle_status = s.lifecycle_status,
            t.retention_days = s.retention_days,
            t.is_active = s.is_active,
            t.frequency = s.frequency
        WHEN NOT MATCHED THEN INSERT (
            dataset_id,
            domain_name,
            data_product_name,
            dataset_name,
            dataset_version,
            owner_team,
            owner_email,
            criticality,
            contains_pii,
            data_classification,
            lifecycle_status,
            retention_days,
            created_at,
            is_active,
            frequency
        )
        VALUES (
            s.dataset_id,
            s.domain_name,
            s.data_product_name,
            s.dataset_name,
            s.dataset_version,
            s.owner_team,
            s.owner_email,
            s.criticality,
            s.contains_pii,
            s.data_classification,
            s.lifecycle_status,
            s.retention_days,
            s.created_at,
            s.is_active,
            s.frequency
        )
    """)

    return dataset_id_map


# Upsert Ingestion Config
def load_ingestion_config(config, dataset_id_map):
    rows = []

    # Step 1: Load existing configs once
    existing_df = spark.sql(f"""
        SELECT config_id, dataset_id, environment
        FROM {INGESTION_CONFIG_TBL}
    """)

    existing_map = {
        (row["dataset_id"], row["environment"]): row["config_id"]
        for row in existing_df.collect()
    }

    # Step 2: Prepare rows
    for dataset in config["datasets"]:

        dataset_name = dataset["dataset_name"]
        dataset_id = dataset_id_map[dataset_name]
        runtime_environment = environment
        runtime_source_bucket = source_bucket

        key = (dataset_id, runtime_environment)
        # logger.info(f"Existing key: {key}")
        # logger.info(f"Incoming key: {key}")
        # logger.info(f"Existing keys: {list(existing_map.keys())[:10]}")

        if key in existing_map:
            config_id = existing_map[key]
            logger.info(f"Ingestion config exists. Reusing config_id for {dataset_name}")
        else:
            config_id = str(uuid.uuid4())
            logger.info(f"New ingestion config created for {dataset_name}")

        # Convert primary keys list → string
        primary_keys_list = dataset.get("primary_keys")
        primary_keys = ",".join(primary_keys_list) if primary_keys_list else None

        rows.append(Row(
            config_id=config_id,
            dataset_id=dataset_id,
            environment=runtime_environment,
            source_bucket=runtime_source_bucket,
            source_path=dataset.get("source_path"),
            file_format=dataset.get("file_format"),
            delimiter=dataset.get("delimiter"),
            file_encoding=dataset.get("encoding"),
            load_type=dataset.get("load_type"),
            ingestion_mode=dataset.get("ingestion_mode"),
            primary_keys=primary_keys,
            watermark_column=dataset.get("watermark_column"),
            checkpoint_location=dataset.get("checkpoint_location"),
            schema_location=dataset.get("schema_location"),
            row_tag=dataset.get("row_tag"),
            file_name=dataset.get("file_name"),
            schema_strategy=None,
            optimize_write=None,
            auto_compact=None,
            is_active=bool(dataset.get("is_active")) if dataset.get("is_active") is not None else None
        ))
        
    schema = StructType([
        StructField("config_id", StringType(), True),
        StructField("dataset_id", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("source_bucket", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("file_format", StringType(), True),
        StructField("delimiter", StringType(), True),
        StructField("file_encoding", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("ingestion_mode", StringType(), True),
        StructField("primary_keys", StringType(), True),
        StructField("watermark_column", StringType(), True),
        StructField("checkpoint_location", StringType(), True),
        StructField("schema_location", StringType(), True),
        StructField("row_tag", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("schema_strategy", StringType(), True),
        StructField("optimize_write", StringType(), True),
        StructField("auto_compact", StringType(), True),
        StructField("is_active", BooleanType(), True),
    ])

    if not rows:
        logger.info("No ingestion configs to process.")
        return

    # Step 3: Create DataFrame
    source_df = spark.createDataFrame(rows, schema=schema)

    # Step 4: Deduplicate (important!)
    source_df = source_df.dropDuplicates(["config_id"])

    # Step 5: Create temp view
    source_df.createOrReplaceTempView("ingestion_config_source")

    # Step 6: MERGE (explicit, safe)
    spark.sql(f"""
        MERGE INTO {INGESTION_CONFIG_TBL} t
        USING ingestion_config_source s
        ON t.config_id = s.config_id

        WHEN MATCHED THEN UPDATE SET
            t.dataset_id = s.dataset_id,
            t.environment = s.environment,
            t.source_bucket = s.source_bucket,
            t.source_path = s.source_path,
            t.file_format = s.file_format,
            t.delimiter = s.delimiter,
            t.file_encoding = s.file_encoding,
            t.load_type = s.load_type,
            t.ingestion_mode = s.ingestion_mode,
            t.primary_keys = s.primary_keys,
            t.watermark_column = s.watermark_column,
            t.checkpoint_location = s.checkpoint_location,
            t.schema_location = s.schema_location,
            t.row_tag = s.row_tag,
            t.file_name = s.file_name,
            t.schema_strategy = s.schema_strategy,
            t.optimize_write = s.optimize_write,
            t.auto_compact = s.auto_compact,
            t.is_active = s.is_active

        WHEN NOT MATCHED THEN INSERT (
            config_id,
            dataset_id,
            environment,
            source_bucket,
            source_path,
            file_format,
            delimiter,
            file_encoding,
            load_type,
            ingestion_mode,
            primary_keys,
            watermark_column,
            checkpoint_location,
            schema_location,
            schema_strategy,
            row_tag,
            file_name,
            optimize_write,
            auto_compact,
            is_active
        )
        VALUES (
            s.config_id,
            s.dataset_id,
            s.environment,
            s.source_bucket,
            s.source_path,
            s.file_format,
            s.delimiter,
            s.file_encoding,
            s.load_type,
            s.ingestion_mode,
            s.primary_keys,
            s.watermark_column,
            s.checkpoint_location,
            s.schema_location,
            s.schema_strategy,
            s.row_tag,
            s.file_name,
            s.optimize_write,
            s.auto_compact,
            s.is_active
        )
    """)

# Upsert Dataset Tags
def load_dataset_tags(config, dataset_id_map):

    rows = []

    # Step 1: Prepare rows
    for dataset in config["datasets"]:

        dataset_name = dataset["dataset_name"]
        dataset_id = dataset_id_map[dataset_name]

        tags = dataset.get("tags")

        if not tags:
            logger.info(f"No tags defined for {dataset_name}. Skipping.")
            continue

        for tag in tags:

            tag_key = tag.get("key")
            tag_value = tag.get("value")

            if not tag_key or not tag_value:
                logger.warning(f"Invalid tag detected for {dataset_name}. Skipping.")
                continue

            rows.append(Row(
                dataset_id=dataset_id,
                tag_key=tag_key,
                tag_value=tag_value
            ))

            logger.info(f"Tag prepared for {dataset_name}: {tag_key} = {tag_value}")

    if not rows:
        logger.info("No dataset tags to process.")
        return

    # Step 2: Create DataFrame
    source_df = spark.createDataFrame(rows)

    # Step 3: Deduplicate (VERY IMPORTANT)
    source_df = source_df.dropDuplicates(["dataset_id", "tag_key", "tag_value"])

    # Step 4: Add tag_id + created_at
    source_df = (
        source_df
        .withColumn("tag_id", F.expr("uuid()"))
        .withColumn("created_at", F.current_timestamp())
    )

    # Step 5: Create temp view
    source_df.createOrReplaceTempView("dataset_tags_source")

    # Step 6: MERGE (insert-only pattern)
    spark.sql(f"""
        MERGE INTO {DATASET_TAGS_TBL} t
        USING dataset_tags_source s
        ON t.dataset_id = s.dataset_id
           AND t.tag_key = s.tag_key
           AND t.tag_value = s.tag_value

        WHEN NOT MATCHED THEN INSERT (
            tag_id,
            dataset_id,
            tag_key,
            tag_value,
            created_at
        )
        VALUES (
            s.tag_id,
            s.dataset_id,
            s.tag_key,
            s.tag_value,
            s.created_at
        )
    """)


# Upsert Dataset Dependencies
def load_dataset_dependencies(config, dataset_id_map):
    rows = []

    # Step 1: Prepare rows + validations
    for dataset in config["datasets"]:

        dataset_name = dataset["dataset_name"]
        dataset_id = dataset_id_map.get(dataset_name)

        dependencies = dataset.get("dependencies")

        if not dependencies:
            logger.info(f"No dependencies defined for {dataset_name}. Skipping.")
            continue

        for dependency in dependencies:

            depends_on_name = dependency.get("dataset_name")
            dependency_type = dependency.get("dependency_type")

            # Validation: dependency dataset must exist
            if depends_on_name not in dataset_id_map:
                raise ValueError(
                    f"Dependency dataset '{depends_on_name}' not found in dataset_registry."
                )

            # Prevent self-dependency
            if depends_on_name == dataset_name:
                raise ValueError(
                    f"Dataset '{dataset_name}' cannot depend on itself."
                )

            depends_on_dataset_id = dataset_id_map[depends_on_name]

            rows.append(Row(
                dataset_id=dataset_id,
                depends_on_dataset_id=depends_on_dataset_id,
                dependency_type=dependency_type
            ))

            logger.info(f"Dependency prepared: {dataset_name} → {depends_on_name}")

    if not rows:
        logger.info("No dataset dependencies to process.")
        return

    # Step 2: Create DataFrame
    source_df = spark.createDataFrame(rows)

    # Step 3: Deduplicate (CRITICAL)
    source_df = source_df.dropDuplicates([
        "dataset_id",
        "depends_on_dataset_id"
    ])

    # Step 4: Add dependency_id + created_at
    source_df = (
        source_df
        .withColumn("dependency_id", F.expr("uuid()"))
        .withColumn("created_at", F.current_timestamp())
    )

    # Step 5: Temp view
    source_df.createOrReplaceTempView("dataset_dependencies_source")

    # Step 6: MERGE (insert-only pattern)
    spark.sql(f"""
        MERGE INTO {DATASET_DEPS_TBL} t
        USING dataset_dependencies_source s
        ON t.dataset_id = s.dataset_id
           AND t.depends_on_dataset_id = s.depends_on_dataset_id

        WHEN NOT MATCHED THEN INSERT (
            dependency_id,
            dataset_id,
            depends_on_dataset_id,
            dependency_type,
            created_at
        )
        VALUES (
            s.dependency_id,
            s.dataset_id,
            s.depends_on_dataset_id,
            s.dependency_type,
            s.created_at
        )
    """)
 
# Upsert Data Quality SLA Config
def load_dq_sla_config(config, dataset_id_map):
    rows = []

    # Step 1: Prepare rows
    for dataset in config["datasets"]:

        dataset_name = dataset["dataset_name"]
        dataset_id = dataset_id_map.get(dataset_name)

        dq_rules = dataset.get("dq_rules")

        if not dq_rules:
            logger.info(f"No DQ rules defined for {dataset_name}. Skipping.")
            continue

        for rule in dq_rules:

            rule_set_name = rule.get("rule_set_name")
            dq_enabled = rule.get("dq_enabled", False)
            fail_action = rule.get("fail_action")
            alert_channel = rule.get("alert_channel")
            escalation_contact = rule.get("escalation_contact")

            if not rule_set_name:
                logger.warning(f"Invalid DQ rule for {dataset_name}. Skipping.")
                continue

            rows.append(Row(
                dataset_id=dataset_id,
                rule_set_name=rule_set_name,
                dq_enabled=bool(dq_enabled), 
                row_count_min=None,
                row_count_max=None,
                freshness_minutes=None,
                null_threshold_pct=None,
                duplicate_threshold_pct=None,
                anomaly_detection_enabled=None,
                fail_action=fail_action,
                alert_channel=alert_channel,
                escalation_contact=escalation_contact
            ))

            logger.info(f"DQ rule prepared for {dataset_name}: {rule_set_name}")
            
    schema = StructType([
        StructField("dataset_id", StringType(), True),
        StructField("rule_set_name", StringType(), True),
        StructField("dq_enabled", BooleanType(), True),
        StructField("row_count_min", IntegerType(), True),
        StructField("row_count_max", IntegerType(), True),
        StructField("freshness_minutes", IntegerType(), True),
        StructField("null_threshold_pct", DoubleType(), True),
        StructField("duplicate_threshold_pct", DoubleType(), True),
        StructField("anomaly_detection_enabled", BooleanType(), True),
        StructField("fail_action", StringType(), True),
        StructField("alert_channel", StringType(), True),
        StructField("escalation_contact", StringType(), True),
    ])
    
    if not rows:
        logger.info("No DQ rules to process.")
        return

    # Step 2: Create DataFrame
    source_df = spark.createDataFrame(rows, schema=schema)

    # Step 3: Deduplicate (CRITICAL)
    source_df = source_df.dropDuplicates(["dataset_id", "rule_set_name"])

    # Step 4: Add dq_id + created_at
    source_df = (
        source_df
        .withColumn("dq_id", F.expr("uuid()"))
        .withColumn("created_at", F.current_timestamp())
    )

    # Step 5: Temp view
    source_df.createOrReplaceTempView("dq_sla_source")

    # Step 6: MERGE (insert-only)
    spark.sql(f"""
        MERGE INTO {DQ_SLA_CONFIG_TBL} t
        USING dq_sla_source s
        ON t.dataset_id = s.dataset_id
           AND t.rule_set_name = s.rule_set_name

        WHEN NOT MATCHED THEN INSERT (
            dq_id,
            dataset_id,
            dq_enabled,
            rule_set_name,
            row_count_min,
            row_count_max,
            freshness_minutes,
            null_threshold_pct,
            duplicate_threshold_pct,
            anomaly_detection_enabled,
            fail_action,
            alert_channel,
            escalation_contact,
            created_at
        )
        VALUES (
            s.dq_id,
            s.dataset_id,
            s.dq_enabled,
            s.rule_set_name,
            s.row_count_min,
            s.row_count_max,
            s.freshness_minutes,
            s.null_threshold_pct,
            s.duplicate_threshold_pct,
            s.anomaly_detection_enabled,
            s.fail_action,
            s.alert_channel,
            s.escalation_contact,
            s.created_at
        )
    """)
 

# Main
def main():

    config = load_config(environment)
    logger.info("Updated config with parameters.")

    dataset_id_map = load_dataset_registry(config)
    logger.info("Dataset registry upsert complete.")

    load_ingestion_config(config, dataset_id_map)
    logger.info("Ingestion config upsert complete.")
    
    load_dataset_tags(config, dataset_id_map)
    logger.info("Dataset tags upsert complete.")
    
    load_dataset_dependencies(config, dataset_id_map)
    logger.info("Dataset dependencies upsert complete.")
    
    load_dq_sla_config(config, dataset_id_map)
    logger.info("DQ rules upsert complete.")

    logger.info("Metadata successfully loaded.")


if __name__ == "__main__":
    main()