from pyspark.sql import SparkSession
import logging
from datetime import datetime
import uuid
import re
import json
from pyspark.sql import functions as F
# from schema_registry import check_and_update_schema
import openpyxl
from pyspark.sql.functions import from_xml, col, lit, explode
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, 
    LongType, FloatType, BooleanType, DateType, TimestampType, DecimalType
)
import yaml
import os
from pyspark.dbutils import DBUtils
import pandas as pd
from io import BytesIO
import sys 
from concurrent.futures import ThreadPoolExecutor, as_completed

spark = SparkSession.builder.getOrCreate()

dbutils = DBUtils(spark)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# _CONFIG_CACHE = None

def main(simulation_type):
    METADATA_CATALOG = spark.conf.get("drugdev.METADATA_CATALOG")
    METADATA_SCHEMA  = spark.conf.get("drugdev.METADATA_SCHEMA")
    REGISTRY_SCHEMA  = spark.conf.get("drugdev.REGISTRY_SCHEMA")
    YML_CONFIG_PATH  = spark.conf.get("drugdev.YML_CONFIG_PATH")
    SCHEMA_REGISTRY_PATH = spark.conf.get("drugdev.SCHEMA_REGISTRY_PATH")
    raw_bucket       = spark.conf.get("drugdev.raw_bucket")
    source_bucket    = spark.conf.get("drugdev.source_bucket")
    environment      = spark.conf.get("drugdev.environment")
    CATALOG          = spark.conf.get("drugdev.CATALOG")
    BRONZE_SCHEMA    = spark.conf.get("drugdev.BRONZE_SCHEMA")


    RUN_DATE = spark.conf.get("drugdev.RUN_DATE", datetime.now().strftime("%Y%m%d"))

    missing = [k for k, v in {
        "drugdev.METADATA_CATALOG": METADATA_CATALOG,
        "drugdev.raw_bucket": raw_bucket,
        "drugdev.METADATA_SCHEMA": METADATA_SCHEMA,
        "drugdev.REGISTRY_SCHEMA": REGISTRY_SCHEMA,
        "drugdev.CATALOG": CATALOG,
        "drugdev.BRONZE_SCHEMA": BRONZE_SCHEMA,
    }.items() if not v]

    if missing:
        raise ValueError(f"Missing required Spark conf(s): {', '.join(missing)}")

    DATASET_REGISTRY_TBL = f"`{METADATA_CATALOG}`.{REGISTRY_SCHEMA}.dataset_registry"
    INGESTION_CONFIG_TBL = f"`{METADATA_CATALOG}`.{METADATA_SCHEMA}.ingestion_config"
    RUNTIME_STATE_TBL    = f"`{METADATA_CATALOG}`.{METADATA_SCHEMA}.ingestion_runtime_state"
    SCHEMA_REGISTRY_TBL = f"`{METADATA_CATALOG}`.{METADATA_SCHEMA}.schema_registry"
    
    # Load config once (FIX: avoid re-reading file on every dataset iteration)
    def load_config():
        # global _CONFIG_CACHE
        # if _CONFIG_CACHE is None:
        logger.info(f"Loading configuration from: {YML_CONFIG_PATH}")
        with open(YML_CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)
        logger.info("Configuration loaded")
        return config

    
    # Helper to load schema from config file for any dataset
    def get_struct_schema(dataset_name, config):
        # config = load_config()
        # logger.info(f"Successfully retrieved config:{config}")
        for ds in config.get("datasets", []):
            schema = ds.get("schema")
            logger.info(f"Checking dataset: {ds.get('dataset_name')}, schema: {schema}")
            # FIX: Handle both Python None and string "None"
            if ds.get("dataset_name") == dataset_name and schema is not None and schema != "None":
                fields = []
                for col in schema:
                    dtype = col["type"].lower()
                    
                    # Map config types to PySpark types
                    if dtype == "string":
                        spark_type = StringType()
                    elif dtype == "double":
                        spark_type = DoubleType()
                    elif dtype in ["integer", "int"]:
                        spark_type = IntegerType()
                    elif dtype == "long":
                        spark_type = LongType()
                    elif dtype == "float":
                        spark_type = FloatType()
                    elif dtype == "boolean":
                        spark_type = BooleanType()
                    elif dtype == "date":
                        spark_type = DateType()
                    elif dtype == "timestamp":
                        spark_type = TimestampType()
                    elif dtype.startswith("decimal"):
                        # Handle decimal(precision, scale) format
                        spark_type = DecimalType()
                    else:
                        logger.warning(f"Unknown type '{dtype}' for column '{col['name']}', defaulting to StringType")
                        spark_type = StringType()
                    
                    fields.append(StructField(col["name"], spark_type, True))
                return StructType(fields)
        return None

    def check_and_update_schema(spark, dataset_id, df, logger):

        # Extract sorted column names
        incoming_columns = sorted(df.columns)
        incoming_schema_json = json.dumps(incoming_columns)
        incoming_schema_sql = incoming_schema_json.replace("'", "''")

        registry_df = spark.sql(f"""
            SELECT schema_json, version
            FROM {SCHEMA_REGISTRY_TBL}
            WHERE dataset_id = '{dataset_id}'
            AND is_active = true
        """)

        records = registry_df.collect()

        if not records:
            schema_changed = True
            new_version = 1
        else:
            existing_schema = records[0]["schema_json"]
            current_version = records[0]["version"]

            if existing_schema == incoming_schema_json:
                logger.info(f"No schema change for {dataset_id}")
                return

            schema_changed = True
            new_version = current_version + 1

        # Deactivate old
        spark.sql(f"""
            UPDATE {SCHEMA_REGISTRY_TBL}
            SET is_active = false
            WHERE dataset_id = '{dataset_id}'
            AND is_active = true
        """)

        # Insert new
        spark.sql(f"""
            INSERT INTO {SCHEMA_REGISTRY_TBL}
            VALUES (
                '{dataset_id}',
                '{incoming_schema_sql}',
                {new_version},
                true,
                current_timestamp()
            )
        """)

        logger.info(f"Schema updated for {dataset_id}. Version {new_version}") 


    # Fetch Active Dataset Configurations
    configs = spark.sql(f"""
        SELECT
            r.dataset_id,
            r.dataset_name,
            c.source_bucket,
            c.source_path,
            c.file_format,
            c.environment,
            c.checkpoint_location,
            c.schema_location,
            c.delimiter,
            c.file_encoding,
            c.load_type,
            r.frequency,
            c.file_name,
            c.row_tag
        FROM {DATASET_REGISTRY_TBL} r
        JOIN {INGESTION_CONFIG_TBL} c
            ON r.dataset_id = c.dataset_id
        WHERE r.is_active = true
        AND c.is_active = true
    """)
    
    def get_dataset_tags(dataset_name, config):
        """Fetch dataset tags from config by dataset name."""
        # config = load_config(config_path)
        for dataset in config.get("datasets", []):
            if dataset.get("dataset_name") == dataset_name:
                return dataset.get("tags", [])
        return []


    def apply_table_tags(spark, full_table_name, dataset_name, tags):
        """Apply dataset tags to Unity Catalog table so they are visible in Databricks UI."""
        if not tags:
            logger.info(f"[{dataset_name}] No tags configured for table. Skipping tag application.")
            return

        applied_count = 0
        for tag in tags:
            tag_key = tag.get("key") if isinstance(tag, dict) else None
            tag_value = tag.get("value") if isinstance(tag, dict) else None

            if not tag_key or tag_value is None:
                logger.warning(f"[{dataset_name}] Invalid tag entry detected: {tag}. Skipping.")
                continue

            escaped_key = str(tag_key).replace("'", "''")
            escaped_value = str(tag_value).replace("'", "''")

            try:
                spark.sql(
                    f"ALTER TABLE {full_table_name} SET TAGS ('{escaped_key}' = '{escaped_value}')"
                )
                applied_count += 1
            except Exception as tag_error:
                logger.warning(
                    f"[{dataset_name}] Failed to apply tag {tag_key}={tag_value} on {full_table_name}: {str(tag_error)}"
                )
        logger.info(f"[{dataset_name}] Applied {applied_count} tag(s) to table {full_table_name}")
    
    
    # Helper to get latest date folder using PySpark
    def get_latest_date_folder_spark(source_bucket, source_path):

        base_path = f"s3://{source_bucket}/{source_path}"

        try:
            entries = dbutils.fs.ls(base_path)
        except Exception as e:
            raise Exception(f"Error accessing path {base_path}: {str(e)}")

        date_folders = []

        for entry in entries:
            if entry.isDir():
                # Extract folder name
                folder_name = entry.name.rstrip("/")

                if re.match(r"^[0-9]{8}$", folder_name):
                    date_folders.append(folder_name)

        if not date_folders:
            raise Exception(f"No date folders found in {base_path}")

        return max(date_folders)

    def resolve_source_path(row):
        freq = getattr(row, 'frequency', None)
        if row.dataset_name == 'drugdev_simulation_bronze':
            today = get_latest_date_folder_spark(row.source_bucket, row.source_path)
            return (f"s3://{row.source_bucket}/{row.source_path}{today}", today)
        elif freq and freq.lower() == 'daily':
            today = get_latest_date_folder_spark(row.source_bucket, row.source_path)
            return (f"s3://{row.source_bucket}/{row.source_path}{today}", today)
        elif freq and freq.lower() == 'daily_with_datefolder':
            return (f"s3://{row.source_bucket}/{row.source_path}", None)
        else:
            return (f"s3://{row.source_bucket}/{row.source_path}", None)
        
    def get_latest_file(source_path, file_pattern):

        regex_pattern = "^" + file_pattern.replace("*", ".*") + "$"

        files = dbutils.fs.ls(source_path)

        matched_files = [
            f for f in files
            if re.match(regex_pattern, f.name)
        ]

        if not matched_files:
            raise Exception(f"No matching files found in {source_path}")

        latest_file = max(matched_files, key=lambda x: x.modificationTime)
        
        s3_landing_date = datetime.fromtimestamp(
            latest_file.modificationTime / 1000
        ).strftime("%Y%m%d")

        return latest_file.name, s3_landing_date

    total_partitions = int(spark.conf.get("spark.sql.shuffle.partitions", "200"))
    logger.info(f"Total Partitions: {total_partitions}")
    
    calculated_threads = max(2, int(total_partitions / 50))
    logger.info(f"Calculated Threads: {calculated_threads}")
    
    MAX_CONCURRENT = min(calculated_threads, 4)
    
    logger.info(f"Dynamically set max concurrent workers to: {MAX_CONCURRENT}")
    
    configs_list = configs.collect()

    
    def process_dataset(row, config, simulation_type):

        runtime_id = str(uuid.uuid4())
        start_time = datetime.now()

        try:
            logger.info(f"[{row.dataset_name}] Starting ingestion")

            source_path, s3_landing_date = resolve_source_path(row)
            logger.info(f"Source Path: {source_path}")
            logger.info(f"Simulation Type: {simulation_type}")
            schema_struct = get_struct_schema(row.dataset_name, config)
            
            dataset_tags = get_dataset_tags(row.dataset_name, config)

            file_extension = row.file_format
            file_ext_lower = file_extension.lower()
            file_pattern = f"*{row.file_name}*.{file_extension}"
            
            if row.frequency.lower() in ('daily_with_datefolder'):
                file_pattern, s3_landing_date = get_latest_file(source_path, file_pattern)
                
            if "-" in s3_landing_date:
                s3_landing_date = datetime.strptime(s3_landing_date, "%Y-%m-%d").strftime("%Y-%m-%d")
            else:
                s3_landing_date = datetime.strptime(s3_landing_date, "%Y%m%d").strftime("%Y-%m-%d")

            query = None
            records_ingested = None
            files_processed = None

            # ==============================
            # FORMAT HANDLING
            # ==============================
            
            if file_ext_lower in ['csv', 'txt']:
                logger.info(f"[{row.dataset_name}] Detected CSV/TXT format: {file_extension}")

                load_type = getattr(row, 'load_type', 'full')
                write_mode = "overwrite" if (load_type or "").strip().lower() in ("full", "full_load") else "append"

                table_name = row.dataset_name.replace(" ", "_").lower()
                full_table_name = f"`{CATALOG}`.{BRONZE_SCHEMA}.{table_name}"
                
                _INVALID = frozenset(" ,;{}\r\n\t=.?`()")

                def clean_column(col_name):
                    leading = col_name.startswith("_")

                    col_name = "".join("_" if c in _INVALID else c for c in col_name)
                    col_name = re.sub(r"_+", "_", col_name)
                    col_name = col_name.strip("_").lower()

                    if leading and col_name:
                        col_name = "_" + col_name

                    return col_name if col_name else "col"

                def extract_details(name: str):
                    parts = name.split("_")

                    vendor = parts[0].upper()
                    study_id = "_".join(parts[1:3]).replace("_", "-").upper()
                    entity_name = "_".join(parts[3:])

                    return vendor, study_id, entity_name

                logger.info(f"[{row.dataset_name}] Load type: {load_type}, Write mode: {write_mode}")

                # ==============================
                # ==============================
                _vendor, _study_id, _entity = extract_details(table_name)
                if write_mode == "overwrite":

                    logger.info(f"[{row.dataset_name}] Using batch processing for full load")

                    if schema_struct:
                        logger.info(f"[{row.dataset_name}] === SCHEMA FROM CONFIG ===")
                        for field in schema_struct.fields:
                            logger.info(f"[{row.dataset_name}]   {field.name}: {field.dataType}")
                        logger.info(f"[{row.dataset_name}] ==========================")

                        df = (
                            spark.read
                                .format("csv")
                                .option("header", "false")
                                .option("delimiter", row.delimiter)
                                .option("quote", "\"")
                                .option("multiline", "true")
                                .option("escape", "\"")
                                .schema(schema_struct)
                                .load(f"{source_path}/{file_pattern}")
                        )
                    else:
                        df = (
                            spark.read
                                .format(row.file_format)
                                .option("header", "true")
                                .option("delimiter", row.delimiter)
                                .option("quote", "\"")
                                .option("multiline", "true")
                                .option("escape", "\"")
                                .option("inferSchema", "true")
                                .load(f"{source_path}/{file_pattern}")
                        )
                    df = df.toDF(*[clean_column(c) for c in df.columns])
                    df = df.withColumn("_ingestion_timestamp", F.current_timestamp())
                    df = df.withColumn("simulation_type", F.lit(simulation_type))
                    df = df.withColumn("_arrival_time", F.to_date(F.lit(s3_landing_date)))
                    df = df.withColumn("_study_id", F.lit(_study_id))
                    df = df.withColumn("_vendor", F.lit(_vendor))
                    df = df.withColumn("_entity", F.lit(_entity))
                    df = df.withColumn("_source_file", F.lit(f"{source_path}/{file_pattern}"))


                    check_and_update_schema(
                        spark=spark,
                        dataset_id=row.dataset_id,
                        df=df,
                        logger=logger
                    )

                    records_ingested = df.count()

                    df.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .option("overwriteSchema", "true") \
                        .saveAsTable(full_table_name)

                    logger.info(f"[{row.dataset_name}] Written {records_ingested} records")

                    files_processed = 1  # still coarse-grained
                    query = None

                # ==============================
                # INCREMENTAL LOAD (STREAMING)
                # ==============================
                else:

                    logger.info(f"[{row.dataset_name}] Using streaming for incremental load")

                    # thread-safe metrics container
                    metrics = {"records": 0, "files": 0}

                    if schema_struct:
                        logger.info(f"[{row.dataset_name}] === SCHEMA FROM CONFIG ===")
                        for field in schema_struct.fields:
                            logger.info(f"[{row.dataset_name}]   {field.name}: {field.dataType}")
                        logger.info(f"[{row.dataset_name}] ==========================")

                        df = (
                            spark.readStream
                                .format("cloudFiles")
                                .option("cloudFiles.format", "csv")
                                .option("pathGlobFilter", file_pattern)
                                .option("cloudFiles.includeExistingFiles", "true")
                                .option("header", "false")
                                .option("delimiter", row.delimiter)
                                .option("quote", "\"")
                                .option("multiline", "true")
                                .option("escape", "\"")
                                .schema(schema_struct)
                                .load(source_path)
                        )
                    else:
                        df = (
                            spark.readStream
                                .format("cloudFiles")
                                .option("cloudFiles.format", row.file_format)
                                .option("pathGlobFilter", file_pattern)
                                .option("cloudFiles.includeExistingFiles", "true")
                                .option("cloudFiles.schemaLocation", row.schema_location)
                                .option("header", "true")
                                .option("delimiter", row.delimiter)
                                .option("quote", "\"")
                                .option("multiline", "true")
                                .option("escape", "\"")
                                .load(source_path)
                        )

                    df = df.toDF(*[clean_column(c) for c in df.columns])
                    df = df.withColumn("_ingestion_timestamp", F.current_timestamp())
                    df = df.withColumn("simulation_type", F.lit(simulation_type))
                    df = df.withColumn("_arrival_time", F.to_date(F.lit(s3_landing_date)))
                    df = df.withColumn("_study_id", F.lit(_study_id))
                    df = df.withColumn("_vendor", F.lit(_vendor))
                    df = df.withColumn("_entity", F.lit(_entity))
                    df = df.withColumn("_source_file", F.lit(f"{source_path}/{file_pattern}"))

                    check_and_update_schema(
                        spark=spark,
                        dataset_id=row.dataset_id,
                        df=df,
                        logger=logger
                    )

                    def write_batch_append(batch_df, batch_id):

                        count = batch_df.count()

                        if count == 0:
                            logger.info(f"[{row.dataset_name}] Batch {batch_id}: No records")
                            return

                        logger.info(f"[{row.dataset_name}] Batch {batch_id}: Processing {count} records")

                        batch_df.write \
                            .format("delta") \
                            .mode("append") \
                            .option("overwriteSchema", "false") \
                            .saveAsTable(full_table_name)

                        # capture metrics safely
                        metrics["records"] += count
                        metrics["files"] += 1  # micro-batch approx

                    query = (
                        df.writeStream
                            .foreachBatch(write_batch_append)
                            .option("checkpointLocation", row.checkpoint_location)
                            .trigger(availableNow=True)
                            .start()
                    )

                    # metrics assigned after awaitTermination
                    records_ingested = None
                    files_processed = None
            
            elif file_ext_lower == "json":
                logger.info(f"[{row.dataset_name}] Detected JSON format")

                load_type = getattr(row, 'load_type', 'full')
                write_mode = "overwrite" if (load_type or "").strip().lower() in ("full", "full_load") else "append"

                table_name = row.dataset_name.replace(" ", "_").lower()
                full_table_name = f"`{CATALOG}`.{BRONZE_SCHEMA}.{table_name}"

                _INVALID = frozenset(" ,;{}\r\n\t=.?`()")

                def clean_column(col_name):
                    leading = col_name.startswith("_")

                    col_name = "".join("_" if c in _INVALID else c for c in col_name)
                    col_name = re.sub(r"_+", "_", col_name)
                    col_name = col_name.strip("_").lower()

                    if leading and col_name:
                        col_name = "_" + col_name

                    return col_name if col_name else "col"
                
                # def clean_column(col_name):
                #     col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
                #     col_name = re.sub(r'_+', '_', col_name)
                #     col_name = col_name.strip('_')
                #     return col_name

                logger.info(f"[{row.dataset_name}] Load type: {load_type}, Write mode: {write_mode}")

                # ==============================
                # FULL LOAD (BATCH)
                # ==============================
                if write_mode == "overwrite":

                    logger.info(f"[{row.dataset_name}] Using batch processing for full load")

                    if schema_struct:
                        logger.info(f"[{row.dataset_name}] === SCHEMA FROM CONFIG ===")
                        for field in schema_struct.fields:
                            logger.info(f"[{row.dataset_name}]   {field.name}: {field.dataType}")
                        logger.info(f"[{row.dataset_name}] ==========================")

                        df = (
                            spark.read
                                .format("json")
                                .option("multiline", "true")
                                .schema(schema_struct)
                                .load(f"{source_path}/{file_pattern}")
                        )
                    else:
                        df = (
                            spark.read
                                .format("json")
                                .option("multiline", "true")
                                .option("inferSchema", "true")
                                .load(source_path)
                        )
                    # JAMF handling
                    if row.file_name.lower().startswith("jam"):  
                        logger.info(f"[{row.dataset_name}] Flattening 'aa' column")

                        if "aa" in df.columns:
                            df = df.select("aa.*")   

                            # Drop unwanted nested column
                            df = df.drop("extensionAttributes") if "extensionAttributes" in df.columns else df
                        else:
                            logger.warning(f"'aa' column not found")
                            
                    df = df.toDF(*[clean_column(c) for c in df.columns])
                    df = df.withColumn("loaddt", F.current_timestamp())
                    df = df.withColumn("s3_landing_dt", F.to_date(F.lit(s3_landing_date)))

                    check_and_update_schema(
                        spark=spark,
                        dataset_id=row.dataset_id,
                        df=df,
                        logger=logger
                    )

                    records_ingested = df.count()

                    df.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .option("overwriteSchema", "true") \
                        .saveAsTable(full_table_name)

                    logger.info(f"[{row.dataset_name}] Written {records_ingested} records")

                    files_processed = 1
                    query = None

                # ==============================
                # INCREMENTAL LOAD (STREAMING)
                # ==============================
                else:

                    logger.info(f"[{row.dataset_name}] Using streaming for incremental load")

                    metrics = {"records": 0, "files": 0}

                    if schema_struct:
                        logger.info(f"[{row.dataset_name}] === SCHEMA FROM CONFIG ===")
                        for field in schema_struct.fields:
                            logger.info(f"[{row.dataset_name}]   {field.name}: {field.dataType}")
                        logger.info(f"[{row.dataset_name}] ==========================")

                        df = (
                            spark.readStream
                                .format("cloudFiles")
                                .option("cloudFiles.format", "json")
                                .option("pathGlobFilter", file_pattern)
                                .option("cloudFiles.includeExistingFiles", "true")
                                .option("multiline", "true")
                                .schema(schema_struct)
                                .load(source_path)
                        )
                    else:
                        df = (
                            spark.readStream
                                .format("cloudFiles")
                                .option("cloudFiles.format", "json")
                                .option("pathGlobFilter", file_pattern)
                                .option("cloudFiles.includeExistingFiles", "true")
                                .option("cloudFiles.schemaLocation", row.schema_location)
                                .option("multiline", "true")
                                .load(source_path)
                        )

                    df = df.toDF(*[clean_column(c) for c in df.columns])
                    df = df.withColumn("loaddt", F.current_timestamp())
                    df = df.withColumn("s3_landing_dt", F.to_date(F.lit(s3_landing_date)))

                    check_and_update_schema(
                        spark=spark,
                        dataset_id=row.dataset_id,
                        df=df,
                        logger=logger
                    )

                    def write_batch_append(batch_df, batch_id):

                        count = batch_df.count()

                        if count == 0:
                            logger.info(f"[{row.dataset_name}] Batch {batch_id}: No records")
                            return

                        logger.info(f"[{row.dataset_name}] Batch {batch_id}: Processing {count} records")

                        batch_df.write \
                            .format("delta") \
                            .mode("append") \
                            .option("overwriteSchema", "false") \
                            .saveAsTable(full_table_name)

                        metrics["records"] += count
                        metrics["files"] += 1

                    query = (
                        df.writeStream
                            .foreachBatch(write_batch_append)
                            .option("checkpointLocation", row.checkpoint_location)
                            .trigger(availableNow=True)
                            .start()
                    )

                    records_ingested = None
                    files_processed = None
            
            elif file_ext_lower == "sas7bdat":
                logger.info(f"[{row.dataset_name}] Detected SAS7BDAT format")

                load_type = getattr(row, 'load_type', 'full')
                write_mode = "overwrite" if (load_type or "").strip().lower() in ("full", "full_load") else "append"

                table_name = row.dataset_name.replace(" ", "_").lower()
                full_table_name = f"`{CATALOG}`.{BRONZE_SCHEMA}.{table_name}"
                
                _INVALID = frozenset(" ,;{}\r\n\t=.?`()")

                def clean_column(col_name):
                    leading = col_name.startswith("_")

                    col_name = "".join("_" if c in _INVALID else c for c in col_name)
                    col_name = re.sub(r"_+", "_", col_name)
                    col_name = col_name.strip("_").lower()

                    if leading and col_name:
                        col_name = "_" + col_name

                    return col_name if col_name else "col"

                # def clean_column(col_name):
                #     col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
                #     col_name = re.sub(r'_+', '_', col_name)
                #     col_name = col_name.strip('_')
                #     return col_name

                logger.info(f"[{row.dataset_name}] Load type: {load_type}, Write mode: {write_mode}")

                # Resolve file encoding — default to latin-1 (common for SAS exports)
                sas_encoding = (getattr(row, 'file_encoding', None) or 'latin-1').replace('-BOM', '')

                # ── Read via pandas.read_sas (pure-Python, accepts BytesIO) ──
                # pandas.read_sas handles SAS charset codes that pyreadstat/libreadstat
                # rejects before the Python-level encoding override can fire.
                binary_rows = (
                    spark.read
                        .format("binaryFile")
                        .option("pathGlobFilter", file_pattern)
                        .load(source_path)
                        .collect()
                )

                if not binary_rows:
                    logger.warning(f"[{row.dataset_name}] No SAS7BDAT files found matching pattern {file_pattern} in {source_path}")
                    records_ingested = 0
                    files_processed = 0
                    query = None
                else:
                    import io
                    per_file_dfs = []
                    for bin_row in binary_rows:
                        content = bytes(bin_row.content)
                        pdf = pd.read_sas(io.BytesIO(content), format='sas7bdat', encoding=sas_encoding)
                        pdf.columns = [str(c) for c in pdf.columns]
                        sdf = spark.createDataFrame(
                            pdf.astype(str).where(pdf.notna(), other=None)
                        )
                        per_file_dfs.append(sdf)

                    from functools import reduce as _reduce
                    df = _reduce(
                        lambda a, b: a.unionByName(b, allowMissingColumns=True),
                        per_file_dfs,
                    )

                    df = df.toDF(*[clean_column(c) for c in df.columns])
                    df = df.withColumn("loaddt", F.current_timestamp())
                    df = df.withColumn("simulation_type", F.lit(simulation_type))
                    df = df.withColumn("s3_landing_dt", F.to_date(F.lit(s3_landing_date)))

                    check_and_update_schema(
                        spark=spark,
                        dataset_id=row.dataset_id,
                        df=df,
                        logger=logger
                    )

                    records_ingested = df.count()

                    df.write \
                        .format("delta") \
                        .mode(write_mode) \
                        .option("overwriteSchema", str(write_mode == "overwrite").lower()) \
                        .option("mergeSchema", "true") \
                        .saveAsTable(full_table_name)

                    logger.info(f"[{row.dataset_name}] Written {records_ingested} records from {len(per_file_dfs)} SAS file(s)")

                    files_processed = len(per_file_dfs)
                    query = None

            elif file_ext_lower in ('xlsx', 'xls'):
                logger.info(f"[{row.dataset_name}] Detected Excel format: {file_extension}")

                load_type = getattr(row, 'load_type', 'full')
                write_mode = "overwrite" if (load_type or "").strip().lower() in ("full", "full_load") else "append"

                table_name = row.dataset_name.replace(" ", "_").lower()
                full_table_name = f"`{CATALOG}`.{BRONZE_SCHEMA}.{table_name}"

                _INVALID = frozenset(" ,;{}\r\n\t=.?`()")

                def clean_column(col_name):
                    leading = col_name.startswith("_")

                    col_name = "".join("_" if c in _INVALID else c for c in col_name)
                    col_name = re.sub(r"_+", "_", col_name)
                    col_name = col_name.strip("_").lower()

                    if leading and col_name:
                        col_name = "_" + col_name

                    return col_name if col_name else "col"
                
                # def clean_column(col_name):
                #     col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
                #     col_name = re.sub(r'_+', '_', col_name)
                #     col_name = col_name.strip('_')
                #     return col_name

                logger.info(f"[{row.dataset_name}] Load type: {load_type}, Write mode: {write_mode}")

                # ── Read via binaryFile → BytesIO → pandas.read_excel ─────────
                # No JVM Excel / crealytics package required.
                # binaryFile honours the cluster IAM profile — no boto3 needed.
                binary_rows = (
                    spark.read
                        .format("binaryFile")
                        .option("pathGlobFilter", file_pattern)
                        .load(source_path)
                        .collect()
                )

                if not binary_rows:
                    logger.warning(f"[{row.dataset_name}] No Excel files found matching pattern {file_pattern} in {source_path}")
                    records_ingested = 0
                    files_processed = 0
                    query = None
                else:
                    import io
                    engine = 'openpyxl' if file_ext_lower == 'xlsx' else 'xlrd'
                    per_file_dfs = []
                    for bin_row in binary_rows:
                        content = bytes(bin_row.content)
                        pdf = pd.read_excel(io.BytesIO(content), header=0, engine=engine, dtype=str)
                        pdf.columns = [str(c) for c in pdf.columns]
                        sdf = spark.createDataFrame(
                            pdf.astype(str).where(pdf.notna(), other=None)
                        )
                        per_file_dfs.append(sdf)

                    from functools import reduce as _reduce
                    df = _reduce(
                        lambda a, b: a.unionByName(b, allowMissingColumns=True),
                        per_file_dfs,
                    )

                    df = df.toDF(*[clean_column(c) for c in df.columns])
                    df = df.withColumn("loaddt", F.current_timestamp())
                    df = df.withColumn("simulation_type", F.lit(simulation_type))
                    df = df.withColumn("s3_landing_dt", F.to_date(F.lit(s3_landing_date)))

                    check_and_update_schema(
                        spark=spark,
                        dataset_id=row.dataset_id,
                        df=df,
                        logger=logger
                    )

                    records_ingested = df.count()

                    df.write \
                        .format("delta") \
                        .mode(write_mode) \
                        .option("overwriteSchema", str(write_mode == "overwrite").lower()) \
                        .option("mergeSchema", "true") \
                        .saveAsTable(full_table_name)

                    logger.info(f"[{row.dataset_name}] Written {records_ingested} records from {len(per_file_dfs)} Excel file(s)")

                    files_processed = len(per_file_dfs)
                    query = None

            else:
                raise ValueError(f"Unsupported file format: {file_extension}")

            # ==============================
            # STREAMING / BATCH HANDLING
            # ==============================

            if query:
                logger.info(f"[{row.dataset_name}] Awaiting streaming completion")

                query.awaitTermination()

                end_time = datetime.now()

                # ---------------------------------
                # Priority 1: metrics from foreachBatch
                # ---------------------------------
                if records_ingested is None and 'metrics' in locals():
                    records_ingested = metrics.get("records", 0)
                    files_processed = metrics.get("files", 0)

                # ---------------------------------
                # Priority 2: fallback to Spark progress
                # ---------------------------------
                if records_ingested is None:
                    progress = query.lastProgress

                    if progress:
                        records_ingested = progress.get("numInputRows", 0)
                        files_processed = progress.get("sources", [{}])[0].get("numFilesProcessed", 0)

                # ---------------------------------
                # Final fallback (avoid NULL confusion)
                # ---------------------------------
                if records_ingested is None:
                    records_ingested = 0
                if files_processed is None:
                    files_processed = 0

            else:
                # Batch case (non-streaming)
                end_time = datetime.now()

                # ensure defaults (batch should already set these)
                if records_ingested is None:
                    records_ingested = 0
                if files_processed is None:
                    files_processed = 0
                    
            # Applying Dataset Tags to the final bronze table
            if dataset_tags: 
                apply_table_tags(
                    spark=spark,
                    full_table_name=full_table_name,
                    dataset_name=row.dataset_name,
                    tags=dataset_tags
                )
            
            # ==============================
            # SUCCESS LOGGING
            # ==============================

            records_sql = str(records_ingested)
            files_sql = str(files_processed)

            spark.sql(f"""
                INSERT INTO `{METADATA_CATALOG}`.{METADATA_SCHEMA}.ingestion_runtime_state
                VALUES (
                    '{runtime_id}',
                    '{row.dataset_id}',
                    '{row.environment}',
                    'success',
                    {records_sql},
                    {files_sql},
                    timestamp('{start_time}'),
                    timestamp('{end_time}'),
                    NULL
                )
            """)

            logger.info(
                f"[{row.dataset_name}] Ingestion completed | "
                f"Records: {records_ingested}, Files: {files_processed}"
            )

            return {
                "status": "SUCCESS",
                "dataset": row.dataset_name,
                "runtime_id": runtime_id,
                "records": records_ingested,
                "files": files_processed
            }

        # ==============================
        # ERROR HANDLING
        # ==============================

        except Exception as e:

            end_time = datetime.now()
            error_message = str(e).replace("'", "''")

            logger.error(f"[{row.dataset_name}] Ingestion failed: {str(e)}")

            spark.sql(f"""
                INSERT INTO `{METADATA_CATALOG}`.{METADATA_SCHEMA}.ingestion_runtime_state
                VALUES (
                    '{runtime_id}',
                    '{row.dataset_id}',
                    '{row.environment}',
                    'failed',
                    0,
                    0,
                    timestamp('{start_time}'),
                    timestamp('{end_time}'),
                    '{error_message}'
                )
            """)

            return {
                "status": "FAILED",
                "dataset": row.dataset_name,
                "runtime_id": runtime_id,
                "error": error_message
            }

    config = load_config()
    
    logger.info(f"Starting processing with max concurrency = {MAX_CONCURRENT}")
 
    results = []
    seen_datasets = set()
    
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
    
        futures = {}
    
        # Submit all tasks
        for row in configs_list:
    
            if row.dataset_id in seen_datasets:
                continue
            seen_datasets.add(row.dataset_id)
    
            future = executor.submit(process_dataset, row, config, simulation_type)
            futures[future] = row.dataset_name  # for logging context
    
        # Process results as they complete
        for future in as_completed(futures):
            dataset_name = futures[future]
    
            try:
                result = future.result()
                results.append(result)
    
                if result["status"] == "SUCCESS":
                    logger.info(f"Completed: {result['dataset']}")
                else:
                    logger.error(f"Failed: {result['dataset']} - {result.get('error')}")
    
            except Exception as e:
                logger.error(f"{dataset_name} failed with exception: {str(e)}")
                results.append({
                    "dataset": dataset_name,
                    "status": "FAILED",
                    "error": str(e)
                })
    

    # ==============================
    # BATCH SUMMARY
    # ==============================

    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    failure_count = sum(1 for r in results if r["status"] == "FAILED")
    
    logger.info(f"Final Summary: {success_count} success, {failure_count} failed")
    
    if failure_count > 0:
        raise Exception(f"Pipeline failed with {failure_count} errors")
                

if __name__ == "__main__":
    main()
