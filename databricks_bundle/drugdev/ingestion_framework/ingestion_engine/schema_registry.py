import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

METADATA_CATALOG = spark.conf.get("drugdev.METADATA_CATALOG")
METADATA_SCHEMA  = spark.conf.get("drugdev.METADATA_SCHEMA")

if not METADATA_CATALOG or not METADATA_SCHEMA:
    raise ValueError("Missing required Spark conf(s): drugdev.METADATA_CATALOG, drugdev.METADATA_SCHEMA")

SCHEMA_REGISTRY_TBL = f"`{METADATA_CATALOG}`.{METADATA_SCHEMA}.schema_registry"

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
 