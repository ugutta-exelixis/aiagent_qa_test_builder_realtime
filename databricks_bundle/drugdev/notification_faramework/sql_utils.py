# src/utils/sql_utils.py


import re
import logging
from typing import Any
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

logger = logging.getLogger(__name__)

# Characters that are always safe in SQL identifiers (table/schema/catalog names)
_IDENT_SAFE = re.compile(r'^[A-Za-z0-9_.`\- ]+$')

# Maximum length for any string inserted into a monitoring table
_MAX_STR_LEN = 2000


def sql_str(value: Any, max_len: int = 500) -> str:
    """
    Escape a value for safe interpolation into a Spark SQL string literal.
    Escapes single quotes by doubling them (standard SQL escaping).
    Truncates to max_len to prevent oversized payloads.

    Usage:
        spark.sql(f"SELECT * FROM {tbl} WHERE vendor = '{sql_str(vendor)}'")

    Never use bare f-string interpolation for user-controlled or external strings.
    """
    if value is None:
        return ''
    s = str(value)
    if len(s) > max_len:
        s = s[:max_len]
    return s.replace("'", "''")   # standard SQL single-quote escaping


def sql_identifier(name: str, context: str = '') -> str:
    """
    Validate that a name is safe to use as a SQL identifier (table, schema, catalog).
    Raises ValueError if the name contains characters that could indicate injection.

    Usage:
        tbl = sql_identifier(f"{catalog}.{schema}.{table_name}")
        spark.sql(f"DESCRIBE TABLE {tbl}")
    """
    if not name:
        raise ValueError(f"SQL identifier is empty{f' ({context})' if context else ''}")
    if not _IDENT_SAFE.match(name):
        raise ValueError(
            f"SQL identifier {name!r} contains invalid characters"
            f"{f' ({context})' if context else ''}. "
            f"Only alphanumerics, dots, underscores, hyphens, spaces, and backticks allowed."
        )
    return name


def df_insert(spark, table_fqn: str, row: dict) -> None:
    """
    Insert a single row into a Delta table using DataFrame write (append).
    Completely avoids SQL string interpolation — all values go through
    Spark's type system, not SQL string building.

    Type alignment strategy:
      1. Read the target table's existing schema from Delta
      2. Build a StructType using the table's actual column types
      3. Cast Python values to match exactly — no type mismatches possible

    This handles:
      - None values (CANNOT_DETERMINE_TYPE with schema-less createDataFrame)
      - INT vs BIGINT conflicts (Python int → LongType ≠ Delta INT column)
      - Any other type mismatch that would cause DELTA_FAILED_TO_MERGE_FIELDS

    Usage:
        df_insert(spark, TRACKING(), {
            "tracking_id": tid,
            "domain": domain,
            "file_status": "RECEIVED",
            "ingested_at": None,   # ← safe — typed from table schema
        })
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType,
        DoubleType, FloatType, BooleanType, TimestampType, DateType,
    )
    from pyspark.sql import Row, functions as F
    from datetime import datetime, date, timezone

    sql_identifier(table_fqn, context='df_insert target')

    # ── Step 1: get target table schema ──────────────────────────────────────
    # Read the actual Delta column types so we match exactly.
    # Falls back to Python-inferred types if the table doesn't exist yet
    # (first-ever create via _ensure_bronze_table).
    try:
        table_schema = spark.read.table(table_fqn).schema
        col_types    = {f.name: f.dataType for f in table_schema.fields}
    except Exception:
        col_types = {}   # table not yet created — use inferred types below

    # ── Step 2: Python fallback type inference (for new tables / new cols) ───
    def _py_type(value):
        if value is None:              return StringType()
        if isinstance(value, bool):    return BooleanType()
        if isinstance(value, int):     return IntegerType()   # INT not BIGINT
        if isinstance(value, float):   return DoubleType()
        if isinstance(value, datetime):return TimestampType()
        if isinstance(value, date):    return DateType()
        return StringType()

    # ── Step 3: build StructType using table types where known ───────────────
    fields = []
    for col, val in row.items():
        dtype = col_types.get(col, _py_type(val))
        fields.append(StructField(col, dtype, nullable=True))

    schema = StructType(fields)

    # ── Step 4: coerce Python values to match the target column type ─────────
    # Handles cases where callers pass strings for DATE/TIMESTAMP columns
    # (e.g. date.today().isoformat() → "2026-03-19" for a DATE column) and
    # ensures timezone-aware datetimes are made naive for TimestampType.
    coerced = {}
    for col, val in row.items():
        if val is not None:
            dtype_name = type(col_types.get(col, StringType())).__name__.lower()
            if isinstance(val, str) and 'date' in dtype_name and 'timestamp' not in dtype_name:
                try:
                    val = date.fromisoformat(val)
                except (ValueError, AttributeError):
                    pass
            elif isinstance(val, str) and 'timestamp' in dtype_name:
                try:
                    val = datetime.fromisoformat(val)
                except (ValueError, AttributeError):
                    pass
            elif isinstance(val, datetime) and val.tzinfo is not None:
                val = val.replace(tzinfo=None)
        coerced[col] = val

    df = spark.createDataFrame([Row(**coerced)], schema=schema)

    # mergeSchema=false: we matched the table schema exactly above.
    # If a genuinely new column appears (ADD-policy drift), Bronze write
    # uses mergeSchema=true separately in _ingest_entity.
    (df.write
       .format('delta')
       .mode('append')
       .option('mergeSchema', 'false')
       .saveAsTable(table_fqn))


def df_update(spark, table_fqn: str, set_values: dict, where_id_col: str, where_id_val: str,
              max_retries: int = 3) -> None:
    """
    Update a single row by primary key using a DataFrame-based MERGE.
    Avoids f-string interpolation for the set values.

    Thread-safe: temp view name is suffixed with a short UUID so concurrent
    calls from the thread pool (parallel study ingestion) cannot race on the
    same view name and corrupt each other's MERGE statements.

    Retries up to max_retries times on ConcurrentAppendException — Delta
    MERGE on partitioned tables can fail transiently when two threads
    hit the same partition simultaneously.

    NOTE: For vendor_file_tracking updates, use the direct UPDATE methods
    in S3FileTracker (update_ingested / update_quarantine) instead of
    df_update. Direct UPDATE avoids MERGE concurrency conflicts entirely.

    Usage:
        df_update(spark, REGISTRY(),
            set_values={"status": "DEPRECATED", "end_date": date.today()},
            where_id_col="schema_id",
            where_id_val=schema_id)
    """
    import uuid as _uuid
    from pyspark.sql import Row
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType,
        DoubleType, BooleanType, TimestampType, DateType,
    )
    from datetime import datetime, date

    sql_identifier(table_fqn, context='df_update target')

    # Read table schema for exact type alignment (same approach as df_insert)
    try:
        table_schema = spark.read.table(table_fqn).schema
        col_types    = {f.name: f.dataType for f in table_schema.fields}
    except Exception:
        col_types = {}

    def _py_type(value):
        if value is None:              return StringType()
        if isinstance(value, bool):    return BooleanType()
        if isinstance(value, int):     return IntegerType()
        if isinstance(value, float):   return DoubleType()
        if isinstance(value, datetime):return TimestampType()
        if isinstance(value, date):    return DateType()
        return StringType()

    # Build the full row (id col + set_values) with table-aligned types
    full_row = {where_id_col: where_id_val, **set_values}
    fields   = [
        StructField(col, col_types.get(col, _py_type(val)), nullable=True)
        for col, val in full_row.items()
    ]
    schema = StructType(fields)

    # Coerce values to match target column types
    coerced = {}
    for col, val in full_row.items():
        if val is not None:
            dtype_name = type(col_types.get(col, StringType())).__name__.lower()
            if isinstance(val, str) and 'date' in dtype_name and 'timestamp' not in dtype_name:
                try:
                    val = date.fromisoformat(val)
                except (ValueError, AttributeError):
                    pass
            elif isinstance(val, str) and 'timestamp' in dtype_name:
                try:
                    val = datetime.fromisoformat(val)
                except (ValueError, AttributeError):
                    pass
            elif isinstance(val, datetime) and val.tzinfo is not None:
                val = val.replace(tzinfo=None)
        coerced[col] = val

    # Thread-safe temp view name: short UUID suffix prevents races in
    # parallel thread pools where two threads call df_update simultaneously
    view_name = f"_ddda_upd_{_uuid.uuid4().hex[:12]}"

    set_cols = ', '.join(
        f"t.{sql_identifier(k, 'set column')} = s.{sql_identifier(k, 'set column')}"
        for k in set_values
    )

    df = spark.createDataFrame([Row(**coerced)], schema=schema)
    df.createOrReplaceTempView(view_name)

    import time as _time
    last_exc = None
    for attempt in range(max(1, max_retries)):
        try:
            spark.sql(f"""
                MERGE INTO {table_fqn} t
                USING {view_name} s
                ON t.{sql_identifier(where_id_col)} = s.{sql_identifier(where_id_col)}
                WHEN MATCHED THEN UPDATE SET {set_cols}
            """)
            last_exc = None
            break   # success
        except Exception as exc:
            last_exc = exc
            if 'ConcurrentAppend' in str(exc) or 'ConcurrentDelete' in str(exc):
                if attempt < max_retries - 1:
                    _time.sleep(0.5 * (attempt + 1))   # 0.5s, 1.0s backoff
                    continue
            raise   # non-retryable error — propagate immediately
        finally:
            if attempt == max_retries - 1:
                try:
                    spark.catalog.dropTempView(view_name)
                except Exception:
                    pass

    if last_exc:
        try:
            spark.catalog.dropTempView(view_name)
        except Exception:
            pass
        raise last_exc

    try:
        spark.catalog.dropTempView(view_name)
    except Exception:
        pass


def clean_where_clause(domain: str = None, vendor: str = None,
                        study_id: str = None, entity: str = None,
                        extra: dict = None) -> str:
    """
    Build a safe WHERE clause for common filter patterns.
    All values are escaped with sql_str().

    Returns a SQL WHERE clause string without the WHERE keyword.
    Caller appends to their query.
    """
    conditions = []
    if domain:
        conditions.append(f"domain = '{sql_str(domain)}'")
    if vendor:
        conditions.append(f"vendor = '{sql_str(vendor)}'")
    if study_id:
        conditions.append(f"study_id = '{sql_str(study_id)}'")
    if entity:
        conditions.append(f"entity = '{sql_str(entity)}'")
    if extra:
        for col, val in extra.items():
            conditions.append(f"{sql_identifier(col)} = '{sql_str(val)}'")
    return ' AND '.join(conditions) if conditions else '1=1'
