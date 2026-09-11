# src/transformation/silver_transformer.py
#
# Transforms Bronze → Silver staging.
# Reads transformation_logic from schema_registry.
# Applies CAST / TO_DATE / UPPER SQL expressions.
# Writes to a staging table for DQX validation before SCD2 merge.

import json
import yaml
import logging
import os
import re
import ast
from pyspark.sql import SparkSession, functions as F
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.window import Window
from functools import reduce
from sanitize import sanitize_col as _sanitize_col
spark = SparkSession.builder.getOrCreate()
from datetime import datetime
logger = logging.getLogger(__name__)
from sql_utils import sql_identifier


def _get_conf():
    keys = {
        "drugdev.METADATA_CATALOG",
        "drugdev.METADATA_SCHEMA",
        "drugdev.REGISTRY_SCHEMA",
        "drugdev.CATALOG",
        "drugdev.BRONZE_SCHEMA",
        "drugdev.SILVER_SCHEMA",
        "drugdev.environment",
        "drugdev.CONFIG_PATH",
    }
    conf = {}
    missing = []
    for k in keys:
        v = spark.conf.get(k, "")
        if not v:
            missing.append(k)
        conf[k.split(".")[-1]] = v
    if missing:
        raise ValueError(f"Missing required Spark conf(s): {', '.join(missing)}")
    conf["RUN_DATE"] = spark.conf.get("drugdev.RUN_DATE", datetime.now().strftime("%Y%m%d"))
    return conf


def build_fqn(domain: str, layer: str, table: str = None,
              spark=None) -> str:
    
    conf = _get_conf()
    cat = f"`{conf['CATALOG']}`"
    lyr = layer.lower()

    if lyr == "bronze":
        schema = conf['BRONZE_SCHEMA']
    elif lyr == "silver":
        schema = conf['SILVER_SCHEMA']
    elif lyr == "common":
        schema = conf['METADATA_SCHEMA']
    else:
        raise ValueError(
            f"Layer {layer!r} not recognised. Use: bronze | silver | gold | common"
        )

    fqn = f"{cat}.{schema}"
    return f"{fqn}.{table}" if table else fqn

def build_bronze_table_name(domain: str, vendor: str, study_id: str,
                             entity: str, spark=None) -> str:
    vendor_short = vendor.lower().replace(" ", "").replace("-", "")[:12]
    study_short  = "global" if study_id == "_GLOBAL" else study_id.replace("-", "_").lower()
    return f"{build_fqn(domain, 'bronze', spark=spark)}.{vendor_short}_{study_short}_{entity}"

def build_silver_table_name(domain: str, entity: str,
                             staging: bool = False, spark=None) -> str:
    return build_fqn(domain, "silver",
                     f"{domain.lower()}_{entity}_staging" if staging else entity, spark=spark)




def _split_sql_expressions(blob: str) -> list[str]:
    """
    Split a multi-expression SQL string into individual expressions.

    YAML ``transformation_overrides`` entries written without the ``|`` block
    scalar style (or without list ``-`` prefixes) are parsed by PyYAML as a
    single plain string rather than a list.  The string contains multiple SQL
    expressions separated by commas, e.g.::

        cast(study_id as string) as study_id,
        case when ... end as country,
        md5(...) as hash_key

    A naive ``str.split(',')`` breaks CASE expressions, function calls, and
    nested parentheses that legitimately contain commas.  This function splits
    only on commas that appear at the *top level* — i.e. outside all
    parentheses and outside CASE/WHEN/END blocks — so complex expressions are
    kept intact.

    Returns a list of stripped, non-empty SQL expression strings.
    """
    expressions: list[str] = []
    depth      = 0   # parenthesis depth
    case_depth = 0   # CASE … END nesting depth
    current:  list[str] = []

    # Tokenise by word boundaries so we can detect CASE / END keywords
    # without false-matching them inside identifiers or string literals.
    token_re = re.compile(
        r"'(?:[^'\\]|\\.)*'"    # single-quoted string literal
        r'|"(?:[^"\\]|\\.)*"'   # double-quoted identifier
        r'|`[^`]*`'             # backtick identifier
        r'|\bCASE\b'            # CASE keyword
        r'|\bEND\b'             # END keyword
        r'|\('                  # open paren
        r'|\)'                  # close paren
        r'|,',                  # comma — potential split point
        re.IGNORECASE,
    )

    pos = 0
    for m in token_re.finditer(blob):
        # Append everything between the last match and this one verbatim
        current.append(blob[pos:m.start()])
        tok = m.group(0)
        pos = m.end()

        if tok.upper() == 'CASE':
            case_depth += 1
            current.append(tok)
        elif tok.upper() == 'END':
            case_depth = max(0, case_depth - 1)
            current.append(tok)
        elif tok == '(':
            depth += 1
            current.append(tok)
        elif tok == ')':
            depth = max(0, depth - 1)
            current.append(tok)
        elif tok == ',':
            if depth == 0 and case_depth == 0:
                # Top-level comma → expression boundary
                expr = ''.join(current).strip()
                if expr:
                    expressions.append(expr)
                current = []
            else:
                current.append(tok)
        else:
            # Quoted string / backtick identifier — preserve verbatim
            current.append(tok)

    # Remainder after the last match
    current.append(blob[pos:])
    expr = ''.join(current).strip()
    if expr:
        expressions.append(expr)

    return [e for e in expressions if e]



CONFIG_PATH = _get_conf()["CONFIG_PATH"]


class SilverTransformer:
    """
    Reads all ACTIVE Bronze rows for a domain/entity and applies:
      1. Column mapping (bronze → canonical) from mapping_rules
      2. Type casts and date conversions from transformation_logic
      3. Cross entity enrichments from cross_entity_joins
      4. SCD metadata columns
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._transformation_cache = {}
        self.conf = _get_conf()
    @staticmethod
    def _clean_col(name: str):
            return re.sub(r'[\s\n\r\t\u00a0\u200b\ufeff]+', '', name)

    # Parse transformation logic
    def _parse_transformation_logic(self, raw_logic: str):

        if not raw_logic:
            return "", None
        if raw_logic in self._transformation_cache:
            return self._transformation_cache[raw_logic]

        try:
            parsed = ast.literal_eval(raw_logic)
            if isinstance(parsed, dict):
                standard_sql = parsed.get("standard", "")
                post_processing = parsed.get("post_processing")
                if isinstance(post_processing, str):
                    post_processing = yaml.safe_load(post_processing)
                result = (standard_sql, post_processing)
                self._transformation_cache[raw_logic] = result
                logger.info("Parsed structured transformation_logic.")
                return result
        except Exception:
            logger.info("Using legacy transformation_logic format")
        result = (raw_logic, None)
        self._transformation_cache[raw_logic] = result
        return result

    def _normalize_bronze_input_schema(self, df):
        """
        Normalize Bronze input columns at source so downstream study unions stay
        stable without cross-frame schema reconciliation.

        - Business columns are cast to STRING consistently across studies.
        - VOID columns are projected as typed NULL STRING placeholders.
        - Operational timestamp columns are preserved for freshness filters.
        """
        sys_cols = {"_ingestion_timestamp", "_arrival_time"}
        dtype_map = dict(df.dtypes)
        exprs = []
        void_cols = []

        for col_name in df.columns:
            col_type = dtype_map.get(col_name, "string")
            if col_name in sys_cols:
                if col_type == "void":
                    exprs.append(F.lit(None).cast("timestamp").alias(col_name))
                    void_cols.append(col_name)
                else:
                    exprs.append(F.col(f"`{col_name}`").alias(col_name))
                continue

            if col_type == "void":
                exprs.append(F.lit(None).cast("string").alias(col_name))
                void_cols.append(col_name)
            else:
                exprs.append(F.col(f"`{col_name}`").cast("string").alias(col_name))

        if void_cols:
            logger.warning("Normalized VOID columns at Bronze source for %d column(s): %s", len(void_cols), sorted(void_cols))

        return df.select(*exprs)
    
    # MAIN TRANSFORM
    def transform(self, domain: str, vendor: str, entity: str, study_id: str = None, allowed_datasets: set | None = None):
        rows = self._get_registry_rows(domain, entity, study_id)
        if allowed_datasets is not None:
            rows = [r for r in rows if (r.get("source_dataset_name") or "") in allowed_datasets]
        if not rows:
            raise ValueError(
                f"No ACTIVE schema_registry rows for "
                f"{domain}/{entity}")

        staging_tbl = build_silver_table_name(
            domain,
            entity,
            staging=True
        )
        self.spark.sql(
            f"DROP TABLE IF EXISTS "
            f"{sql_identifier(staging_tbl, 'staging table')}"
        )
        frames = []
        post_processing_list = []
        dataset_results = []
 
        # Transform each study — parallel across rows (2 threads safe for 4-core cluster)
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix=f"transformer_{entity}") as executor:
            futures = {
                executor.submit(self._transform_one, domain, entity, row): row
                for row in rows
            }
            for future in as_completed(futures):
                try:
                    row_result = future.result()
                    if row_result:
                        dataset_results.append({
                            "source_dataset_name": row_result.get("source_dataset_name", ""),
                            "vendor": row_result.get("vendor", ""),
                            "study_id": row_result.get("study_id", ""),
                            "bronze_table": row_result.get("bronze_table", ""),
                            "status": row_result.get("status", "FAILED"),
                            "reason": row_result.get("reason", ""),
                            "row_count": int(row_result.get("row_count", 0) or 0),
                        })
                    df = row_result.get("df") if row_result else None
                    post_processing = row_result.get("post_processing") if row_result else None
                    if df is not None:
                        frames.append(df)
                        if post_processing:
                            post_processing_list.append(post_processing)
                except Exception as exc:
                    logger.error("_transform_one failed for %s/%s: %s", domain, entity, exc)
                    raise
        if not frames:
            logger.warning("No Bronze data found for %s/%s", domain, entity)
            return None, [], [], dataset_results
        
        # Union all studies. Source-level normalization keeps schemas stable.
        combined = reduce(
            lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

        # Ensure ingestion timestamp exists in all silver outputs.
        if "_ingestion_timestamp" not in combined.columns:
            combined = combined.withColumn("_ingestion_timestamp", F.current_timestamp())

        # Add SCD columns
        for col_name, default in [
            ("_scd_start_date", "CURRENT_TIMESTAMP()"),
            ("_scd_end_date", "CAST(NULL AS TIMESTAMP)"),
            ("_scd_is_current", "TRUE"),
        ]:
            if col_name not in combined.columns:
                combined = combined.withColumn(col_name, F.expr(default))

        # Write staging
        (
            combined.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(staging_tbl)
        )

        # APPLY CROSS ENTITY ENRICHMENTS
        self._apply_cross_entity_joins(domain=domain, entity=entity, staging_tbl=staging_tbl, rows=rows)

        # Enforce canonical schema after cross-entity enrichments.
        canonical_cols = next(
            (r["_canonical_cols"] for r in rows if r.get("_canonical_cols")),
            None,
        )
        if canonical_cols:
            post_join_df = self.spark.read.table(staging_tbl)
            post_join_df = self._enforce_canonical_columns(post_join_df, canonical_cols)
            (
                post_join_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(staging_tbl)
            )

        logger.info("Silver staging written: %s ", staging_tbl)

        metadata_list = [
            {
                "vendor": row["vendor"],
                "study_id": row["study_id"],
                "domain": domain,
                "entity": entity,
                "source_dataset_name": row.get("source_dataset_name", ""),
            }
            for row in rows
        ]
        return staging_tbl, metadata_list, post_processing_list, dataset_results

    # GET REGISTRY ROWS
    def _get_registry_rows(self, domain: str, entity: str, study_id: str = None):
        from sql_utils import sql_str
        registry = f"`{self.conf['CATALOG']}`.{self.conf['REGISTRY_SCHEMA']}.drugdev_silver_registry"
        query = f"""
            SELECT DISTINCT
                vendor,
                study_id,
                source_dataset_name,
                mapping_rules,
                transformation_logic,
                cross_entity_sql,
                canonical_cols
            FROM {registry}
            WHERE domain = '{sql_str(domain)}'
              AND entity = '{sql_str(entity)}'
              AND is_active = TRUE
        """
        if study_id:
            query += f" AND study_id = '{sql_str(study_id)}'"
        query += " ORDER BY study_id, vendor"
        rows = self.spark.sql(query).collect()
        processed_rows = []
        for row in rows:
            row_dict = row.asDict()
            row_dict["_mapping_rules"] = (json.loads(row.mapping_rules) if row.mapping_rules else {})
            standard_sql, post_processing = self._parse_transformation_logic(row.transformation_logic)
            row_dict["_standard_sql"] = standard_sql
            row_dict["_post_processing"] = post_processing
            row_dict["_canonical_cols"] = (json.loads(row.canonical_cols) if row.canonical_cols else [])
            if isinstance(row_dict["_canonical_cols"], dict):
                row_dict["canonical_cols"] = list(row_dict["canonical_cols"].keys())
            processed_rows.append(row_dict)
        return processed_rows

    # TRANSFORM SINGLE STUDY
    def _transform_one(self, domain: str, entity: str, row):
        vendor = row["vendor"]
        row_study_id = row["study_id"] or "unknown"
        source_dataset_name = row.get("source_dataset_name") or ""
        bronze = build_bronze_table_name(domain, vendor, row_study_id, entity)
        logger.info("Transforming %s/%s/%s → %s", domain, entity, row_study_id, bronze)
        try:
            self.spark.sql(f"DESCRIBE TABLE "
                f"{sql_identifier(bronze, 'bronze table')}")
        except Exception:
            logger.warning("Bronze table not found: %s — skipping", bronze)
            return {
                "df": None,
                "post_processing": None,
                "source_dataset_name": source_dataset_name,
                "vendor": vendor,
                "study_id": row_study_id,
                "bronze_table": bronze,
                "status": "FAILED",
                "reason": "Bronze table not found",
                "row_count": 0,
            }
        df = self.spark.read.table(bronze)
        df = df.toDF(*[self._clean_col(c) for c in df.columns])
        logger.info("Bronze columns after strip: %s", [repr(c) for c in df.columns])
        # Action 1 — count + max arrival time (single Spark job)
        agg_exprs = [F.count("*").alias("cnt")]
        if "_arrival_time" in df.columns:
            agg_exprs.append(F.max("_arrival_time").alias("max_ts"))
 
        stats = df.agg(*agg_exprs).first()
 
        if stats["cnt"] == 0:
            logger.warning("Bronze table is empty: %s — skipping", bronze)
            return {
                "df": None,
                "post_processing": None,
                "source_dataset_name": source_dataset_name,
                "vendor": vendor,
                "study_id": row_study_id,
                "bronze_table": bronze,
                "status": "FAILED",
                "reason": "Bronze table empty",
                "row_count": 0,
            }
 
        if "_arrival_time" in df.columns:
            max_ts = stats["max_ts"]
            if max_ts:
                df = df.filter(F.col("_arrival_time") == max_ts)
                logger.info("Filtered latest _arrival_time = %s for %s", str(max_ts), bronze)
            else:
                logger.warning("max_ts is null for %s", bronze)
        else:
            logger.warning("No _arrival_time column found in %s", bronze)
 
        # Action 2 — max ingestion WITHIN latest arrival batch only
        if "_ingestion_timestamp" in df.columns:
            max_its = df.agg(F.max("_ingestion_timestamp")).first()[0]
            if max_its:
                df = df.filter(F.col("_ingestion_timestamp") == max_its)
                logger.info("Filtered latest _ingestion_timestamp = %s for %s", str(max_its), bronze)

        # Normalize Bronze input dtypes up-front so multi-study unions do not
        # depend on cross-frame dtype reconciliation.
        df = self._normalize_bronze_input_schema(df)

        # Mapping rules
        mapping_rules = row["_mapping_rules"]

        if not mapping_rules:
            logger.warning("No mapping_rules found for %s/%s/%s", domain, entity, row_study_id)
            return {
                "df": None,
                "post_processing": None,
                "source_dataset_name": source_dataset_name,
                "vendor": vendor,
                "study_id": row_study_id,
                "bronze_table": bronze,
                "status": "FAILED",
                "reason": "No mapping_rules",
                "row_count": 0,
            }

        # logger.info("Columns before applying mapping_rules for %s/%s/%s: %s", domain, entity, row_study_id, df.columns)

        df = self._apply_canonical_mapping(df, mapping_rules)
        # logger.info("Applied canonical mapping for %s/%s/%s, columns after mapping: %s", domain, entity, row_study_id, df.columns)
        df = self._fill_study_identifiers(df, row_study_id )
        # logger.info("Filled study identifiers for %s/%s/%s, columns after fill: %s", domain, entity, row_study_id, df.columns)
        # ensure canonical schema before transformation
        df = self._ensure_canonical_columns_exist(df, row["_canonical_cols"])

        # logger.info("Renamed columns for %s/%s/%s: %s", domain, entity, row_study_id, df.columns)
        
        # Transformation logic
        standard_sql = row["_standard_sql"]
        post_processing = row["_post_processing"]

        if standard_sql.strip():
            df = self._apply_transformation(df, standard_sql)

        logger.debug("Transformed %s/%s/%s cols=%d", domain, entity, row_study_id, len(df.columns))
        return {
            "df": df,
            "post_processing": post_processing,
            "source_dataset_name": source_dataset_name,
            "vendor": vendor,
            "study_id": row_study_id,
            "bronze_table": bronze,
            "status": "SUCCESS",
            "reason": "",
            "row_count": int(stats["cnt"]),
        }

    # Fill study identifiers
    def _fill_study_identifiers( self, df, study_id_value: str):
        """
        Ensure study identifier columns are populated using registry study_id.

        Logic:
          - Check whether ANY of the three identifier columns exist in the DataFrame.
          - If NONE are present → add only `study_id` filled with study_id_value.
          - If at least ONE is present → coalesce NULLs in whichever ones exist;
            columns that are simply absent are left alone (not added).
        """
        _STUDY_ID_COLS = (
            "study_id",
            "study_protocol_number",
            "clinical_source_id",
        )

        present = [c for c in df.columns if c.lower() in _STUDY_ID_COLS]

        if not present:
            logger.info("Adding study_id=%r", study_id_value)
            df = df.withColumn("study_id", F.lit(study_id_value))

        else:
            for c in present:
                df = df.withColumn(c, F.coalesce(F.regexp_replace(F.col(f"`{c}`"),r'[\r\n\t]',''), F.lit(study_id_value)))
        return df
    
    def _ensure_canonical_columns_exist(self, df, canonical_cols):
        logger.debug("Ensuring canonical columns exist: %s", canonical_cols)
        if not canonical_cols:
            logger.warning("No canonical_cols found")
            return df
        existing_cols = set(df.columns)
        missing_cols = [F.lit(None).cast("string").alias(col) for col in canonical_cols if col not in existing_cols]
        if missing_cols:
            logger.debug("Adding missing canonical columns as NULL: %s", missing_cols)
            df = df.select("*", *missing_cols)
        else:
            logger.debug("All canonical columns already present: %s", canonical_cols)
        return df
    # Canonical mapping
    def _apply_canonical_mapping(self, df, mapping_rules: dict ):
        """
        Rename Bronze columns (sanitized vendor names) to canonical Silver column names.
        mapping_rules = {canonical_col: vendor_col}  (from schema_registry)

        Bronze stored vendor cols with sanitized names (spaces->underscores).
        e.g. "Study Site" was written as "Study_Site" in Bronze.
        We apply the same sanitization to vendor_col before looking it up.
        Column sanitization uses the canonical set from utils.sanitize.sanitize_col.
        """
        col_lkp = {self._clean_col(c).lower():self._clean_col(c) for c in df.columns}
        exprs = []
        mapped_source_cols = set()
        for canonical, vendor in mapping_rules.items():
            canonical= self._clean_col(canonical).strip()
            col = _sanitize_col(vendor.strip())
            match = col_lkp.get(col)


            if match:
                exprs.append(f"`{match}` AS `{canonical}`")
                mapped_source_cols.add(match.lower())

            else:
                logger.warning( "Column %r not found → NULL fill",col)
                exprs.append( f"cast(NULL AS string) "f"as `{canonical}`")
        # return df.selectExpr(*exprs)
        
        sys_cols = {
            "_ingestion_timestamp",
            "_arrival_time"}
        # Keep unmapped columns
        extra_cols = [
            f"`{self._clean_col(c)}`"
            for c in df.columns
            if self._clean_col(c).lower() not in mapped_source_cols
            and c.lower() not in sys_cols
        ]

        passthrough = [f"`{self._clean_col(c)}`" for c in df.columns if c.lower() in sys_cols]
        return df.selectExpr(*(exprs + extra_cols + passthrough))

    # Apply transformation logic
    def _apply_transformation(self, df, transformation_logic: str):
        """
        Apply transformation SQL expressions to DataFrame using _split_sql_expressions
        so that multi-line CASE expressions are handled correctly.
        """
        # from onboarding.intake_engine import _split_sql_expressions
        raw_exprs = _split_sql_expressions(transformation_logic)
        exprs = []
        for e in raw_exprs:
            e = e.strip()
            if not e or e.startswith("--"):
                continue
            if e.startswith("-"):
                e = e[1:].strip()
            exprs.append(e)
        logger.debug("parsed transformation expressions : %s", exprs)

        alias_set = set()
        for e in exprs:
            m = re.search(r"\bAS\s+(\w+)\s*$", e, re.IGNORECASE)
            if m:
                alias_set.add(m.group(1).lower())

        passthrough = [
            f"`{c}`"
            for c in df.columns
            if c.lower() not in alias_set
        ]

        try:
            return df.selectExpr(*(exprs + passthrough))
        except Exception as exc:
            logger.error("transformation_logic failed: %s", exc)
            return df

    # Canonical column enforcement
    def _enforce_canonical_columns(self, df, canonical_cols: str):
        """
        Ensure ONLY canonical columns exist in final DF.
        Missing → add as NULL STRING.
        Extra → drop.
        """
        if not canonical_cols:
            logger.warning("No canonical_cols found")
            return df
        # canonical = json.loads(canonical_cols_json)
        # if isinstance(canonical, dict):
        #     canonical = list(canonical.keys())

        final = []
        for col in canonical_cols:
            match = next(
                ( c for c in df.columns
                    if c.lower() == col.lower()
                ), None )
            if match:
                final.append(F.col(f"`{match}`").alias(col))
            else:
                logger.warning( "Canonical column %s missing", col)
                final.append(F.lit(None).cast("string").alias(col))

        # Preserve operational/system columns even if not listed in canonical_cols.
        for sys_col in ("_ingestion_timestamp", "_arrival_time"):
            if sys_col in df.columns:
                final.append(F.col(sys_col))

        return df.select(*final)
    def _latest_partition_view(self, table_name):
        # Single-scan ROW_NUMBER window replaces the triple-nested MAX subquery
        # that required 3 full table scans (one per nested SELECT MAX).
        return f"""
        (
            SELECT * EXCEPT (_rn)
            FROM (
                SELECT *,
                       RANK() OVER (
                           ORDER BY _arrival_time DESC, _ingestion_timestamp DESC
                       ) AS _rn
                FROM {table_name}
            )
            WHERE _rn = 1
        )
        """
    # Resolve cross entity table placeholders
    # ADDED
    def _resolve_cross_entity_tables(self, sql_template: str, domain: str, row):
        pattern = r"\{([a-zA-Z0-9_]+):([a-zA-Z0-9_]+)\}"
        matches = re.findall(
            pattern,
            sql_template
        )

        resolved_sql = sql_template
        for layer,table_name in matches:
            layer = layer.lower()
            if layer == 'bronze':
                resolved_table = build_bronze_table_name(
                    domain=domain,
                    vendor=row["vendor"],
                    study_id=row["study_id"],
                    entity = table_name )
                resolved_table = self._latest_partition_view(resolved_table)
            else:
                resolved_table = build_fqn(
                    domain=domain,
                    layer=layer,
                    table=table_name,
                )
            resolved_sql = resolved_sql.replace(
                f"{{{layer}:{table_name}}}",
                resolved_table
            )
        return resolved_sql
    
    # CROSS ENTITY ENRICHMENT
    def _apply_cross_entity_joins(self, domain: str, entity: str, staging_tbl: str, rows ):
        """
        Resolve placeholders in cross_entity_sql and run it, overwriting the
        staging table with the enriched result.

        Supported placeholders:
          {staging_table}   → fully-qualified staging table name
          {silver.<DOMAIN>} → silver schema FQN for that domain
                              e.g. {silver.CTMS} → <catalog>.silver_ctms
        """
        configured_rows = [
            row for row in rows
            # if getattr(row, "cross_entity_sql", None)
            if row.get("cross_entity_sql")
            and str(row["cross_entity_sql"]).strip()
        ]
        if not configured_rows:
            logger.info(
                "No cross_entity_joins configured "
                "for %s/%s", domain, entity)
            return

        base_df = self.spark.read.table(staging_tbl)
        if "study_id" not in base_df.columns:
            logger.warning(
                "study_id not present in %s; skipping cross_entity_joins",
                staging_tbl,
            )
            return

        untouched_df = base_df
        enriched_frames = []

        # Apply each configured SQL only to that study's rows.
        for batch_idx, row in enumerate(configured_rows, start=1):
            scoped_study_id = row["study_id"] or "unknown"
            cross_sql = str(row["cross_entity_sql"]).strip()
            resolved_sql = self._resolve_cross_entity_tables(
                sql_template=cross_sql,
                domain=domain,
                row=row,
            )

            logger.info( "Applying cross_entity_joins "
                "for study_id=%s",
                scoped_study_id,
            )

            scoped_df = base_df.filter(
                F.col("study_id") == F.lit(scoped_study_id)
            )

            # Use limit(1).count() instead of rdd.isEmpty() — .rdd is not
            # available on Spark Connect (Databricks 13+ / serverless clusters).
            if not scoped_df.schema.fields or scoped_df.limit(1).count() == 0:
                logger.warning(
                    "No rows found in %s for study_id=%s; skipping cross_entity SQL",
                    staging_tbl,
                    scoped_study_id,
                )
                continue

            temp_view = f"{entity}_cross_entity_temp_{batch_idx}"
            scoped_df.createOrReplaceTempView(temp_view)
            sql = (resolved_sql.replace("{staging_table}", temp_view))
            logger.info(
                "Cross entity batch=%d temp_view=%s study_id=%s",
                batch_idx,
                temp_view,
                scoped_study_id,
            )
            logger.debug("Cross entity SQL:\n%s", sql)

            # Materialize now so later unions/log previews cannot be rebound
            # to a different temp view from another batch.
            enriched_df = self.spark.sql(sql).cache()

            logger.info(
                "cross_entity completed study_id=%s",
                scoped_study_id,
            )
            enriched_frames.append(enriched_df)
            untouched_df = untouched_df.filter(F.col("study_id") != F.lit(scoped_study_id))

        # Union untouched + enriched
        final_df = untouched_df
        # for enriched_df in enriched_frames:
        #     final_df = final_df.unionByName(enriched_df,allowMissingColumns=True)
        for idx, enriched_df in enumerate(enriched_frames, start=1):
            # enriched_count = enriched_df.count()
            # untouched_count = final_df.count()
            # logger.info(
            #     "Adding enriched rows into unchanged dataframe: batch=%d enriched_rows=%d unchanged_rows_before=%d",
            #     idx,
            #     enriched_count,
            #     untouched_count,
            # )

            # preview_cols = [
            #     c for c in ["study_id", "study_country_id", "country_name", "country_status", "hash_key"]
            #     if c in enriched_df.columns
            # ]
            # preview_df = (
            #     enriched_df.select(*preview_cols)
            #     if preview_cols
            #     else enriched_df
            # )
            # preview_rows = [row.asDict(recursive=True) for row in preview_df.limit(20).collect()]
            # logger.info("Enriched row preview (batch=%d, limit=20): %s", idx, preview_rows)

            final_df = final_df.unionByName(enriched_df,allowMissingColumns=True)
            # logger.info(
            #     "Union complete: batch=%d unchanged_rows_after=%d",
            #     idx,
            #     final_df.count(),
            # )

        
        # overwrite staging
        (
            final_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(staging_tbl)
        )

        logger.info("cross_entity_joins completed " "for %s",staging_tbl)

    
    # POST PROCESSING
    def apply_post_processing(self, df, post_processing: dict):
        if not post_processing:
            return df
        
        # FILTERS
        for f in post_processing.get("filters", []):
            if f.get("condition"):
                logger.info("Applying filter: %s",  f["condition"])
                df = df.filter(F.expr(f["condition"]))

        window_spec = None
        latest = post_processing.get("latest")
        dedup = post_processing.get("deduplication")
        
        # LATEST
        if latest:
            partition_cols = (
                latest.get("partition_by")
                or ["study_id"]
            )
            order_col = latest["column"]
            window_spec = {
                "partition_by": partition_cols,
                "order_by": [F.col(order_col).desc()]
            }
            logger.info(
                "Applying LATEST logic: partition=%s order_by=%s DESC",
                partition_cols, order_col
            )
        # DEDUP
        elif dedup:
            order_by_config = dedup.get("order_by",[])
            if isinstance(order_by_config, str):
                order_by_config = [order_by_config]
            parsed_order = []
            for col_expr in order_by_config:
                if isinstance(col_expr, list):
                    col_expr = " ".join(map(str, col_expr))
                col_expr = str(col_expr).strip()
                parsed_order.append(col_expr.split())

            order_exprs = [
                (F.col(parts[0]).desc()
                    if len(parts) > 1
                    and parts[1].lower() == "desc"
                    else F.col(parts[0]).asc()
                )
                for parts in parsed_order
            ]

            window_spec = {
                "partition_by": dedup.get("keys",[]),
                "order_by": order_exprs}
            logger.info(
                "Applying DEDUP logic: keys=%s order_by=%s",
                dedup.get("keys", []), dedup.get("order_by")
            )
        # APPLY WINDOW
        if window_spec:
            w = Window.partitionBy(*window_spec["partition_by"]).orderBy(*window_spec["order_by"])
            if latest:
                df = df.withColumn("_rn", F.rank().over(w)).filter(F.col("_rn") == 1).drop("_rn")
            else:
                df = (df.withColumn( "_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn"))

        return df