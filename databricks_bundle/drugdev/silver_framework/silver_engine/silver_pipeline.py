# src/pipelines/silver_pipeline.py
#
# Orchestrates Bronze → Silver for one domain/entity or full domain.
# scd_business_keys and zorder_cols read from schema_registry — no hardcoding.

import json
import logging
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pyspark import StorageLevel
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession
import yaml
from dqx_validator import DQXValidator, DQCriticalFailureError
from silver_transformer import SilverTransformer, build_bronze_table_name
spark = SparkSession.builder.getOrCreate()


logger = logging.getLogger(__name__)
from sql_utils import df_insert, sql_str, sql_identifier

_DEFAULT_BIZ_KEYS = ['subject_id', 'study_id']
_DEFAULT_ZORDER   = ['study_id']
_MAX_ENTITY_THREADS_CEILING = 8


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
    conf["PIPELINE_RUN_ID"] = spark.conf.get("drugdev.PIPELINE_RUN_ID", "").strip()
    return conf

def REGISTRY() -> str:   return f"`{_get_conf()['CATALOG']}`.{_get_conf()['REGISTRY_SCHEMA']}.drugdev_silver_registry"

CONFIG_PATH = _get_conf()["CONFIG_PATH"]

class SilverPipeline:

    def __init__(self, spark: SparkSession):
        self.spark       = spark        
        self.conf       = _get_conf()
        self.transformer = SilverTransformer(spark)
        self.validator   = DQXValidator(spark)
        
        

    def run_entity(self, domain: str, vendor: str, entity: str, study_id=None,
                   scd_keys: list = None, zorder: list = None,
                   drop_staging: bool = True,
                   do_optimize: bool = True) -> dict:
        """Run full Silver pipeline for one domain/entity."""
        start  = time.time()
        run_id = self.conf.get("PIPELINE_RUN_ID") or uuid.uuid4().hex
        silver = f"`{self.conf['CATALOG']}`.{self.conf['SILVER_SCHEMA']}.{domain.lower()}_{entity}"
        df_staging = None

        logger.info("Silver pipeline: %s/%s", domain, entity)
        try:
            readiness = self._check_bronze_readiness(domain, vendor, entity, study_id)
            if not readiness["ready"]:
                duration = int(time.time() - start)
                msg = (
                    "Bronze readiness gate failed for "
                    f"{domain}/{entity}: {readiness['message']}"
                )
                logger.error(msg)
                self._log(run_id, domain, entity, silver, 'FAILED', msg, start, duration, 0)
                return {
                    'status': 'FAILED',
                    'rows': 0,
                    'dq_pass_rate': 0,
                    'error': msg,
                }

            staging, metadata, post_processing_list, dataset_results = self.transformer.transform(
                domain,
                vendor,
                entity,
                study_id,
                allowed_datasets=set(readiness.get("available_datasets", [])) if readiness.get("available_datasets") else None,
            )
            self._log_dataset_results(run_id, domain, entity, dataset_results)
            if not staging:
                duration = int(time.time()-start)
                msg = "No bronze tables available"
                logger.error("Entity failed %s/%s/%s: %s", domain, vendor, entity, msg)
                self._log(run_id, domain, entity, silver, 'FAILED', msg, start, duration, 0)
                return {'status':'FAILED','rows': 0, 'dq_pass_rate': 0}
            df_staging  = self.spark.read.table(staging)
            if post_processing_list:
                logger.info("Applying %d post transformation rule(s)", len(post_processing_list))
                for post_processing in post_processing_list:
                    df_staging = self.transformer.apply_post_processing(df_staging, post_processing)
                logger.info("Post processing completed")

            # dq_summary  = self.validator.validate(df_staging, domain, entity, metadata = metadata)

            if study_id:
                study_ids   = [study_id]
            else:
                if study_id:
                    study_ids = [study_id]
                elif "study_id" in df_staging.columns:
                    study_ids = [r.study_id
                        for r in df_staging.select("study_id").distinct().collect()
                        if r.study_id]
                else:
                    study_ids = [None]
            if not study_ids:
                logger.warning("No studies found for %s/%s — processing full dataset", domain, entity)
                study_ids = [None]
            metadata_by_study_id = {}
            for m in metadata:
                metadata_by_study_id.setdefault(m.get("study_id"), []).append(m)

            # Persist because the same staged DataFrame is reused across study loops
            # by validation + merge actions.
            if "study_id" in df_staging.columns:
                df_staging = df_staging.repartition("study_id")

            df_staging = df_staging.persist(StorageLevel.MEMORY_AND_DISK)
            # Materialise the cache once here; this count is reused as the final
            # row metric — avoids a second full-table scan after OPTIMIZE.
            staging_count = df_staging.count()


            dq_pass_rate_sum = 0.0
            dq_pass_rate_count = 0
            dq_summary = {"pass_rate": 0}

            if len(study_ids) > 1:
                logger.info("Silver entity %s: study-level processing is sequential  studies=%d  vendor=%s",
                    entity,len(study_ids),vendor or "ALL", )
        
            # Fetch governance metadata from schema_registry so Silver table
            # creation (first run) can apply Unity Catalog tags + masks
            # immediately after DDL — identical pattern to Bronze ingestion.
            gov = self._fetch_governance(domain, entity)
            if scd_keys is None or zorder is None:
                vendor_clause = (
                    f"AND vendor = '{sql_str(vendor)}'"
                    if vendor else
                    ""
                )
                rows = self.spark.sql(f"""
                                      SELECT scd_business_keys, zorder_cols
                                      FROM {REGISTRY()}
                                      WHERE domain = '{sql_str(domain)}'
                                        AND entity = '{sql_str(entity)}'
                                        {vendor_clause}
                                        AND is_active = TRUE
                                      """).collect()

                if scd_keys is None:
                    scd_candidates = []
                    for row in rows:
                        if not row.scd_business_keys:
                            continue
                        try:
                            scd_candidates.append(json.loads(row.scd_business_keys))
                        except Exception:
                            logger.error("Invalid JSON in scd_business_keys for %s/%s (vendor=%s): %s",
                                domain, entity, vendor or "ALL", row.scd_business_keys,) 
                            raise

                    if scd_candidates:
                        scd_keys = scd_candidates[0]
                        if any(candidate != scd_keys for candidate in scd_candidates[1:]):
                            logger.warning("Inconsistent scd_business_keys across active rows for %s/%s (vendor=%s) — using first candidate", domain, entity, vendor or "ALL",)
                    else:
                        scd_keys = _DEFAULT_BIZ_KEYS
                        logger.warning("Missing scd_business_keys for %s/%s (vendor=%s) — using DEFAULT",
                            domain, entity, vendor or "ALL",)

                if zorder is None:
                    merged_zorder = []
                    for row in rows:
                        if not row.zorder_cols:
                            continue
                        try:
                            parsed = json.loads(row.zorder_cols)
                        except Exception:
                            logger.error("Invalid JSON in zorder_cols for %s/%s (vendor=%s): %s",
                                domain, entity, vendor or "ALL", row.zorder_cols, )
                            raise
                        for col_name in parsed:
                            if col_name not in merged_zorder:
                                merged_zorder.append(col_name)

                    if merged_zorder:
                        zorder = merged_zorder
                    else:
                        zorder = _DEFAULT_ZORDER
                        logger.warning(
                            "Missing zorder_cols for %s/%s (vendor=%s) — using DEFAULT",
                            domain,
                            entity,
                            vendor or "ALL",
                        )
            # --------------------------------------------------
            # Run DQ study-wise
            # --------------------------------------------------
            for sid in study_ids:

                if sid is not None and "study_id" in df_staging.columns:
                    logger.info("Processing study_id = %s", sid)
                    df_study = df_staging.filter(F.col("study_id") == sid)
                    sid_key = sid
                else:
                    logger.info(
                        "Processing full dataset %s from %s",
                        entity,
                        domain,
                    )
                    df_study = df_staging
                    sid_key = sid or "_Global"

                dq_result = self.validator.validate(
                    df_study,
                    domain,
                    entity,
                    study_id=sid_key,
                    vendor=vendor,
                    metadata=metadata_by_study_id.get(sid_key, []),
                )

                if isinstance(dq_result, dict):
                    dq_pass_rate_sum += dq_result.get("pass_rate", 0)
                    dq_pass_rate_count += 1

                    dq_summary = {
                        "pass_rate": (
                            dq_pass_rate_sum / dq_pass_rate_count
                        )
                    }

            # --------------------------------------------------
            # Merge once for entire entity
            # --------------------------------------------------

            temp_view = self._safe_temp_view_name(
                sid_key="ENTITY",
                entity=entity,
            )

            logger.info(
                "Creating entity-level temp view %s",
                temp_view,
            )

            df_staging.createOrReplaceTempView(temp_view)

            

            (
                df_staging.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(silver)
            )

            logger.info(
                "Silver table overwritten: %s (%d rows)",
                silver,
                df_staging.count(),
            )

            self._apply_silver_tags(
                table_name=silver,
                table_columns=df_staging.columns,
                domain=domain,
                vendor=vendor,
                entity=entity,
                table_sensitivity=gov.get("table_sensitivity", "CONFIDENTIAL"),
                pii_columns_raw=gov.get("pii_columns", "[]"),
            )

            df_final = self.spark.read.table(silver)
            logger.info("scd_business_keys=%s  zorder_cols=%s  final row count=%d",
                        scd_keys, zorder, df_final.count())
            zorder_cols = zorder if zorder else _DEFAULT_ZORDER
            logger.info("ZORDER BY %s", ",".join(zorder_cols))
            safe_zorder = [c for c in zorder_cols if c in df_final.columns]
            if do_optimize:
                if safe_zorder:
                    try:
                        self.optimize(silver, safe_zorder)
                    except Exception as opt_exc:
                        if "DELTA_ZORDERING_ON_COLUMN_WITHOUT_STATS" in str(opt_exc):
                            logger.warning("Skipping OPTIMIZE ZORDER for %s/%s due missing stats on one or more columns: %s",
                                domain, entity, opt_exc, )
                        else:
                            raise
                else:
                    logger.warning("skipping Zorder for %s/%s", domain, entity)
           
            if drop_staging:
                if df_staging is not None:
                    try:
                        df_staging.unpersist(blocking=False)
                    except Exception as cleanup_exc:
                        logger.warning("Failed to unpersist staging dataframe for %s/%s before drop: %s",
                            domain, entity, cleanup_exc, )
                    finally:
                        df_staging = None
                self.spark.sql(f"DROP TABLE IF EXISTS {staging}")
                logger.info("Dropped staging table %s", staging)

            duration = int(time.time() - start)
            # Use staging row count captured before the merge rather than
            # issuing a COUNT(*) against the full Silver table after OPTIMIZE.
            count = staging_count
            dataset_success_count = sum(1 for d in dataset_results if str(d.get("status", "")).upper() == "SUCCESS")
            dataset_failed_count = sum(1 for d in dataset_results if str(d.get("status", "")).upper() != "SUCCESS")
            entity_status = 'PARTIAL_SUCCESS' if dataset_success_count > 0 and dataset_failed_count > 0 else 'SUCCESS'
            status_note = (
                f"Partial source availability: success={dataset_success_count}, failed={dataset_failed_count}"
                if entity_status == 'PARTIAL_SUCCESS' else
                None
            )
            logger.info("  %s  current=%d  DQ=%.1f%%  dur=%ds",
                        silver, count, dq_summary['pass_rate'], duration)
            self._log(run_id, domain, entity, silver, entity_status,
                      status_note, start, duration, count)
            return {
                'status': entity_status,
                'rows': count,
                'dq_pass_rate': dq_summary['pass_rate'],
                'dataset_success_count': dataset_success_count,
                'dataset_failed_count': dataset_failed_count,
            }

        except DQCriticalFailureError as e:
            duration = int(time.time() - start)
            logger.error("  DQ CRITICAL failure — Silver NOT written: %s", e)
            self._log(run_id, domain, entity, silver, 'FAILED', str(e), start, duration, 0)
            raise

        except Exception as e:
            duration = int(time.time() - start)
            logger.error("  Silver pipeline error: %s", e)
            self._log(run_id, domain, entity, silver, 'FAILED', str(e), start, duration, 0)
            raise
        finally:
            if df_staging is not None:
                try:
                    df_staging.unpersist(blocking=False)
                except Exception as cleanup_exc:
                    logger.warning("Failed to unpersist staging dataframe for %s/%s in finally: %s", domain, entity, cleanup_exc,)

    def run_domain(self, domain: str, entity_workers_override: int = None) -> dict:
        """Run all active entities for a domain.

        Args:
            domain: Domain name to process.
            entity_workers_override: Optional explicit thread count for
                entity-level parallelism. When None, uses
                domain_registry.max_parallel_ingestion.
        """
        rows = self.spark.sql(f"""
                        SELECT entity, scd_business_keys, zorder_cols
            FROM `{self.conf['CATALOG']}`.{self.conf['REGISTRY_SCHEMA']}.drugdev_silver_registry
            WHERE domain = '{sql_str(domain)}' AND is_active = TRUE
            ORDER BY entity
        """).collect()

        logger.info("Silver domain: %s  entities=%s",
                    domain, [r.entity for r in rows])
        results = {}
        entity_jobs = []
        entity_meta = {}
        for row in rows:
            meta = entity_meta.setdefault(
                row.entity,
                {"scd_candidates": [], "zorder_merged": []},
            )

            raw_keys = row.scd_business_keys
            logger.info("Raw scd_business_keys from registry for %s: %s", row.entity, raw_keys)

            if raw_keys:
                try:
                    meta["scd_candidates"].append(json.loads(raw_keys))
                except Exception:
                    logger.error("Invalid JSON in scd_business_keys for %s: %s",
                                row.entity, raw_keys)
                    raise

            if row.zorder_cols:
                try:
                    parsed_zorder = json.loads(row.zorder_cols)
                except Exception:
                    logger.error("Invalid JSON in zorder_cols for %s: %s",
                                row.entity, row.zorder_cols)
                    raise
                for col_name in parsed_zorder:
                    if col_name not in meta["zorder_merged"]:
                        meta["zorder_merged"].append(col_name)

        for entity_name, meta in entity_meta.items():
            if meta["scd_candidates"]:
                bkeys = meta["scd_candidates"][0]
                if any(candidate != bkeys for candidate in meta["scd_candidates"][1:]):
                    logger.warning("Inconsistent scd_business_keys across active rows for %s/%s — using first candidate",
                        domain,entity_name,
                    )
            else:
                logger.warning("Missing scd_business_keys for %s — using DEFAULT", entity_name)
                bkeys = _DEFAULT_BIZ_KEYS

            if meta["zorder_merged"]:
                zorder = meta["zorder_merged"]
                logger.info("ZORDER BY %s", ",".join(zorder))
            else:
                logger.warning("Missing zorder_cols for %s — using DEFAULT", entity_name)
                zorder = _DEFAULT_ZORDER

            entity_jobs.append((entity_name, bkeys, zorder))

        domain_cfg = {}
        driver_cores = int(
            self.spark.conf.get("spark.databricks.clusterUsageTags.driverCores", "4")
        )
        configured_workers = int(domain_cfg.get('max_parallel_ingestion', 2))
        if entity_workers_override is not None:
            if int(entity_workers_override) < 1:
                raise ValueError("entity_workers_override must be >= 1")
            configured_workers = int(entity_workers_override)

        max_workers = min(
            configured_workers,
            _MAX_ENTITY_THREADS_CEILING,
            # max(driver_cores, 1),
            max(len(entity_jobs), 1),
        )

        logger.info(
            "Silver domain %s: entity parallelism request=%d  effective=%d  entities=%d  driver_cores=%d",
            domain, configured_workers, max_workers, len(entity_jobs), driver_cores, )

        if max_workers <= 1 or len(entity_jobs) <= 1:
            for entity, bkeys, zorder in entity_jobs:
                try:
                    results[entity] = self.run_entity(
                        domain=domain,
                        vendor=None,
                        entity=entity,
                        scd_keys=bkeys,
                        zorder=zorder,
                        do_optimize=False,
                    )
                except Exception as exc:
                    logger.error("Silver entity failed %s/%s: %s", domain, entity, exc)
                    results[entity] = {
                        'status': 'FAILED',
                        'rows': 0,
                        'dq_pass_rate': 0,
                        'error': str(exc),
                    }
            self._run_domain_optimize(domain, entity_jobs, results)						   
            return results

        logger.info("Silver domain %s: entity-level parallelism enabled  entities=%d  max_workers=%d  driver_cores=%d",
            domain, len(entity_jobs), max_workers, driver_cores,)

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"silver_{domain}",
        ) as pool:
            futures = {
                pool.submit(self.run_entity,
                    domain,
                    None,
                    entity,
                    None,
                    bkeys,
                    zorder,
                    True,   # drop_staging
                    False,  # do_optimize — deferred to domain-level pass below										  																				 
                ): entity
                for entity, bkeys, zorder in entity_jobs
            }

            for future in as_completed(futures):
                entity = futures[future]
                try:
                    results[entity] = future.result()
                except Exception as exc:
                    logger.error("Silver entity failed %s/%s: %s", domain, entity, exc)
                    results[entity] = {
                        'status': 'FAILED',
                        'rows': 0,
                        'dq_pass_rate': 0,
                        'error': str(exc),
                    }
        self._run_domain_optimize(domain, entity_jobs, results)													   
        return {entity: results[entity] for entity, _, _ in entity_jobs if entity in results}

    def _check_bronze_readiness(self, domain: str, vendor: str, entity: str, study_id: str = None) -> dict:
        """Readiness gate based on Bronze snapshot availability, not latest ingestion run state."""
        vendor_clause = (
            f"AND vendor = '{sql_str(vendor)}'"
            if vendor else
            ""
        )
        study_clause = (
            f"AND study_id = '{sql_str(study_id)}'"
            if study_id else
            ""
        )

        src_rows = self.spark.sql(f"""
            SELECT DISTINCT source_dataset_name, vendor, study_id
            FROM {REGISTRY()}
            WHERE domain = '{sql_str(domain)}'
              AND entity = '{sql_str(entity)}'
              {vendor_clause}
              {study_clause}
              AND is_active = TRUE
              AND source_dataset_name IS NOT NULL
              AND TRIM(source_dataset_name) <> ''
        """).collect()

        datasets = [r.source_dataset_name for r in src_rows if r.source_dataset_name]
        dataset_sources = {}
        for r in src_rows:
            ds_name = r.source_dataset_name
            if not ds_name:
                continue
            dataset_sources.setdefault(ds_name, []).append(
                {
                    "vendor": r.vendor,
                    "study_id": r.study_id,
                }
            )

        if not datasets:
            return {
                "ready": False,
                "message": "No source_dataset_name mapping found in drugdev_silver_registry.",
            }

        not_ready = []
        available = []

        for dataset_name in datasets:
            has_bronze_snapshot = False
            for src in dataset_sources.get(dataset_name, []):
                row_vendor = src.get("vendor")
                row_study_id = src.get("study_id") or "_GLOBAL"
                if not row_vendor:
                    continue

                bronze_tbl = build_bronze_table_name(domain, row_vendor, row_study_id, entity)
                try:
                    probe = self.spark.sql(
                        f"SELECT 1 FROM {sql_identifier(bronze_tbl, 'bronze table')} LIMIT 1"
                    ).first()
                    if probe is not None:
                        has_bronze_snapshot = True
                        break
                except Exception:
                    continue

            if has_bronze_snapshot:
                available.append(dataset_name)
            else:
                not_ready.append(f"{dataset_name}: no available Bronze snapshot")

        if not available:
            return {
                "ready": False,
                "message": "; ".join(not_ready[:10]) or "No source datasets available for processing.",
                "available_datasets": [],
                "unavailable_datasets": sorted(set(datasets)),
            }

        return {
            "ready": True,
            "message": "OK" if not not_ready else f"Partial availability: available={len(set(available))}, unavailable={len(set(datasets) - set(available))}",
            "available_datasets": sorted(set(available)),
            "unavailable_datasets": sorted(set(datasets) - set(available)),
        }

    def _run_domain_optimize(self, domain: str, entity_jobs: list, results: dict) -> None:
        """Run OPTIMIZE ZORDER once per Silver table after all entities complete.

        Running OPTIMIZE inside run_entity meant one full-table rewrite per
        entity during the pipeline.  Deferring to this single pass means Delta
        compacts all incremental merge writes in one operation per table,
        reducing total OPTIMIZE time by 1-2× on small clusters.
        """
        logger.info(
            "Domain-level OPTIMIZE pass: domain=%s  entities=%d",
            domain, len(entity_jobs),
        )
        for entity, _bkeys, zorder in entity_jobs:
            if results.get(entity, {}).get("status") not in {"SUCCESS", "PARTIAL_SUCCESS"}:
                logger.warning("Skipping OPTIMIZE for %s/%s (entity did not succeed)", domain, entity)
                continue
            silver_tbl = f"`{self.conf['CATALOG']}`.{self.conf['SILVER_SCHEMA']}.{domain.lower()}_{entity}"
            zorder_cols = zorder if zorder else _DEFAULT_ZORDER
            try:
                df_check  = self.spark.read.table(silver_tbl)
                safe_cols = [c for c in zorder_cols if c in df_check.columns]
                if safe_cols:
                    self.optimize(silver_tbl, safe_cols)
                else:
                    logger.warning("No valid ZORDER cols for %s/%s — skipping OPTIMIZE", domain, entity)
            except Exception as opt_exc:
                if "DELTA_ZORDERING_ON_COLUMN_WITHOUT_STATS" in str(opt_exc):
                    logger.warning("Skipping OPTIMIZE ZORDER for %s/%s (missing column stats): %s",
                                   domain, entity, opt_exc)
                else:
                    logger.warning("OPTIMIZE failed for %s/%s: %s", domain, entity, opt_exc)

    def optimize(self, table: str, zorder_cols: list) -> None:
        """OPTIMIZE with ZORDER for query performance.
        Validates table name and column names as SQL identifiers
        before interpolation — both come from internal config but
        should be validated for defence in depth.
        """
        tbl = sql_identifier(table, 'optimize table')
        if not zorder_cols:
            self.spark.sql(f"OPTIMIZE {tbl}")
        else:
            cols = ', '.join(
                f'`{sql_identifier(c, "zorder col")}`' for c in zorder_cols
            )
            self.spark.sql(f"OPTIMIZE {tbl} ZORDER BY ({cols})")
        logger.info("OPTIMIZE %s  ZORDER=%s", table, zorder_cols)
    
    def _safe_temp_view_name(self, sid_key: str, entity: str) -> str:
        safe_sid = re.sub(r"[^A-Za-z0-9_]", "_", str(sid_key)).strip("_") or "Global"
        safe_entity = re.sub(r"[^A-Za-z0-9_]", "_", str(entity)).strip("_") or "entity"
        return f"{safe_sid}_{safe_entity}_staging"

    def _apply_silver_tags(
        self,
        table_name: str,
        table_columns: list,
        domain: str,
        vendor: str,
        entity: str,
        table_sensitivity: str,
        pii_columns_raw,
    ) -> None:
        """Apply table-level and column-level governance tags on Silver output tables."""
        try:
            tbl_id = sql_identifier(table_name, "silver tag table")
            sensitivity = str(table_sensitivity or "CONFIDENTIAL").upper()
            domain_tag = str(domain or "UNKNOWN").upper()
            vendor_tag = str(vendor or "ALL").strip() or "ALL"
            entity_tag = str(entity or "UNKNOWN").strip() or "UNKNOWN"

            self.spark.sql(
                f"ALTER TABLE {tbl_id} SET TAGS ("
                f"  'sensitivity'  = '{sql_str(sensitivity)}', "
                f"  'domain'       = '{sql_str(domain_tag)}', "
                f"  'layer'        = 'SILVER', "
                f"  'vendor'       = '{sql_str(vendor_tag)}', "
                f"  'entity'       = '{sql_str(entity_tag)}'"
                f")"
            )
            logger.info(
                "Silver table tags applied: %s  sensitivity=%s  domain=%s  vendor=%s  entity=%s",
                table_name,
                sensitivity,
                domain_tag,
                vendor_tag,
                entity_tag,
            )
        except Exception as tag_exc:
            logger.warning("Failed applying table tags for %s: %s", table_name, tag_exc)
            return

        self._grant_unmask_on_table(table_name)

        pii_entries = []
        try:
            if isinstance(pii_columns_raw, str):
                pii_entries = json.loads(pii_columns_raw) if pii_columns_raw else []
            elif isinstance(pii_columns_raw, list):
                pii_entries = pii_columns_raw
        except Exception as parse_exc:
            logger.warning("Invalid pii_columns metadata for %s: %s", table_name, parse_exc)
            pii_entries = []

        if not pii_entries:
            return

        column_types = self._get_table_column_types(table_name)
        table_cols_lower = {c.lower() for c in table_columns}
        for entry in pii_entries:
            if isinstance(entry, dict):
                col_name = str(entry.get("column", "") or "").strip()
                col_sensitivity = str(entry.get("sensitivity", sensitivity) or sensitivity).upper()
                pii_category = str(entry.get("pii_category", "PII") or "PII").upper()
                mask_function = str(entry.get("mask_function", "") or "").strip()
            else:
                col_name = str(entry or "").strip()
                col_sensitivity = sensitivity
                pii_category = "PII"
                mask_function = ""

            if not col_name:
                continue
            if col_name.lower() not in table_cols_lower:
                logger.warning("PII column from registry not present in %s: %s", table_name, col_name)
                continue

            try:
                self.spark.sql(
                    f"ALTER TABLE {tbl_id} "
                    f"ALTER COLUMN `{col_name}` SET TAGS ("
                    f"  'sensitivity'  = '{sql_str(col_sensitivity)}', "
                    f"  'pii_category' = '{sql_str(pii_category)}'"
                    f")"
                )
            except Exception as col_exc:
                logger.warning(
                    "Failed applying PII column tags for %s.%s: %s",
                    table_name,
                    col_name,
                    col_exc,
                )

            try:
                dtype = column_types.get(col_name.lower(), "")
                resolved_mask_fn = self._resolve_mask_function(mask_function, dtype)
                if resolved_mask_fn:
                    self.spark.sql(
                        f"ALTER TABLE {tbl_id} "
                        f"ALTER COLUMN `{col_name}` SET MASK {resolved_mask_fn}"
                    )
                    logger.info(
                        "Applied PII mask for %s.%s using %s",
                        table_name,
                        col_name,
                        resolved_mask_fn,
                    )
                else:
                    logger.warning(
                        "Skipping PII mask for %s.%s (unsupported type=%s and no mask_function override)",
                        table_name,
                        col_name,
                        dtype,
                    )
            except Exception as mask_exc:
                logger.warning(
                    "Failed applying PII mask for %s.%s: %s",
                    table_name,
                    col_name,
                    mask_exc,
                )

    def _quote_principal_identifier(self, principal: str) -> str:
        return f"`{str(principal).replace('`', '``')}`"

    def _get_unmask_grantees(self) -> list:
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
            runtime_identity = (
                self.spark.sql("SELECT current_user() AS current_user").collect()[0]["current_user"]
            )
            if runtime_identity:
                grantees.append(runtime_identity)
        except Exception as exc:
            logger.warning("Unable to resolve current_user() for UNMASK grants: %s", exc)

        return list(dict.fromkeys(grantees))

    def _grant_unmask_on_table(self, table_name: str) -> None:
        """Grant UNMASK on the Silver table to configured identities and runtime principal."""
        try:
            tbl_id = sql_identifier(table_name, "silver unmask table")
        except Exception as exc:
            logger.warning("Skipping UNMASK grants due to invalid Silver table identifier %s: %s", table_name, exc)
            return

        for grantee in self._get_unmask_grantees():
            try:
                principal_id = self._quote_principal_identifier(grantee)
                self.spark.sql(f"GRANT UNMASK ON TABLE {tbl_id} TO {principal_id}")
                logger.info("Granted UNMASK on %s to %s", table_name, grantee)
            except Exception as grant_exc:
                logger.warning("Could not grant UNMASK on %s to %s: %s", table_name, grantee, grant_exc)

    def _get_table_column_types(self, table_name: str) -> dict:
        """Return lowercased column name -> Spark SQL simple type string."""
        try:
            schema = self.spark.read.table(table_name).schema
            return {f.name.lower(): f.dataType.simpleString().lower() for f in schema.fields}
        except Exception as exc:
            logger.warning("Could not load schema for masking: %s (%s)", table_name, exc)
            return {}

    def _resolve_mask_function(self, configured_mask_fn: str, dtype: str) -> str:
        """Resolve mask function FQN; default masking currently supports string-like types."""
        if configured_mask_fn:
            return configured_mask_fn

        dtype_l = (dtype or "").lower()
        if dtype_l == "string" or dtype_l.startswith("varchar") or dtype_l.startswith("char"):
            return self._ensure_default_string_mask_function()

        if dtype_l in {"tinyint", "smallint", "int", "bigint"}:
            return self._ensure_default_numeric_mask_function(dtype_l)

        if dtype_l in {"float", "double"}:
            return self._ensure_default_numeric_mask_function(dtype_l)

        if dtype_l.startswith("decimal("):
            return self._ensure_default_decimal_mask_function(dtype_l)

        if dtype_l == "date":
            return self._ensure_default_date_mask_function()

        if dtype_l == "timestamp":
            return self._ensure_default_timestamp_mask_function()

        return ""

    def _build_unmask_predicate(self) -> str:
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

        checks = [f"is_account_group_member('{sql_str(g)}')" for g in dict.fromkeys(group_names)]
        checks.extend(
            f"lower(current_user()) = lower('{sql_str(p)}')"
            for p in dict.fromkeys(principal_names)
        )
        if not checks:
            checks = ["is_account_group_member('pii_unmasked_access')"]
        return " OR ".join(checks)

    def _ensure_default_string_mask_function(self) -> str:
        """Create and return a default string mask function for PII columns."""
        fn_name = spark.conf.get("drugdev.PII_MASK_STRING_FUNCTION", "").strip()
        if fn_name:
            return fn_name

        unmask_predicate = self._build_unmask_predicate()
        cat = self.conf["METADATA_CATALOG"]
        sch = self.conf["METADATA_SCHEMA"]
        fqn = f"`{cat}`.{sch}.mask_pii_string"

        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {fqn}(val STRING)
            RETURNS STRING
            RETURN CASE
                WHEN ({unmask_predicate}) THEN val
                WHEN val IS NULL THEN NULL
                ELSE '***MASKED***'
            END
        """)
        return fqn

    def _ensure_default_numeric_mask_function(self, dtype_l: str) -> str:
        """Create and return a default numeric mask function for integral/float types."""
        unmask_predicate = self._build_unmask_predicate()
        cat = self.conf["METADATA_CATALOG"]
        sch = self.conf["METADATA_SCHEMA"]

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
        fqn = f"`{cat}`.{sch}.{fn_short}"
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {fqn}(val {sql_type})
            RETURNS {sql_type}
            RETURN CASE
                WHEN ({unmask_predicate}) THEN val
                WHEN val IS NULL THEN NULL
                ELSE {masked_expr}
            END
        """)
        return fqn

    def _ensure_default_decimal_mask_function(self, dtype_l: str) -> str:
        """Create and return a default decimal mask function for a specific precision/scale."""
        m = re.match(r"^decimal\((\d+)\s*,\s*(\d+)\)$", dtype_l)
        if not m:
            return ""

        precision = int(m.group(1))
        scale = int(m.group(2))
        if precision < 1 or precision > 38 or scale < 0 or scale > precision:
            return ""

        unmask_predicate = self._build_unmask_predicate()
        cat = self.conf["METADATA_CATALOG"]
        sch = self.conf["METADATA_SCHEMA"]
        sql_type = f"DECIMAL({precision},{scale})"
        fn_short = f"mask_pii_decimal_{precision}_{scale}"
        fqn = f"`{cat}`.{sch}.{fn_short}"
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {fqn}(val {sql_type})
            RETURNS {sql_type}
            RETURN CASE
                WHEN ({unmask_predicate}) THEN val
                WHEN val IS NULL THEN NULL
                ELSE CAST(0 AS {sql_type})
            END
        """)
        return fqn

    def _ensure_default_date_mask_function(self) -> str:
        """Create and return a default DATE mask function."""
        unmask_predicate = self._build_unmask_predicate()
        cat = self.conf["METADATA_CATALOG"]
        sch = self.conf["METADATA_SCHEMA"]
        fqn = f"`{cat}`.{sch}.mask_pii_date"
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {fqn}(val DATE)
            RETURNS DATE
            RETURN CASE
                WHEN ({unmask_predicate}) THEN val
                WHEN val IS NULL THEN NULL
                ELSE DATE'1900-01-01'
            END
        """)
        return fqn

    def _ensure_default_timestamp_mask_function(self) -> str:
        """Create and return a default TIMESTAMP mask function."""
        unmask_predicate = self._build_unmask_predicate()
        cat = self.conf["METADATA_CATALOG"]
        sch = self.conf["METADATA_SCHEMA"]
        fqn = f"`{cat}`.{sch}.mask_pii_timestamp"
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {fqn}(val TIMESTAMP)
            RETURNS TIMESTAMP
            RETURN CASE
                WHEN ({unmask_predicate}) THEN val
                WHEN val IS NULL THEN NULL
                ELSE TIMESTAMP'1900-01-01 00:00:00'
            END
        """)
        return fqn

    def _log_dataset_results(self, run_id: str, domain: str, entity: str, dataset_results: list) -> None:
        """Persist per-source-dataset status rows for each Silver entity execution."""
        for item in (dataset_results or []):
            dataset_name = item.get("source_dataset_name") or item.get("bronze_table") or "unknown_dataset"
            status = str(item.get("status", "FAILED")).upper()
            error = item.get("reason", "") if status != "SUCCESS" else ""
            row_count = int(item.get("row_count", 0) or 0)
            df_insert(self.spark, f"`{self.conf['CATALOG']}`.{self.conf['METADATA_SCHEMA']}.pipeline_execution_metrics", {
                "run_id": run_id,
                "job_name": "silver_pipeline_dataset",
                "task_name": dataset_name,
                "domain": domain,
                "vendor": str(item.get("vendor", "") or ""),
                "study_id": str(item.get("study_id", "") or ""),
                "layer": "SILVER_DATASET",
                "status": status,
                "error_message": str(error or "")[:500],
                "execution_timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
                "records_written": row_count,
            })

    def _fetch_governance(self, domain: str, entity: str) -> dict:
        """
        Fetch table_sensitivity and pii_columns from silver registry
        for use when creating a new Silver table.
        Returns safe defaults if the row is not found or columns are absent.
        """
        try:
            row = self.spark.sql(f"""
                SELECT *
                FROM `{self.conf['CATALOG']}`.{self.conf['REGISTRY_SCHEMA']}.drugdev_silver_registry
                WHERE domain = '{sql_str(domain)}'
                  AND entity = '{sql_str(entity)}'
                  AND is_active = TRUE
                LIMIT 1
            """).first()
            if row:
                row_dict = row.asDict(recursive=True)
                return {
                    "table_sensitivity": row_dict.get("table_sensitivity") or "CONFIDENTIAL",
                    "pii_columns": (
                        row_dict.get("pii_columns")
                        or row_dict.get("pii_column_metadata")
                        or "[]"
                    ),
                }
        except Exception as exc:
            logger.warning(
                "  Could not fetch governance metadata for %s/%s: %s — using defaults",
                domain, entity, exc
            )
        return {
            "table_sensitivity":   "CONFIDENTIAL",
            "pii_columns": "[]",
        }
    def _log(self, run_id, domain, entity, table,
             status, error, start, dur, count):
        """Log silver pipeline metrics using df_insert."""
        df_insert(self.spark, f"`{self.conf['CATALOG']}`.{self.conf['METADATA_SCHEMA']}.pipeline_execution_metrics", {
            "run_id":            run_id,
            "job_name":          "silver_pipeline",
            "task_name":         entity,
            "domain":            domain,
            "layer":             "SILVER",
            "status":            status,
            "error_message":     str(error or '')[:500],
            "start_time":        datetime.fromtimestamp(start, tz=timezone.utc).replace(tzinfo=None),
            "end_time":          datetime.now(timezone.utc).replace(tzinfo=None),
            "duration_seconds":  int(dur),
            "execution_timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
            "records_written":   int(count),
        })

 