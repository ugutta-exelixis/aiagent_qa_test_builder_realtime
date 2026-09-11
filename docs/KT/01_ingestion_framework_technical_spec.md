# Ingestion Framework - Technical Specification and KT Guide

## 1. Purpose
This document provides implementation-level KT for the Bronze ingestion framework used in the source-notebook pipeline.

Primary runtime notebook:
- databricks_bundle/src/drugdev_data_extraction_workflow.ipynb

Core code modules:
- databricks_bundle/drugdev/ingestion_framework/metadata_service/metadata_loader.py
- databricks_bundle/drugdev/ingestion_framework/ingestion_engine/autoloader_engine.py
- databricks_bundle/drugdev/ingestion_framework/ingestion_engine/schema_registry.py

## 2. YAML Generation (Design-Time)
Use the ingestion YAML generator to produce the runtime config:

```powershell
python .\databricks_bundle\drugdev\ingestion_framework\configs\yaml_generator.py --input .\docs\drugdev_datasets_intake.xlsx --output .\databricks_bundle\drugdev\ingestion_framework\configs\drugdev_config.yaml
```

What it does:
- Reads intake Excel (Domain_Info + Datasets).
- Converts data types and list fields (for example primary_keys).
- Builds domain metadata + datasets into a single YAML.
- Output file is consumed by metadata loader and autoloader engine.

## 3. Runtime Architecture
High-level sequence:
1. Notebook initializes Spark session and path resolution.
2. Widgets are loaded and propagated to spark.conf under drugdev.* keys.
3. Alert notifier is initialized (email/teams routing).
4. Metadata and registry tables are created if missing.
5. Ingestion metadata is loaded from YAML into Delta metadata tables.
6. AutoLoader engine ingests datasets from S3 into Bronze Delta tables.
7. Completion/failure alert is sent.

## 4. Runtime Inputs
Widget parameters expected by workflow notebook:
- raw_bucket, stg_bucket
- catalog, bronze_schema, silver_schema, gold_schema
- metadata_catalog, registry_schema, metadata_schema
- environment, source_bucket
- run_date, simulation_type

Important spark.conf outputs set by notebook:
- drugdev.YML_CONFIG_PATH
- drugdev.SCHEMA_REGISTRY_PATH
- drugdev.RUN_DATE
- all schema/catalog identifiers for downstream jobs

## 5. Metadata Model (Created/Used)
The notebook creates and/or uses these metadata tables:
- dataset_registry
- ingestion_config
- dataset_tags
- dataset_dependencies
- ingestion_runtime_state
- dq_sla_config
- schema_registry

Plus volumes:
- schemas
- checkpoints

Metadata loader responsibilities:
- Replace placeholders in YAML using runtime spark.conf values.
- Upsert dataset registry rows.
- Upsert ingestion_config rows by dataset/environment.
- Upsert tags and dependencies.
- Upsert DQ SLA settings.

### 5.1 Registry Usage by Table
- dataset_registry: master catalog of datasets and lifecycle state used by Bronze ingestion joins and governance.
- ingestion_config: runtime ingestion contract per dataset and environment (bucket, path, format, delimiter, load_type, checkpoint, schema location).
- dataset_tags: metadata tags applied to Unity Catalog Bronze tables for discoverability and governance.
- dataset_dependencies: lineage-style dependency map used for orchestration and impact analysis.
- dq_sla_config: dataset-level DQ policy and escalation settings for downstream quality controls.
- schema_registry: stores schema snapshots and version history per dataset_id to detect and audit schema drift.

### 5.2 Ingestion Runtime Logging Model
Primary logging table:
- ingestion_runtime_state

Typical row semantics:
- One row per dataset ingestion attempt.
- runtime_id: unique execution identifier for a dataset-level run.
- dataset_id and environment: scope identifiers.
- last_run_status: success or failed.
- records_ingested and files_processed: runtime throughput metrics.
- last_run_start_time and last_run_end_time: duration and SLA analysis.
- failure_reason: detailed error payload for failed runs.

Operational usage:
- Identify failed datasets for rerun prioritization.
- Build freshness dashboards for Bronze completion latency.
- Measure volume anomalies by comparing records_ingested against historical baseline.
- Join with dataset_registry to convert dataset_id into business dataset_name.

Example operational queries:
```sql
-- Latest failed Bronze datasets
SELECT irs.dataset_id, dr.dataset_name, irs.last_run_status, irs.failure_reason,
			 irs.last_run_start_time, irs.last_run_end_time
FROM <metadata_catalog>.<metadata_schema>.ingestion_runtime_state irs
LEFT JOIN <metadata_catalog>.<registry_schema>.dataset_registry dr
	ON dr.dataset_id = irs.dataset_id
WHERE LOWER(COALESCE(irs.last_run_status, '')) = 'failed'
ORDER BY COALESCE(irs.last_run_end_time, irs.last_run_start_time) DESC;
```

```sql
-- Bronze ingestion throughput by dataset for last 7 days
SELECT irs.dataset_id, dr.dataset_name,
			 SUM(COALESCE(irs.records_ingested, 0)) AS total_records,
			 SUM(COALESCE(irs.files_processed, 0)) AS total_files
FROM <metadata_catalog>.<metadata_schema>.ingestion_runtime_state irs
LEFT JOIN <metadata_catalog>.<registry_schema>.dataset_registry dr
	ON dr.dataset_id = irs.dataset_id
WHERE COALESCE(irs.last_run_end_time, irs.last_run_start_time) >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY irs.dataset_id, dr.dataset_name
ORDER BY total_records DESC;
```

## 6. AutoLoader Engine Behavior
Supported formats and handling:
- CSV/TXT: batch overwrite or cloudFiles streaming append.
- JSON: batch overwrite or cloudFiles streaming append.
- SAS7BDAT: binary read + pandas.read_sas + Spark write.
- XLSX/XLS: binary read + pandas.read_excel + Spark write.

Operational behavior:
- Reads active datasets from metadata tables.
- Resolves source path pattern based on dataset frequency.
- Sanitizes column names and enriches with technical columns.
- Performs schema change detection and schema_registry versioning.
- Writes ingestion runtime status to ingestion_runtime_state.
- Applies Unity Catalog table tags from dataset YAML config.
- Runs dataset processing concurrently using ThreadPoolExecutor.

## 7. Failure Model and Recovery
Failure handling pattern:
- Dataset-level exceptions are caught and logged.
- Failed dataset runtime row written with failure_reason.
- Batch run fails if one or more datasets fail (final exception).

Recovery approach:
1. Inspect ingestion_runtime_state for failed dataset_id and reason.
2. Validate source object presence/path and file pattern.
3. Validate delimiter/encoding/schema in YAML.
4. Re-run Bronze notebook only (Silver/Gold can be rerun later).

## 8. Production Controls
Recommended controls during KT handover:
- Maintain strict naming conventions in intake Excel.
- Keep checkpoint and schema locations unique per dataset.
- Use explicit file_name patterns for daily_with_datefolder datasets.
- Monitor schema_registry version increments for drift tracking.
- Keep source_bucket and environment aligned with deployment target.

## 9. KT Runbook (Step-by-Step)
1. Update intake workbook docs/drugdev_datasets_intake.xlsx.
2. Regenerate YAML with yaml_generator.py command.
3. Commit YAML and intake changes.
4. Trigger Databricks pipeline or run Bronze notebook manually.
5. Validate new/updated Bronze tables and runtime_state records.
6. Confirm completion alert in alert_history.

## 10. Handover Checklist
- Team can generate and validate ingestion YAML independently.
- Team understands metadata table contracts and purpose.
- Team can triage dataset failures and recover safely.
- Team can onboard a new Bronze dataset end-to-end.
- Team can explain schema drift/version behavior.
