# Silver Framework - Technical Specification and KT Guide

## 1. Purpose
This document describes implementation details for Silver transformation, metadata, DQ, and execution orchestration.

Primary runtime notebook:
- databricks_bundle/src/drugdev_silver_pipeline_workflow.ipynb

Core modules:
- databricks_bundle/drugdev/silver_framework/metadata_service/metadata_loader.py
- databricks_bundle/drugdev/silver_framework/silver_engine/silver_pipeline.py
- databricks_bundle/drugdev/silver_framework/silver_engine/silver_transformer.py
- databricks_bundle/drugdev/silver_framework/silver_engine/dqx_validator.py

## 2. YAML Generation (Design-Time)
Silver uses split config generation: vendor catalog + entity overrides.

```powershell
python .\databricks_bundle\drugdev\silver_framework\configs\silver_yaml_generator.py `
  --vendor-catalog docs/silver_vendor_catalog.xlsx `
  --entity-level docs/silver_entity_level.xlsx `
  --vendor-output databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_vendor_catalog.yaml `
  --entity-output databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_entity_level.yaml
```

Optional legacy merged output:
- add --output databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_config.yaml

Merge logic summary:
- Vendor YAML stores default rules/patterns by vendor+entity.
- Entity YAML stores dataset-level mapping and overrides.
- Entity-level values override vendor defaults during runtime merge.

## 3. Runtime Architecture
Notebook execution sequence:
1. Install dependencies and bootstrap Silver engine import path.
2. Set Spark performance tuning flags (AQE/Delta write settings).
3. Load widgets and validate scope controls.
4. Compute dynamic worker count and apply spark.conf values.
5. Create required metadata and monitoring tables (idempotent).
6. Load Silver metadata from YAML into registry tables.
7. Validate required dependency tables.
8. Build alerting config and run Silver pipeline by mode.
9. Emit layer completion alert and run summary.

## 4. Runtime Inputs and Modes
Key widget parameters:
- catalog, bronze_schema, silver_schema
- metadata_catalog, registry_schema, metadata_schema
- environment, source_bucket, run_date
- mode, domain, entity, study_id, vendor
- entity_workers, config_path, pii_unmask_group

Mode semantics:
- all: run all active domains in registry.
- domain: run one domain.
- entity: run one entity in one domain.
- study: run one entity for one study.

## 5. Metadata and Control Tables
Silver workflow ensures these are available:
- drugdev_silver_registry
- pipeline_execution_metrics
- alert_history
- dq_validation_results_log

Registry model highlights:
- One row per (study, vendor, entity).
- Stores mapping_rules, canonical_cols, SCD keys, DQ rules, sensitivities.
- Supports active/inactive lifecycle and environment partitioning.

### 5.1 Logging and Registry Usage Details
Primary Silver control and observability tables:
- drugdev_silver_registry: canonical execution contract for each Silver dataset slice.
- pipeline_execution_metrics: task/entity execution audit across layers (Silver writes here for runtime status and throughput).
- dq_validation_results_log: rule-level quality outcomes and pass rates.
- alert_history: notification audit records for completion and failure alerts.

How Silver uses each table:
- drugdev_silver_registry
  - Source of truth for active scopes in mode=all/domain/entity/study.
  - Defines transformation behavior (mapping_rules, canonical_cols, scd_type, zorder_cols, validation_rules).
  - Supports environment-specific filtering and lifecycle deactivation without code changes.
- pipeline_execution_metrics
  - Used to persist execution start/end, duration, status, error details, records_written.
  - Enables trend analytics such as run-time drift, repeated failures, and throughput regressions.
- dq_validation_results_log
  - Captures each evaluated rule with severity, mode, result, failed counts, and pass_rate_pct.
  - Feeds DQ degradation detection and layer completion alert severity.
- alert_history
  - Stores SILVER_COMPLETION events and supports audit of whether email/teams notifications were sent.

Common monitoring queries:
```sql
-- Silver execution status trend (last 7 days)
SELECT task_name, status, COUNT(*) AS runs,
       AVG(duration_seconds) AS avg_duration_s,
       SUM(COALESCE(records_written, 0)) AS total_records_written
FROM <metadata_catalog>.<metadata_schema>.pipeline_execution_metrics
WHERE UPPER(layer) = 'SILVER'
  AND execution_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY task_name, status
ORDER BY runs DESC;
```

```sql
-- Silver DQ failed rules by table
SELECT domain, vendor, study_id, table_name,
       COUNT(*) AS failed_rules,
       ROUND(AVG(pass_rate_pct), 2) AS avg_pass_rate_pct
FROM <metadata_catalog>.<metadata_schema>.dq_validation_results_log
WHERE UPPER(layer) = 'SILVER'
  AND UPPER(COALESCE(result, '')) <> 'SUCCESS'
  AND validation_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY domain, vendor, study_id, table_name
ORDER BY failed_rules DESC;
```

## 6. Silver Processing Responsibilities
At runtime, Silver engine typically performs:
- Bronze-to-canonical column mapping.
- Transformation logic execution.
- Cross-entity SQL enrichment where configured.
- SCD handling and current/historical state writes.
- DQ validation and pass-rate tracking.
- Monitoring inserts to metrics and DQ logs.

Completion state interpretation:
- SUCCESS
- PARTIAL_SUCCESS (for partial entity outcomes or DQ degradation)
- FAILED

## 7. Dependency Contract
Silver notebook checks required metadata lookups:
- country_region_mapping
- planisware_exs_ref
- site_parent_mapping

Pipeline fails fast if any required table is missing.

## 8. Alerts and Observability
Alerting is generated from Spark conf (no hardcoded recipients in notebook).

Silver completion alert payload includes:
- pipeline_run_id
- failed_count
- partial_count
- dq_degraded_count

Core observability tables:
- pipeline_execution_metrics for task/entity metrics.
- dq_validation_results_log for rule outcomes.
- alert_history for notification audit.

## 9. Failure and Recovery
Typical issues:
- Missing metadata dependency table.
- Invalid mapping rules or missing source columns.
- DQ failures causing degraded/failed status.
- Performance bottlenecks due to over-parallelism.

Recovery:
1. Re-run in narrower scope mode (domain/entity/study).
2. Fix metadata entries in registry or config YAML.
3. Re-run Silver only; Bronze re-ingestion is usually not required.
4. Confirm alert and metrics status normalization after rerun.

## 10. KT Runbook
1. Update vendor/entity Excel files.
2. Regenerate split Silver YAMLs.
3. Execute Silver notebook in test scope first.
4. Validate registry, metrics, DQ and output tables.
5. Scale to full domain/all mode after validation.

## 11. Handover Checklist
- Team can regenerate split Silver YAMLs.
- Team can explain mode-based execution controls.
- Team can diagnose PARTIAL_SUCCESS vs FAILED.
- Team can tune entity_workers safely.
- Team can trace DQ failures to specific rules/datasets.
