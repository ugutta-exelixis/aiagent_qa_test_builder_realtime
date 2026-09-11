# Gold Framework - Technical Specification and KT Guide

## 1. Purpose
This document provides detailed KT for Gold object metadata, orchestration, model execution, tagging, and masking.

Primary runtime notebook:
- databricks_bundle/src/drugdev_gold_pipeline_workflow.ipynb

Core modules:
- databricks_bundle/drugdev/gold_framework/metadata_service/metadata_loader.py
- databricks_bundle/drugdev/gold_framework/gold_engine/gold_engine.py
- databricks_bundle/drugdev/gold_framework/gold_engine/gold_model.py
- databricks_bundle/drugdev/gold_framework/gold_engine/models/*

## 2. YAML Generation (Design-Time)
Gold object config generation command:

```powershell
python .\databricks_bundle\drugdev\gold_framework\configs\gold_objects_yaml_generator.py --input .\docs\gold_objects.xlsx --output .\databricks_bundle\drugdev\gold_framework\configs\drugdev_gold_config.yaml
```

Generator behavior:
- Reads GoldObjects sheet.
- Validates required metadata columns.
- Parses dependencies, tags, and optional column_tags.
- Outputs objects list consumed by Gold metadata loader/engine.

## 3. Runtime Architecture
Workflow notebook sequence:
1. Load widgets, performance settings, and path context.
2. Apply Spark conf for catalog/schema/environment settings.
3. Initialize secret/token resolver from environment config.
4. Create idempotent metadata and monitoring tables.
5. Load gold object metadata from YAML to gold_object_registry.
6. Run inline master table population for normalization seeds.
7. Execute dependency checks (required and optional metadata tables).
8. Run Gold engine with mode=all or mode=object.
9. Emit run summary and fail on unrecovered errors.

## 4. Runtime Inputs and Modes
Primary widgets:
- catalog, silver_schema, gold_schema
- metadata_catalog, metadata_schema, registry_schema
- environment, run_date, source_bucket
- mode, object_name, max_workers, config_path
- pii_unmask_group, pipeline_run_id

Mode semantics:
- all: execute enabled Gold objects by execution_order waves.
- object: execute only one named object.

## 5. Gold Metadata Contract
gold_object_registry columns include:
- object_name, object_type, build_strategy, class_path
- write_mode, execution_order, dependencies, sql_template
- partition_cols, tags, is_enabled

Supported build strategies:
- PYSPARK_MODEL
- FULL_REFRESH
- VIEW_DDL

### 5.1 Gold Registry and Logging Usage
Primary Gold control and logging tables:
- gold_object_registry: execution catalog of Gold objects and build contracts.
- pipeline_execution_metrics: per-object execution audit with status and performance metrics.
- alert_history: GOLD_COMPLETION notification audit.
- dq_validation_results_log: cross-layer DQ log table (Gold section present if Gold-level checks write results).

gold_object_registry usage:
- Controls object enablement using is_enabled.
- Determines wave orchestration through execution_order.
- Selects build behavior via build_strategy.
- Supports dependency visibility through dependencies metadata.
- Carries governance tags and column_tags used for table/column tagging and mask application.

pipeline_execution_metrics usage for Gold:
- Writes one row per Gold object execution attempt.
- Captures run_id, task_name, layer=GOLD, status, duration_seconds, records_written.
- Records compacted error_message and error_type for failed objects.
- Enables object-level SLA and reliability dashboards.

Common monitoring queries:
```sql
-- Gold object reliability and runtime profile
SELECT task_name AS object_name,
			 status,
			 COUNT(*) AS runs,
			 AVG(duration_seconds) AS avg_duration_s,
			 SUM(COALESCE(records_written, 0)) AS total_rows_written
FROM <metadata_catalog>.<metadata_schema>.pipeline_execution_metrics
WHERE UPPER(layer) = 'GOLD'
	AND execution_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 14 DAYS
GROUP BY task_name, status
ORDER BY runs DESC, object_name;
```

```sql
-- Most recent Gold failures with grouped diagnostics
SELECT task_name AS object_name, error_type, error_message,
			 start_time, end_time
FROM <metadata_catalog>.<metadata_schema>.pipeline_execution_metrics
WHERE UPPER(layer) = 'GOLD'
	AND UPPER(status) = 'FAILED'
ORDER BY end_time DESC;
```

## 6. Gold Engine Execution Model
gold_engine.py features:
- Fetch enabled objects from registry.
- Group by execution_order and run each wave in parallel thread pool.
- Optional fail-fast via function arg or spark.conf override.
- Persist per-object execution metrics to pipeline_execution_metrics.
- Aggregate and raise RuntimeError if any object fails.

Error handling model:
- Compact error normalization to reduce JVM noise.
- Optional stacktrace logging via drugdev.GOLD_LOG_STACKTRACE.
- Failure summary grouped by error text for triage.

## 7. Data Security and Tagging
Gold engine includes advanced governance behavior:
- Applies table tags from registry tags JSON.
- Applies column tags from tags.column_tags metadata.
- Creates and applies default masking UDFs by datatype.
- Grants UNMASK permission to configured principals/groups.

Mask support includes:
- string, numeric, decimal, date, timestamp

## 8. Dependency Contracts
Notebook enforces required metadata tables before engine run and treats some tables as optional with fallback behavior.

Required examples include:
- ct_portfolio_source
- participant_status_master
- milestone_code_map
- country_region_mapping
- dim_participant_phase_rules
- study_tf_plus_tumors

Optional examples include:
- site_status_master
- cohort_status_master
- treatment_arm_master

## 9. Failure and Recovery
Common failure categories:
- Missing class_path module or import path mismatch.
- Missing required dependency table.
- SQL template errors in FULL_REFRESH/VIEW_DDL.
- Model logic exceptions or schema mismatches.

Recovery workflow:
1. Re-run single object in mode=object for fast isolation.
2. Fix object metadata or model code.
3. Re-run object or full pipeline depending on impact.
4. Validate metrics and target table/view object state.

## 10. KT Runbook
1. Update docs/gold_objects.xlsx.
2. Regenerate Gold YAML config.
3. Run Gold notebook in object mode for impacted objects.
4. Promote to full all-mode execution.
5. Validate tags, masks, and metrics post-run.

## 11. Handover Checklist
- Team can regenerate Gold config from Excel.
- Team understands wave-based parallel execution.
- Team can apply object-mode recovery.
- Team can explain masking and UNMASK grants.
- Team can troubleshoot class import and strategy errors.

## 12. Recent Gold Object Additions
The following PYSPARK_MODEL objects were added to extend site, participant, and query analytics outputs:

- dim_planisware_daily_hist: daily planisware milestone history projection from normalized milestones.
- site_info: site profile output combining site, country, milestone, and address enrichment.
- enrolled_participants: per-study/per-site enrolled participant counts based on participant status and enrollment visit logic.
- final_monthly_screening_rate: site-level screening and recruitment rate output using milestone boundaries and participant rollups.
- open_queries: opened query counts by study and site.
- query_resolution_time: aggregated opened-to-answered durations and resolved query counts by study and site.
- screen_failure_percentage: site-level screened and screen-failed participant counts.
- site_dates: site activation/selection and first-patient screened/enrolled date output.
- site_study: per-site study counts segmented into active and completed states.

Operational notes:
- Registry source of truth remains docs/gold_objects.xlsx.
- Generated config remains databricks_bundle/drugdev/gold_framework/configs/drugdev_gold_config.yaml.
- Current generator output includes these objects in the total object count.
