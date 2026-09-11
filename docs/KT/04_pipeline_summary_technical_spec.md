# Pipeline Summary Layer - Technical Specification and KT Guide

## 1. Purpose
This document covers the operational summary layer that consolidates Bronze, Silver, Gold, and DQ outcomes into a final run status and notification.

Primary runtime notebook:
- databricks_bundle/src/drugdev_pipeline_summary_standalone.ipynb

Note:
- The Databricks job references this pipeline_summary notebook after Gold completion.

## 2. Responsibilities
Pipeline summary notebook responsibilities:
- Read recent execution records from monitoring tables.
- Present operational views for task, Bronze runtime, DQ, and Gold status.
- Compute overall FAILED/SUCCESS status for a lookback window.
- Send WORKFLOW_SUMMARY alert via notification framework.
- Exit notebook with final status.

## 3. Runtime Inputs
Widget parameters:
- days
- metadata_catalog, metadata_schema, registry_schema
- environment
- catalog, bronze_schema, silver_schema

Derived spark.conf values used:
- drugdev.METADATA_CATALOG
- drugdev.METADATA_SCHEMA
- drugdev.REGISTRY_SCHEMA
- drugdev.environment

## 4. Data Sources
The notebook reads from:
- pipeline_execution_metrics
- ingestion_runtime_state
- dq_validation_results_log
- dataset_registry (optional join for dataset_name resolution)

Primary output is observational/alerting only; it does not build Silver/Gold tables.

### 4.1 Registry and Logging Usage Interpretation
- pipeline_execution_metrics
	- Silver and Gold runtime truth for task/object status, duration, and records_written.
	- Used to derive pipeline_failed_tasks in final status block.
- ingestion_runtime_state
	- Bronze runtime truth for dataset-level success/failure and ingestion volume.
	- Used to derive bronze_failed_datasets and for dataset-specific triage.
- dq_validation_results_log
	- Rule-level quality truth across layers, with pass/fail results and pass_rate_pct.
	- Used to derive dq_failed_rules and quality health summary.
- dataset_registry
	- Optional semantic mapping from dataset_id to dataset_name in summary views.
	- Improves readability for operations and stakeholder reporting.

Cross-table usage patterns:
- Runtime incident triage starts at pipeline_execution_metrics (failed task), then drills to ingestion_runtime_state (Bronze source issue) or dq_validation_results_log (quality issue).
- Trend reporting combines execution durations (pipeline_execution_metrics) with failed rule rates (dq_validation_results_log).
- Business-facing operational reports use dataset_registry joins to avoid technical-only IDs.

## 5. Query Blocks
Query areas included in notebook:
1. Recent execution detail by task/layer.
2. Bronze dataset runtime detail with statuses.
3. Silver DQ summary by domain/vendor/study/table.
4. Layer-level rollup with success/failure counts and durations.
5. Gold object execution detail.
6. Final failure counters and overall status derivation.

Overall logic:
- overall_failed = pipeline_failed_tasks + bronze_failed_datasets + dq_failed_rules
- overall_status = FAILED if overall_failed > 0 else SUCCESS

## 6. Alerting Behavior
Alert type:
- WORKFLOW_SUMMARY

Severity mapping:
- CRITICAL when overall_status=FAILED
- MEDIUM when overall_status=SUCCESS

Alert context includes:
- days
- failed task counts
- failed bronze dataset counts
- failed DQ rule counts
- generated_at_utc

## 7. Operational Usage
When to run:
- End of workflow after Gold completion.
- On-demand run for operational review window (for example days=7).

Use cases:
- Daily monitoring report for platform health.
- Incident triage entry point.
- Audit snapshot for business/operations handover.

## 8. Failure and Recovery
Typical failure reasons:
- Missing required spark.conf keys.
- Monitoring table not available.
- Notification delivery issue.

Recovery:
1. Ensure metadata catalog/schema configs are set.
2. Validate monitoring table existence and access rights.
3. Re-run summary notebook with same days window.
4. If alerting fails, inspect alert_notifier configuration and fallback logs.

## 9. KT Runbook
1. Execute notebook with days=1 for daily checks.
2. Review all displayed query outputs.
3. Validate final status string and notebook exit status.
4. Confirm WORKFLOW_SUMMARY entry in alert_history.
5. Share output snapshot to platform ops channel.

## 10. Handover Checklist
- Team can explain overall status formula.
- Team can trace each failed count back to source table.
- Team can run variable windows (1/3/7 days).
- Team can troubleshoot summary alerting failures.
