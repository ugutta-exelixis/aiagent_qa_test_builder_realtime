# Notification Framework - Technical Specification and KT Guide

## 1. Purpose
This document describes the shared notification framework used by Bronze, Silver, Gold, and pipeline summary notebooks.

Framework location:
- databricks_bundle/drugdev/notification_faramework

Core files:
- alert_notifier.py
- sql_utils.py

## 2. Functional Scope
AlertNotifier supports:
- Email notifications (AWS SES or SMTP).
- Microsoft Teams webhook notifications.
- Severity-based routing controls.
- Duplicate alert suppression window.
- Alert audit persistence in alert_history table.

## 3. Configuration Contract
Configuration is passed from workflow notebooks via spark.conf-driven JSON and then instantiated into cfg dict.

Expected config structure:
- notifications.email
- notifications.teams
- notifications.routing
- notifications.domain_recipients

Required spark.conf keys are validated in notifier bootstrap:
- METADATA_CATALOG, METADATA_SCHEMA, REGISTRY_SCHEMA
- CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA
- environment, CONFIG_PATH

## 4. Routing and Dedup Logic
Routing logic:
- Severity controls Teams enablement through routing map.
- Email enablement controlled by notifications.email.enabled.
- Recipient resolution:
  1. domain_recipients override by domain
  2. normalized domain fallback
  3. severity default recipients

Dedup logic:
- Suppresses same alert_type + domain + title within 1 hour.
- Suppressed alerts are still logged with status=SUPPRESSED.

## 5. Persistence and Audit
All alerts are logged to alert_history with:
- alert_id, type, severity, title, message
- domain, vendor, entity
- status
- timestamp
- teams_sent/email_sent flags

Logging uses DataFrame-based insert helper (df_insert) for safer writes.

## 6. SQL Safety Utilities
sql_utils.py provides:
- sql_str: escapes literals for SQL interpolation.
- sql_identifier: validates identifiers against safe pattern.
- df_insert: typed DataFrame append writes.
- df_update: DataFrame MERGE-based updates with retry for concurrency conflicts.

Security intent:
- Prevent SQL injection and malformed writes from free-text alert content.
- Reduce Delta merge type/concurrency issues with schema-aligned DataFrame writes.

## 7. Integration Points
Integrated by:
- Bronze workflow notebook
- Silver workflow notebook
- Gold workflow notebook
- Pipeline summary notebook

Typical call pattern:
1. Build config from spark.conf.
2. Create AlertNotifier(cfg).
3. Send completion/failure/summary alerts with context payload.

## 8. Failure Behavior
Notification failures are generally non-fatal to data processing path:
- Email/Teams send exceptions are caught and logged.
- Pipeline may continue while logging notification warning.

Critical exception paths:
- Missing required configuration keys can raise early ValueError.

## 9. KT Runbook
1. Validate spark.conf notification keys.
2. Run a low-severity test alert from notebook.
3. Verify email/teams delivery.
4. Verify alert_history row insertion.
5. Test duplicate suppression behavior by replaying same payload.

## 10. Handover Checklist
- Team can configure routing for new domains/severities.
- Team can switch SES/SMTP drivers safely.
- Team can verify and query alert_history audit records.
- Team can debug delivery vs routing vs dedup issues.
