# config/workflows/

## Single Pipeline — study_id is the execution unit

`clinical_domain_pipeline.yaml` is the one workflow definition for the entire platform.

### Design principle

| Before | After |
|---|---|
| One job per domain | One job for all domains |
| `domain` was the execution unit | `study_id` is the execution unit |
| Domain passed as external parameter | Domain always read from schema_registry |
| 4 scheduled jobs | 1 scheduled job |

### DAG shape

```
file_tracking
    │
    │  emits active_studies task value
    │  [{study_id: "XL092-009", domain: "IRT",  lookback_days: 1},
    │   {study_id: "XB002-101", domain: "EDC",  lookback_days: 1},
    │   {study_id: "PRJ-0042",  domain: "CTMS", lookback_days: 7}, ...]
    │
    ▼
bronze_foreach  [ForEach — concurrency=8]
    ├── bronze_study  study=XL092-009  domain=IRT
    ├── bronze_study  study=XB002-101  domain=EDC
    ├── bronze_study  study=PRJ-0042   domain=CTMS
    └── ... one task per active study
    │
    ▼
schema_drift  (once — all studies)
    │
    ▼
silver  (once — all studies)
    │
    ▼
dq_alert
    │
    ▼
gold
    │
    ▼
pipeline_summary
```

### What each task receives

| Task | Parameters | How it gets domain/study |
|---|---|---|
| `file_tracking` | none | queries schema_registry for all ACTIVE studies |
| `bronze_study` | `study_input` = `{"study_id":…, "domain":…, "lookback_days":…}` | parsed from task value JSON |
| `schema_drift` | none | queries schema_registry internally |
| `silver` | none | queries schema_registry internally |
| `dq_alert` | none | queries schema_registry internally |
| `gold` | none | queries gold_source_config + schema_registry |
| `pipeline_summary` | none | queries pipeline_execution_metrics |

### Adding a new study

```sql
INSERT INTO common.schema_registry (
  schema_id, domain, vendor, study_id, entity, ...
  status, effective_date, is_current_version
) VALUES (..., 'ACTIVE', current_date(), TRUE);
```

It appears in the next scheduled run automatically. No workflow changes. No code changes.

### Adding a new domain

1. INSERT into `common.domain_registry` (sets `default_lookback_days` for the domain).
2. INSERT study/vendor/entity rows into `common.schema_registry`.
3. Done — next run picks it up automatically.

### Deploying

```bash
cd dab/
databricks bundle deploy --target prod
```
