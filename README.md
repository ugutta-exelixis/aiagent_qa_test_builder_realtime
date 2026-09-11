# DDDA Clinical Data Platform

Databricks Lakehouse for clinical trial data — IRT, EDC, CTMS, IPP.

## Architecture

```
S3 Raw → Bronze (Auto Loader) → Silver (DQX + SCD2) → Gold (Star Schema) → Tableau
```

## Quick Start

### 1. Infrastructure (Phase 1)
```bash
databricks clusters create --json @config/cluster/clinical-etl-prod.json
# Run SQL:
# sql/setup/01_create_catalogs_schemas.sql
# sql/setup/02_create_schema_registry.sql
# sql/setup/03_create_monitoring_tables.sql
bash scripts/setup/create_s3_structure.sh
python scripts/setup/validate_phase1.py
```

### 2. Deploy Workflow
```bash
databricks jobs create --json @config/workflows/clinical_daily.json
```

### 3. Onboard a Study (Phase 5)
```bash
cp config/intake_forms/TEMPLATE_intake.yaml config/intake_forms/XL092-305_intake.yaml
# Fill in the YAML...
# Set approval.status = APPROVED
# Run notebooks/onboarding/discover_schema.py
# Run notebooks/onboarding/register_study.py
# Run notebooks/onboarding/validate_onboarding.py
```

## Project Structure

```
ddda-clinical-platform/
├── config/          ← prod.yaml, cluster, workflow, intake forms
├── sql/             ← DDL: catalogs, schemas, silver, gold, views
├── src/             ← Python package (all business logic)
├── notebooks/       ← Databricks notebook entry points (thin wrappers)
├── scripts/         ← One-time setup + recovery utilities
└── tests/           ← pytest unit + integration tests
```

## Catalog Topology

| Catalog | Bronze | Silver | Gold |
|---------|--------|--------|------|
| prod_clinical_irt_catalog  | ✓ | ✓ | ✓ |
| prod_clinical_edc_catalog  | ✓ | ✓ | ✓ |
| prod_clinical_ctms_catalog | ✓ | ✓ | ✓ |
| prod_clinical_ipp_catalog  | ✓ | ✓ | — |
| prod_clinical_common_catalog | — | — | gold_common (dims, aggs, monitoring) |

## Daily Workflow DAG

```
file_tracking
  ├─ bronze_irt
  ├─ bronze_edc
  └─ bronze_ctms
       └─ schema_drift
            ├─ silver_irt
            ├─ silver_edc
            └─ silver_ctms
                 └─ dq_alert_check
                      └─ gold_pipeline
                           └─ pipeline_summary
```

## Running Tests
```bash
cd ddda-clinical-platform
pytest tests/unit/ -v
pytest tests/integration/ -v --databricks  # requires cluster
```
