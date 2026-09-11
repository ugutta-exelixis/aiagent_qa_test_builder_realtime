# CHANGES.md

## v2.0.0 — 2026-03-15

### Architecture Changes
- **Three-layer config model**: vendor catalog YAML + study overrides YAML + study intake YAML
  - `config/vendor_catalog/<domain>/<vendor>.yaml` — canonical_cols, drift_policy, DQ rules, transformation_logic
  - `config/vendor_catalog/irt/4g_clinical_study_overrides.yaml` — per-study renames + transformation_overrides
  - `config/intake_forms/<STUDY_ID>_intake.yaml` — s3_delivery_path, enrollment_statuses, enabled flags only
- **S3 file discovery**: intake forms no longer require per-entity `sample_file` paths. Engine auto-discovers latest file via `s3_delivery_path` + vendor `file_name_pattern_template`
- **Single catalog**: `dev_ddda_clinical_platform` with domain sub-schemas (`bronze_irt`, `silver_irt` etc.)
  - Registry tables (vendor_config, domain_registry, etc.) → `gold` schema
  - Monitoring tables (schema_registry, dq_validation_results, etc.) → `common` schema
- **Planisware** replaces `IPP` as domain name — schemas: `bronze_planisware`, `silver_planisware`
- **Flat Gold schema**: all Gold objects in single `gold` schema (not domain-prefixed)

### New Files
- `src/onboarding/intake_engine.py` — full intake engine with S3 discovery, canonical mapping, DQ auto-generation
- `config/vendor_catalog/` — vendor catalog YAMLs for all 9 vendors across IRT/EDC/CTMS/IPP
- `config/intake_forms/XB002-101_intake.yaml` through `XL092-311_intake.yaml` — all 10 study intakes
- `databricks.yml` — Databricks Asset Bundle configuration for dev/uat/prod promotion

### Updated Files
- `src/utils/config_loader.py` — updated FQN builder for new schema layout
- `config/environments/dev.yaml` — new schema layout, bronze_schemas/silver_schemas dicts
- `config/environments/uat.yaml` — new (added)
- `config/environments/prod.yaml` — updated for new schema layout
- `scripts/setup/generate_workflows.py` — reads from vendor_schedule table, uses Databricks SDK

### Removed
- Per-entity `sample_file` in intake forms → replaced by `s3_delivery_path` at vendor level
- `mapping_rules` in intake forms → moved to `4g_clinical_study_overrides.yaml`
- `extra_cols` / `core_cols` / `optional_cols` → replaced by `canonical_cols` in vendor catalog
- Separate `bronze_irt.py`, `bronze_edc.py` etc. notebooks → single generic `bronze.py`

---

## v1.0.0 — 2026-03-01

### Initial Release
- Bronze ingestion via Auto Loader for IRT (4G, Cenduit), EDC (Medidata),
  CTMS (PRA, IQVIA), IPP (Planisware)
- Silver layer with DQX validation and SCD Type 2 merge
- Gold layer with dimension, fact, and aggregate builders
- schema_registry as central metadata table
- Per-domain Workflow JSONs generated from vendor_schedule table
- Study onboarding via intake YAML forms
