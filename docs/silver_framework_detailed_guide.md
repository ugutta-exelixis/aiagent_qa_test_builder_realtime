# Silver Framework Detailed Guide

## 1. Purpose and Scope

This document explains the Silver framework implementation in this repository, starting from the standalone execution notebook and the Excel-driven config generation process.

Primary starting notebook:

- `databricks_bundle/src/drugdev_silver_pipeline_standalone.ipynb`

Config generation command used before notebook execution:

```bash
python .\databricks_bundle\drugdev\silver_framework\configs\silver_yaml_generator.py \
  --vendor-catalog docs/silver_vendor_catalog.xlsx \
  --entity-level   docs/silver_entity_level.xlsx \
  --output         databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_config.yaml
```

## 2. Silver Framework Architecture

At a high level, the Silver flow has two phases:

1. Metadata build phase (Excel -> YAML -> registry metadata).
2. Data processing phase (Bronze -> staged Silver -> DQ -> Silver final table write + optimize).

Core components:

- Notebook orchestrator: `databricks_bundle/src/drugdev_silver_pipeline_standalone.ipynb`
- Excel-to-YAML generator: `databricks_bundle/drugdev/silver_framework/configs/silver_yaml_generator.py`
- Metadata loader: `databricks_bundle/drugdev/silver_framework/metadata_service/metadata_loader.py`
- Pipeline engine: `databricks_bundle/drugdev/silver_framework/silver_engine/silver_pipeline.py`
- Transformer: `databricks_bundle/drugdev/silver_framework/silver_engine/silver_transformer.py`
- DQ validator: `databricks_bundle/drugdev/silver_framework/silver_engine/dqx_validator.py`
- SCD2 merger/optimize utilities: `databricks_bundle/drugdev/silver_framework/silver_engine/scd_type2_merger.py`

## 3. Pre-Run Configuration Step (Excel -> YAML)

### 3.1 Why this step exists

The notebook expects a generated Silver config YAML at:

- `databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_config.yaml`

This YAML is not hand-maintained line-by-line. It is built from two source Excel files:

- `docs/silver_vendor_catalog.xlsx`
- `docs/silver_entity_level.xlsx`

### 3.2 Inputs verified in current repository state

#### Vendor catalog Excel (`docs/silver_vendor_catalog.xlsx`)

- Sheet `Vendors`: 7 rows, 8 columns
  - Columns: `vendor_name, domain, path_type, delivery_method, delivery_schedule, date_regex, date_format, file_name_pattern_template`
- Sheet `Entities`: 45 rows, 8 columns
  - Columns: `vendor_name, entity_name, scd_business_keys, zorder_cols, table_sensitivity, pii_columns, transformation_logic, cross_entity_sql`
- Sheet `CanonicalCols`: 566 rows, 5 columns
  - Columns: `vendor_name, entity_name, canonical_col, col_order, target_type`
- Sheet `ValidationRules`: 222 rows, 9 columns
  - Columns: `vendor_name, entity_name, rule_id, field, check, expression, severity, mode, description`

#### Entity-level Excel (`docs/silver_entity_level.xlsx`)

- Sheet `Datasets`: 301 rows, 10 columns
  - Columns: `dataset_name, vendor_name, entity_name, study_id, domain, source_dataset_name, silver_table_name, is_active, scd_type, transformation_logic`
- Sheet `ColumnMappings`: 175 rows, 4 columns
  - Columns: `dataset_name, bronze_col, canonical_col, target_type`
- Sheet `ValidationRules`: 0 rows, 8 columns
  - Columns: `dataset_name, rule_id, field, check, expression, severity, mode, description`

### 3.3 Merge precedence implemented by generator

The generator applies an explicit precedence model (entity-level overrides win):

- `canonical_cols`: entity-level override if present, else vendor catalog
- `scd_business_keys`: vendor defaults (unless generator or dataset-level logic provides overrides)
- `zorder_cols`: vendor defaults
- `transformation_logic`: combines vendor baseline and dataset override logic
- `cross_entity_sql`: dataset override first, fallback vendor-level
- `validation_rules`: entity-level rules replace vendor-level when present
- `mapping_rules`: built from `ColumnMappings` (converted to canonical->bronze direction)
- `column_types`: merged from vendor canonical types + entity-level mapping types

### 3.4 Output YAML profile (current repo state)

Generated file:

- `databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_config.yaml`

Current summary:

- Datasets: 301
- Distinct entities: 29
- Domain distribution:
  - `ctms`: 215
  - `edc`: 39
  - `planisware`: 1
  - `irt`: 46
- Vendor distribution:
  - `PRA`: 183
  - `IQVIA`: 30
  - `PAREXEL`: 2
  - `MEDIDATA`: 39
  - `PLANISWARE`: 1
  - `4G`: 42
  - `CENDUIT`: 4

## 4. Notebook Execution Flow

Entry notebook:

- `databricks_bundle/src/drugdev_silver_pipeline_standalone.ipynb`

Major notebook stages:

1. Install/import dependencies and initialize Spark session.
2. Set Spark performance configs.
3. Set runtime parameters (catalog/schema/environment/mode/domain/entity/study).
4. Push runtime values into Spark conf for downstream modules.
5. Create/ensure metadata tables.
6. Load Silver registry metadata from generated YAML.
7. Verify metadata content.
8. Check required dependency tables.
9. Execute pipeline (`all`, `domain`, `entity`, or `study` mode).
10. Print run summaries from final Silver tables.

### 4.1 Runtime modes

Supported values:

- `all`: run all active domains from registry
- `domain`: run one domain
- `entity`: run one domain/entity pair
- `study`: run one domain/entity/study combination

Optional vendor filter is supported for entity/study mode.

### 4.2 Dynamic parallelism

Notebook computes `entity_workers` from cluster executor count and cores, with caps and manual override support (`MAX_WORKERS`). This value is passed as Spark conf (`drugdev.entity_workers`) and consumed by pipeline logic.

## 5. Metadata Service Behavior

Module:

- `databricks_bundle/drugdev/silver_framework/metadata_service/metadata_loader.py`

### 5.1 Registry table

Primary metadata table:

- `<METADATA_CATALOG>.<METADATA_SCHEMA>.drugdev_silver_registry`

One row per `(study_id, vendor, entity)` dataset-level Silver configuration.

Important registry columns:

- Dataset identity and grouping: `domain, entity, vendor, study_id, source_dataset_name`
- Target output: `silver_table_name`
- Schema/merge metadata: `scd_type, scd_business_keys, canonical_cols, zorder_cols`
- Mapping and transform metadata: `mapping_rules, transformation_logic, cross_entity_sql`
- Governance metadata: `table_sensitivity, pii_columns`
- DQ metadata: `validation_rules`
- Lifecycle flags: `is_active, environment`

### 5.2 Upsert semantics

`MERGE` key:

- `(source_dataset_name, environment)`

Behavior:

- Match: update metadata fields
- No match: insert new row with generated `registry_id`

### 5.3 Additional metadata tables created by notebook

- `pipeline_execution_metrics`
- `alert_history`
- `dq_validation_results_log`

These support operational observability, alert auditing, and DQ rule logging.

## 6. Silver Pipeline Engine Behavior

Module:

- `databricks_bundle/drugdev/silver_framework/silver_engine/silver_pipeline.py`

### 6.1 `run_entity` flow

Per entity execution sequence:

1. Transform Bronze to staging via transformer.
2. Optionally apply post-processing steps.
3. Evaluate DQ rules per study (or whole dataset if no study key).
4. Write final Silver table (current implementation overwrites table with staged DataFrame).
5. Apply governance tags at table/column level.
6. Optionally run optimize with ZORDER.
7. Log run status and row counts.

Notes:

- Critical DQ failures raise exception and block write.
- Domain-level runs can execute multiple entities in parallel using thread pool.
- Domain-level optimize pass is deferred to run once per entity table after parallel processing.

### 6.2 `run_domain` flow

For selected domain:

1. Read all active entities from Silver registry.
2. Resolve merged `scd_business_keys` and `zorder_cols` from registry JSON.
3. Compute effective worker concurrency (`entity_workers_override`, ceilings, job count).
4. Execute entities sequentially or in thread pool.
5. Run `_run_domain_optimize` over successful entities.

## 7. Transformation Behavior

Module:

- `databricks_bundle/drugdev/silver_framework/silver_engine/silver_transformer.py`

Responsibilities:

- Resolve environment Spark conf context and target FQNs.
- Read registry metadata for selected domain/entity/study/vendor scope.
- Build/normalize column mappings.
- Parse transformation SQL logic (including dict-style standard/post-processing config).
- Apply cross-entity SQL enrichment where configured.
- Produce staging output consumed by DQ and write step.

## 8. Data Quality (DQ) Behavior

Module:

- `databricks_bundle/drugdev/silver_framework/silver_engine/dqx_validator.py`

DQ rule source:

- `validation_rules` JSON in `drugdev_silver_registry`

Supported check families include:

- `not_null`
- `unique`
- `in_list`
- `range`
- `cross_field`
- `date_format`
- `completeness`

Severity and controls:

- `CRITICAL` failures raise `DQCriticalFailureError` and prevent Silver write.
- Pass-rate threshold from config can trigger alerts.
- DQ results are written to `dq_validation_results_log`.

## 9. Governance and Tagging

Pipeline applies Unity Catalog tags on Silver tables after write:

- Table tags: `sensitivity`, `domain`, `layer=SILVER`, `data_product`
- Column tags for PII columns: `sensitivity`, `pii_category`

Source metadata:

- `table_sensitivity` and `pii_columns` from Silver registry row.

## 10. Security and Secrets

Multiple modules implement a shared secret-resolution pattern:

1. Read `aws_secret_name` from environment YAML.
2. Fetch JSON secret from AWS Secrets Manager (when available).
3. Fallback to `SECRET_<KEY>` environment variables in non-AWS contexts.

Secret token format supported in YAML text values:

- `{{SECRET:key_name}}`

## 11. End-to-End Runbook

### Step 1: Refresh Silver config YAML from Excel

```bash
python .\databricks_bundle\drugdev\silver_framework\configs\silver_yaml_generator.py \
  --vendor-catalog docs/silver_vendor_catalog.xlsx \
  --entity-level   docs/silver_entity_level.xlsx \
  --output         databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_config.yaml
```

### Step 2: Open and run notebook

- Notebook: `databricks_bundle/src/drugdev_silver_pipeline_standalone.ipynb`

Execute cells in order:

1. Environment and Spark setup
2. Metadata table ensure/create
3. Metadata loader execution
4. Metadata verification
5. Dependency check
6. Silver pipeline execution
7. Summary output

### Step 3: Validate outputs

Check:

- Registry population in `drugdev_silver_registry`
- DQ results in `dq_validation_results_log`
- Alerts (if any) in `alert_history`
- Final Silver table row counts from notebook summary cell

## 12. Common Failure Points and Troubleshooting

1. Missing or stale generated YAML
   - Symptom: metadata load fails or missing datasets
   - Fix: regenerate YAML from latest Excel files before notebook run

2. Missing Spark conf values
   - Symptom: module initialization raises `Missing required Spark conf`
   - Fix: ensure notebook environment cell is run before importing pipeline modules

3. Missing dependency metadata tables
   - Symptom: dependency check raises runtime error
   - Fix: create/populate required common tables (country/region, mapping tables)

4. Invalid JSON-like content in registry fields
   - Symptom: parse errors for keys/zorder/validation rules
   - Fix: validate Excel entries and regenerate YAML

5. DQ critical rule failures
   - Symptom: entity fails with `DQCriticalFailureError`
   - Fix: inspect `dq_validation_results_log`, correct source data or metadata rules, rerun

6. Optimize/ZORDER warnings
   - Symptom: ZORDER skipped due to missing stats
   - Fix: run ANALYZE/statistics workflow as needed, or tune zorder column list

## 13. Operational Recommendations

1. Keep Excel sheets under change control and peer review before regeneration.
2. Regenerate YAML as an explicit CI/CD step before Silver deployment/runs.
3. Add automated checks for required columns and duplicate dataset keys in Excel inputs.
4. Track domain/entity-level SLA with `pipeline_execution_metrics` and alert history.
5. Keep DQ rule ownership clear by vendor/entity and study-specific overrides.

## 14. Key Artifacts and Paths

- Standalone notebook:
  - `databricks_bundle/src/drugdev_silver_pipeline_standalone.ipynb`
- Config generator:
  - `databricks_bundle/drugdev/silver_framework/configs/silver_yaml_generator.py`
- Config output:
  - `databricks_bundle/drugdev/silver_framework/configs/drugdev_silver_config.yaml`
- Excel sources:
  - `docs/silver_vendor_catalog.xlsx`
  - `docs/silver_entity_level.xlsx`
- Metadata loader:
  - `databricks_bundle/drugdev/silver_framework/metadata_service/metadata_loader.py`
- Pipeline engine:
  - `databricks_bundle/drugdev/silver_framework/silver_engine/silver_pipeline.py`
- Transformer:
  - `databricks_bundle/drugdev/silver_framework/silver_engine/silver_transformer.py`
- DQ engine:
  - `databricks_bundle/drugdev/silver_framework/silver_engine/dqx_validator.py`
- SCD helper:
  - `databricks_bundle/drugdev/silver_framework/silver_engine/scd_type2_merger.py`

---

Document generated from repository code and configuration state as of 2026-08-03.