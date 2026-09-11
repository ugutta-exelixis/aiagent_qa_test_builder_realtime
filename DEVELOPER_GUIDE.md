# Developer Guide

This guide explains how to work in `dt-drugdevelopment-dna`, how to use the repository structure consistently, and how to onboard new studies, source systems, vendors, and workflows without breaking the platform contract.

## Purpose

The platform separates responsibilities across configuration, runtime code, SQL DDL, notebooks, and tests.

Use this guide when you need to:

- add or update a study intake form
- onboard a new vendor within an existing domain
- onboard a new source system or domain
- update governance and tagging metadata
- create or regenerate workflows for newly onboarded domains or vendors

## Repository Structure

```text
dt-drugdevelopment-dna/
├── config/
│   ├── cluster/                 # Cluster specs and environment runtime settings
│   ├── environments/            # Environment YAMLs used by config_loader
│   ├── intake_forms/            # One intake YAML per study
│   ├── vendor_catalog/          # Vendor-level schema and governance contracts
│   └── workflows/               # Generated Databricks workflow JSON artifacts
├── dab/                         # Databricks Asset Bundle definition
├── notebooks/                   # Notebook entry points used by jobs
├── scripts/                     # Deployment, setup, validation, and test helpers
├── sql/                         # Catalog/schema/table DDL and seed scripts
├── src/                         # Reusable Python modules for onboarding, ingestion, DQ, monitoring
└── tests/                       # Unit and integration tests
```

## Folder Usage Guidelines

### `config/`

This directory is the source of truth for environment-specific and onboarding-specific configuration.

- `config/environments/*.yaml`
  - Defines workspace host, catalog, buckets, schema names, registry table names, notifications, and onboarding behavior.
  - `src/utils/config_loader.py` reads these files and should remain the only place that builds catalog, schema, and table FQNs.
- `config/intake_forms/*.yaml`
  - One file per study.
  - Turns domains, vendors, and entities on or off for a study.
  - Holds study business metadata and study-specific operational settings.
- `config/vendor_catalog/<domain>/<vendor>.yaml`
  - Defines the vendor contract for a domain.
  - Owns canonical schema, delivery pattern, file matching, transformation logic, DQ rules, drift policy, and table sensitivity.
  - Do not put per-study logic here unless it is truly global for all studies using that vendor.
- `config/vendor_catalog/<domain>/<vendor>_study_overrides.yaml`
  - Holds study-specific rename maps and transformation overrides.
  - Use this when one study deviates from the vendor’s standard file layout.
- `config/workflows/*.json`
  - Generated artifacts only.
  - Do not edit manually.

### `dab/`

- `dab/databricks.yml` is the Databricks Asset Bundle entry point.
- Use it to define deployable jobs, artifact packaging, workspace root, and per-target overrides.
- Keep bundle workspace paths aligned with the actual Databricks workspace folder structure.

### `notebooks/`

- Notebook files are execution entry points for Databricks jobs.
- Keep heavy business logic in `src/` and keep notebooks thin.
- Use notebooks to orchestrate module calls, parameter passing, and runtime context.

### `scripts/`

- `scripts/setup/` contains setup and validation helpers.
- `scripts/deploy/` contains deployment-time generators and utilities.
- `scripts/testing/` contains developer-facing local validation tools.
- Prefer scripts for repeatable operational tasks instead of ad hoc notebook-only logic.

### `sql/`

- `sql/setup/` provisions catalogs, schemas, registries, and monitoring tables.
- `sql/silver/` and `sql/gold/` define platform-level downstream structures.
- Keep SQL idempotent where practical, and align schema names with `config/environments/*.yaml`.

### `src/`

- `src/onboarding/` handles intake processing, registry updates, and provisioning.
- `src/ingestion/` handles Bronze ingestion orchestration.
- `src/governance/` handles schema drift and metadata enforcement.
- `src/monitoring/` handles alerts and operational telemetry.
- `src/quality/` holds data quality validation logic.
- `src/utils/` contains reusable helpers such as config loading and SQL utilities.

### `tests/`

- Add unit tests for reusable Python logic under `tests/unit/`.
- Add integration tests when behavior spans Spark, tables, or multiple modules.
- Prefer targeted tests around onboarding, config parsing, drift policy, and DQ rule generation when making platform changes.

## Operating Model by Layer

Use the repository by responsibility, not by convenience.

| Concern | Correct location | Notes |
|---|---|---|
| Study-level enablement and ownership | `config/intake_forms/` | One YAML per study |
| Vendor-wide schema and file contract | `config/vendor_catalog/<domain>/<vendor>.yaml` | Applies to all studies for that vendor |
| Study-specific field mapping deltas | `config/vendor_catalog/<domain>/<vendor>_study_overrides.yaml` | Avoid duplicating vendor catalogs |
| Environment-specific catalog/bucket/schema settings | `config/environments/*.yaml` | Read through `config_loader.py` |
| Runtime ingestion or onboarding logic | `src/` | Avoid embedding logic in notebooks |
| Job deployment definition | `dab/databricks.yml` or generated workflow JSON | Keep generated JSON immutable |

## Naming Conventions

Follow the existing patterns consistently.

- **Studies**: use protocol-style identifiers such as `XB010-101`, `XL092-303`
- **Intake files**: `<STUDY_ID>_intake.yaml`
- **Vendor catalogs**: `<vendor>.yaml` under `config/vendor_catalog/<domain>/`
- **Study overrides**: `<vendor>_study_overrides.yaml`
- **Domains**: use uppercase intake keys such as `irt`, `edc`, `ctms`, `ipp`, which resolve to canonical domains in code
- **Bronze schemas**: `bronze_<domain>`
- **Silver schemas**: `silver_<domain>`
- **Workflow JSON**: `clinical_<domain>_<cadence>.json`
- **Bronze table names**: built by `build_bronze_table_name()` from domain, vendor, study, entity

## Governance and Tagging Guidelines

Governance metadata starts in the vendor catalog and is applied during onboarding and ingestion.

### Required Governance Fields in Vendor Catalogs

Each entity in a vendor catalog should define, where applicable:

- `table_sensitivity`
- `pii_columns`
- `canonical_cols`
- `drift_policy`
- `validation_rules`
- `scd_business_keys`
- `zorder_cols`

### Sensitivity Levels

Use the same sensitivity vocabulary already present in the repo:

- `INTERNAL`
  - Operational or business data with no direct sensitive subject/person data.
- `CONFIDENTIAL`
  - Sensitive study-linked data that should be protected but is not direct identifying information.
- `RESTRICTED`
  - Direct subject identifiers, sensitive demographic fields, clinical outcomes, or other highly sensitive elements.

### PII Categories

Use `pii_columns` to tag sensitive columns with a domain-appropriate category. Existing examples include:

- `SUBJECT_IDENTIFIER`
- `TREATMENT`
- `DEMOGRAPHIC`
- `CLINICAL_OUTCOME`

If you introduce a new category, keep it meaningful, stable, and reusable across vendors.

### Governance Rules

- Define `table_sensitivity` for every entity.
- Use `pii_columns: []` explicitly when an entity has no sensitive columns.
- Tag at the most specific level possible.
  - Table-level sensitivity is mandatory.
  - Column-level tagging is required when sensitivity differs within a table.
- Keep governance definitions in the vendor catalog, not in notebooks.
- Use study override files only for mapping or transformation differences, not for changing baseline governance unless the raw study delivery truly differs.
- Any new canonical column carrying subject, investigator, treatment, demographic, or clinical outcome meaning should be reviewed for sensitivity tagging.

### Drift Policy Guidance

Use `drift_policy` to describe what happens when upstream raw files change.

- `on_new_column: ADD` is appropriate when non-breaking additions are expected.
- Use `on_modified: REJECT` for identifiers and critical keys.
- Use `on_deleted: REJECT` for required business keys and mandatory reporting columns.
- Use `ALERT` or `NULL_FILL` only when downstream consumers can tolerate the change.

For critical identity columns such as `study_id`, `subject_number`, and primary business keys, default toward stricter behavior.

## How to Create a New Study Intake YAML

Use `config/intake_forms/study_intake.yaml` as the template pattern, then create a study-specific file named `config/intake_forms/<STUDY_ID>_intake.yaml`.

### What the Intake YAML Owns

The intake file should define:

- study metadata
- enabled domains for the study
- enabled vendors within each domain
- enabled entities within each vendor
- study-specific contacts
- approval status
- any business-rule values such as `enrollment_statuses`

### Intake Authoring Rules

- Create one intake file per study.
- Only enable domains and vendors that are actually approved for onboarding.
- Only reference vendors that already exist in `config/vendor_catalog/<domain>/`.
- `study_overrides_ref` should point to the vendor study overrides file without the `.yaml` extension, matching existing examples.
- Keep entity names aligned exactly with the vendor catalog entity names.
- Set `enabled: false` rather than removing unused domains or vendors; this keeps the file explicit.
- Keep dates in a consistent string format such as `YYYY-MM-DD`.

### Recommended Intake Template

```yaml
study_id:          "<STUDY_ID>"
study_name:        "<Display Name>"
study_phase:       "<Phase>"
therapeutic_area:  "<Therapeutic Area>"
sponsor:           "Exelixis"
planned_start:     "YYYY-MM-DD"

data_sources:

  edc:
    enabled: true
    vendors:
      - vendor_catalog:      edc/<vendor_name>
        study_overrides_ref: edc/<vendor_name>_study_overrides
        enabled: true
        entities:
          - { name: <entity_name>, enabled: true }

  ctms:
    enabled: false

  irt:
    enabled: false

  ipp:
    enabled: false

enrollment_statuses: [Enrolled, Screened, "Screen Failure", Discontinued, Completed]

contacts:
  clinical_ops_lead: clinical-ops@exelixis.com
  data_manager:      data-management@exelixis.com
  requested_by:      data-engineering@exelixis.com
  requested_date:    "YYYY-MM-DD"

approval:
  status:        APPROVED
  approved_by:   de-lead@exelixis.com
  approved_date: "YYYY-MM-DD"
```

### Intake Validation Checklist

Before committing a new intake file, verify:

- the study ID matches the filename
- every `vendor_catalog` path exists
- every entity exists in the referenced vendor catalog
- the `study_overrides_ref` matches an existing overrides file when used
- governance-sensitive entities are enabled intentionally
- contacts and approval metadata are complete

For local validation, use `scripts/testing/run_intake_engine.py` when your local environment supports the required dependencies.

## How to Onboard a New Vendor Within an Existing Domain

Use this path when the domain already exists, such as adding a new EDC or CTMS vendor.

### Step 1: Create the Vendor Catalog

Add a file at:

- `config/vendor_catalog/<domain>/<vendor>.yaml`

Define the following at minimum:

- vendor metadata: `vendor_name`, `path_type`, `delivery_method`, `delivery_schedule`
- file discovery settings: `date_regex`, `date_format`, `file_name_pattern_template`
- entity list with one block per delivered file/entity
- canonical schema and transformation logic
- governance and DQ metadata
- drift policy and performance hints such as `zorder_cols`

### Step 2: Create Study Overrides File

Add:

- `config/vendor_catalog/<domain>/<vendor>_study_overrides.yaml`

Even if most studies have no overrides, create the file so future study-specific deltas have a canonical home.

### Step 3: Register the Vendor Operationally

Make sure the vendor is represented in the metadata/registry layer used by runtime logic.

This typically means ensuring records exist for:

- `vendor_config`
- `vendor_schedule`

The vendor must have a valid:

- domain assignment
- path type and delivery method
- S3 prefix
- date extraction settings
- active schedule entry if it should run automatically

### Step 4: Update Study Intake Files

For each study using the vendor:

- add the vendor under the appropriate domain in `config/intake_forms/<STUDY_ID>_intake.yaml`
- reference the vendor catalog path and study overrides file
- enable only approved entities

### Step 5: Validate End-to-End

Validate these concerns before deployment:

- latest sample/raw files match the discovery pattern
- canonical columns cover required business outputs
- DQ rules and sensitivity tags are complete
- schema drift actions are safe for critical fields
- workflow scheduling exists if the vendor should run automatically

## How to Onboard a New Source System or Domain

Use this path when the source belongs to a new domain that does not yet exist in the platform.

Examples of domain concepts in this repo include `IRT`, `EDC`, `CTMS`, and `Planisware`.

### Step 1: Define the Domain in Environment Config

Update `config/environments/<env>.yaml` with:

- a Bronze schema under `bronze_schemas`
- a Silver schema under `silver_schemas`
- a domain alias if intake forms use a different key than the canonical runtime name

Example pattern:

```yaml
bronze_schemas:
  NEWDOMAIN: bronze_newdomain

silver_schemas:
  NEWDOMAIN: silver_newdomain
```

### Step 2: Provision SQL Objects

Update the setup SQL so the new schemas and any required domain-specific tables exist.

Check these locations:

- `sql/setup/01_create_catalogs_schemas.sql`
- `sql/silver/01_create_silver_tables.sql`
- `sql/gold/01_create_dimension_tables.sql`
- `sql/gold/02_create_fact_tables.sql`

Only add downstream tables that are actually needed for the new domain’s reporting use cases.

### Step 3: Add Runtime Support

If the new domain requires custom behavior, update the relevant modules in `src/`.

Common touch points:

- `src/utils/config_loader.py`
- `src/onboarding/intake_engine.py`
- `src/ingestion/`
- `src/transformation/`

Prefer extending shared patterns rather than branching logic unnecessarily.

### Step 4: Create Vendor Catalogs for the Domain

Add the new domain folder under:

- `config/vendor_catalog/<new_domain>/`

Then create at least one vendor catalog and optional study overrides file.

### Step 5: Register Domain Metadata

Make sure the new domain is reflected in the operational metadata tables and any setup or seed SQL that populates them.

The domain should be discoverable by runtime code that uses domain registry and schedule metadata.

### Step 6: Create Workflow Support

Add or generate a workflow definition for the new domain so ingestion and transformation can run on schedule.

See the workflow guidelines below.

## Guidelines for Vendor Catalog Authoring

When creating or editing `config/vendor_catalog/<domain>/<vendor>.yaml`, use this checklist.

- **File contract**: define `path_type`, `date_regex`, `date_format`, and `file_name_pattern_template` that match real delivered files.
- **Entity completeness**: create one entity block per inbound dataset.
- **Canonical schema**: use stable business-facing canonical column names in `canonical_cols`.
- **Transformations**: put reusable SQL in `transformation_logic`; keep it canonical and vendor-wide.
- **Study deltas**: put study-specific field renames in `<vendor>_study_overrides.yaml`, not in the vendor catalog.
- **DQ rules**: define explicit `rule_id`, `severity`, `mode`, and meaningful descriptions.
- **Governance**: set `table_sensitivity` and `pii_columns` intentionally for each entity.
- **Performance**: define `scd_business_keys` and `zorder_cols` that match query patterns.

## Guidelines for Study Overrides Files

Use `<vendor>_study_overrides.yaml` for controlled per-study exceptions only.

Good uses:

- a study delivers a column with a different raw name
- a study needs one additional transformation for the same canonical output
- a study omits a non-critical field that can be filled safely

Avoid using overrides files to:

- redefine the full canonical schema
- duplicate the vendor catalog logic
- hide upstream data quality issues that should be corrected at the source

## Workflow Guidelines for New Onboarded Vendors

The repo already treats workflow JSON under `config/workflows/` as generated output.

### Core Principles

- Do not hand-edit generated JSON files.
- Workflows should be domain-oriented, not vendor-oriented, unless there is a compelling operational reason to isolate a vendor.
- Vendor-specific scheduling belongs in registry/config metadata that the workflow generator reads.
- Notebook parameters should carry the domain and runtime context, not hardcoded vendor lists.

### Existing Workflow Pattern

Current jobs generally follow this sequence:

1. file tracking
2. Bronze ingestion for a domain
3. Silver transformation for a domain
4. optional Gold pipeline execution

### For a Newly Onboarded Vendor in an Existing Domain

Usually you should:

- add or update `vendor_config`
- add or update `vendor_schedule`
- regenerate workflows through the repository’s workflow generation process

You normally do not need a brand-new workflow JSON just because one more vendor is added to an existing domain.

### For a Newly Onboarded Domain

Create or generate a new workflow artifact that:

- follows the `clinical_<domain>_<cadence>.json` naming pattern
- uses the correct domain in task names and notebook parameters
- points to the correct notebook entry points
- inherits cluster and environment settings from `dab/databricks.yml` or the generation mechanism

### Workflow Authoring Checklist

- use the correct domain parameter value expected by runtime code
- point notebook paths to actual workspace notebook locations
- keep schedules aligned with `vendor_schedule` metadata when generation is metadata-driven
- reuse shared cluster definitions when the workload profile matches
- include timeouts for long-running Bronze and Silver tasks
- keep workflow names environment-aware, such as `[${var.env}]`

## Databricks Bundle and Workspace Path Guidelines

`dab/databricks.yml` should stay aligned with the actual Databricks workspace repository layout.

Use these rules:

- `workspace.root_path` should reflect the real root folder used in Databricks Workspace.
- `platform_config` variables should point to the actual deployed `config/environments/<env>.yaml` path under that workspace root.
- notebook paths in bundle jobs must match the real notebook location in Databricks Workspace.
- when repo or workspace folder names change, update bundle paths consistently instead of patching only one job.

## Recommended Developer Workflow

When onboarding anything new, work in this order:

1. update configuration contracts first
2. add or adjust runtime code only if the contract requires new behavior
3. validate onboarding locally where possible
4. update workflow metadata or generator inputs
5. deploy via the Databricks bundle or the repo’s deployment process
6. verify catalog objects, schedules, and monitoring outputs after deployment

## Pre-Commit Checklist for Pull Requests

Before opening a PR, confirm:

- new files are in the correct folder and follow naming conventions
- no generated workflow JSON was hand-edited without updating the generator path/process
- vendor catalogs include governance and DQ metadata
- study intake references valid vendor and override paths
- environment config changes are mirrored across relevant targets where needed
- tests were added or updated for changed Python behavior
- documentation is updated when introducing a new domain, vendor, or onboarding rule

## Quick Reference

- **New study**: add `config/intake_forms/<STUDY_ID>_intake.yaml`
- **New vendor in existing domain**: add vendor catalog, overrides file, registry metadata, and intake references
- **New domain/source system**: add schemas, metadata, runtime support, vendor catalog folder, and workflow support
- **Workflow changes**: update generator inputs or bundle definitions; avoid manual edits to generated workflow JSON
- **Governance changes**: update `table_sensitivity`, `pii_columns`, and drift/DQ policy in vendor catalogs
