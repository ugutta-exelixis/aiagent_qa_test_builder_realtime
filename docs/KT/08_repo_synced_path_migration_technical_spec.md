# Repo-Synced Notebook Path Migration - Technical Specification and Decision Guide

## 1. Purpose
This document explains:
- the current implementation where Databricks Asset Bundle deploys notebooks/jobs under `/Workspace/Shared/...`
- why this exists even though a Databricks Repo is already synced by GitHub Actions
- the implementation steps required to stop using Shared notebook paths and use the synced Databricks Repo path instead
- pros, cons, risks, and decision criteria

Scope:
- SRC pipeline job deployment and runtime notebook path resolution
- GitHub workflow and DAB configuration changes
- operational controls for DEV/UAT/PROD migration

Out of scope:
- business logic changes inside Bronze/Silver/Gold notebook code
- schema/model changes
- cluster sizing and performance tuning unrelated to path migration

## 2. Current Implementation (As-Is)

### 2.1 CI/CD Flow Summary
Current GitHub workflow performs these major actions:
1. Checkout repository and install Databricks CLI.
2. Resolve branch -> target environment.
3. Read Databricks and runtime settings from AWS Secrets Manager.
4. Sync Databricks Repo to the branch.
5. Run `databricks bundle validate`.
6. Run `databricks bundle deploy`.
7. Verify deployed SRC job and permissions.

Important observation:
- Step 4 (Databricks Repo sync) and steps 5-6 (bundle deploy) are independent flows.
- Bundle deployment currently points job notebook paths to files under the bundle workspace root in Shared.

### 2.2 Active Path Model
Current bundle root:
- `/Workspace/Shared/ddda-clinical-platform/${bundle.target}`

Current source notebook base variable:
- `${workspace.file_path}/databricks_bundle/src`

Resulting job notebook paths at deploy time:
- `/Workspace/Shared/ddda-clinical-platform/<env>/databricks_bundle/src/drugdev_data_extraction_workflow`
- `/Workspace/Shared/ddda-clinical-platform/<env>/databricks_bundle/src/drugdev_silver_pipeline_workflow`
- `/Workspace/Shared/ddda-clinical-platform/<env>/databricks_bundle/src/drugdev_gold_pipeline_workflow`
- `/Workspace/Shared/ddda-clinical-platform/<env>/databricks_bundle/src/drugdev_pipeline_summary_standalone`

### 2.3 Why Shared Was Chosen
The bundle config comments explicitly indicate:
- avoid development mode deployment to SP home paths (`~/.bundle/...`)
- keep deployed job visible/manageable for workspace users
- use a non-personal shared path for CI/CD ownership and discoverability

Net effect:
- Databricks Repo sync exists, but jobs do not execute notebooks directly from `/Workspace/Repos/...`.

## 3. Problem Statement
You already maintain a Databricks Repo clone that is branch-synced by GitHub workflow. Maintaining an additional synced notebook copy under Shared can feel redundant and may create confusion on source-of-truth.

Key question:
- Should job tasks resolve notebooks from the existing synced Repo path instead of Shared path artifacts from bundle sync?

## 4. Target State (To-Be)

### 4.1 Desired Behavior
- Keep GitHub workflow repo sync step.
- Keep bundle deployment for job object lifecycle (create/update job, parameters, permissions, schedule).
- Change notebook task paths to `/Workspace/Repos/.../databricks_bundle/src/...`.
- Optionally disable bundle notebook file sync to Shared for this job.

### 4.2 Canonical Repo Path Principle
For stability, define one canonical repo path per environment and use it consistently in:
- workflow secrets
- bundle variables
- optional pre-deploy validation checks
- operational runbooks

Example canonical style:
- `/Workspace/Repos/<repo-owner-or-service-principal>/dt-drugdevelopment-dna/databricks_bundle/src`

## 5. Migration Options

### Option A: Full Repo-Path Runtime (Recommended if branch discipline is strong)
- Jobs run notebooks from Repos path only.
- Shared path no longer used for SRC notebook tasks.

### Option B: Hybrid with Controlled Fallback
- Keep both variables available.
- Default to Repos path.
- Retain Shared path as emergency fallback toggle.

### Option C: Keep Current Shared Runtime
- No migration.
- Continue using synced Shared deployment as runtime source.

## 6. Detailed Implementation Steps (Repo-Path Runtime)

## 6.1 Pre-Migration Readiness Checks
1. Confirm Databricks Repo path exists in each env workspace.
2. Confirm service principal used by CI can read notebooks under that Repo path.
3. Confirm branch mapping is correct for DEV/UAT/PROD and repo sync is successful.
4. Confirm current jobs are healthy before migration (baseline).

## 6.2 Configuration Changes in DAB
1. Add new variable in `dab/databricks.yml`:
   - `repo_nb_base`
   - default to canonical Repos src path
2. Update job task notebook paths to use `${var.repo_nb_base}` instead of `${var.src_nb_base}`.
3. Keep `src_nb_base` temporarily for rollback window (optional).
4. Keep bundle `workspace.root_path` for job metadata deployment unless you intentionally want full repo-managed deployment artifacts.

Notes:
- Bundle can still deploy job definitions even when notebooks are referenced from Repos path.
- This decouples job object lifecycle from notebook file location.

## 6.3 GitHub Workflow Changes
1. Keep `Sync Databricks Repo` step.
2. Add pre-deploy validation command to ensure expected notebooks exist in repo path:
   - use `databricks workspace get-status` for each required notebook path.
3. Pass `repo_nb_base` as `--var` during both `bundle validate` and `bundle deploy`.
4. Optionally make `repo_nb_base` environment-specific from branch mapping.

## 6.4 Workflow Config and Documentation Updates
1. Update `config/workflows/clinical_platform_pipeline_src.yaml` references from Shared to Repos path examples.
2. Update KT docs:
   - workflow/pipeline technical spec
   - GitHub workflow technical spec
3. Add clear source-of-truth statement:
   - "Runtime notebooks are executed from Databricks Repo path."

## 6.5 Rollout Strategy
1. DEV dry run deployment using repo paths.
2. Execute one full pipeline run with normal parameters.
3. Execute one targeted run (for focused Silver/Gold modes).
4. Validate outputs and alerts.
5. Promote to UAT.
6. Promote to PROD.

## 7. Validation Checklist After Migration
1. `databricks bundle validate` passes with `repo_nb_base`.
2. Deployed job task notebook paths point to `/Workspace/Repos/...`.
3. Databricks job run starts all tasks without "notebook not found" errors.
4. Bronze, Silver, Gold, Summary complete with expected statuses.
5. users group permissions remain unchanged.
6. Rollback variable switch is tested once.

## 8. Pros and Cons Analysis

### 8.1 Pros of Repo-Path Runtime
1. Single notebook source used at runtime.
2. Better traceability to branch/commit semantics of Databricks Repos.
3. Reduced conceptual duplication between "synced repo" and "shared copy".
4. Faster troubleshooting for path-related source drift.
5. Cleaner developer mental model when debugging notebook behavior.

### 8.2 Cons of Repo-Path Runtime
1. Runtime dependency on Repo sync health each deployment.
2. Access model can be stricter than Shared; SP permissions must be correct.
3. Repo path naming and ownership differences across environments can break portability.
4. If repo sync is delayed or branch mapping is wrong, jobs may run stale code.
5. Some teams prefer Shared deployment for centrally controlled immutable deploy artifacts.

### 8.3 Pros of Keeping Shared Runtime
1. Deterministic deployed artifact path controlled by bundle target.
2. Easier cross-user visibility in shared workspace tree.
3. Less coupling between Databricks Repos behavior and job runtime.
4. Lower risk from repo ownership/path variations.

### 8.4 Cons of Keeping Shared Runtime
1. Apparently duplicated sync concepts (repo plus shared synced files).
2. Potential confusion around actual runtime source notebooks.
3. Extra documentation burden to explain two parallel paths.

## 9. Risk Register and Mitigations

1. Risk: Notebook not found at Repos path.
   Mitigation: Add pre-deploy get-status checks for all 4 SRC notebooks.

2. Risk: Service principal cannot read Repos notebooks.
   Mitigation: Verify repo ACL and run_as SP access before cutover.

3. Risk: Wrong branch synced for environment.
   Mitigation: Explicit branch mapping validation and log output in workflow.

4. Risk: Path mismatch across environments.
   Mitigation: Parameterize `repo_nb_base` per target and store in env secrets/config.

5. Risk: Hidden stale code execution.
   Mitigation: Add post-sync verification and optional commit hash capture in run logs.

## 10. Rollback Plan
1. Keep `src_nb_base` variable and Shared notebook paths available for one release window.
2. If failures occur, redeploy bundle with `src_nb_base`-based paths (or set `repo_nb_base` to current Shared equivalent if templated).
3. Re-run failed job from first failed task.
4. Capture incident notes and root cause before retrying migration.

## 11. Decision Matrix

Use Repo-path runtime when:
- team wants one runtime source aligned with synced repo branch
- repo permissions are stable for CI service principal
- branch governance is strict

Keep Shared runtime when:
- team prioritizes centralized shared deploy artifacts
- repo ACL/path ownership varies by environment
- operational model values decoupling from repo sync state

## 12. Suggested Execution Plan for This Repository
1. Implement Option B (Hybrid) first for safe cutover.
2. Add `repo_nb_base` variable and update task notebook paths to use it.
3. Keep `src_nb_base` for rollback during first 2 release cycles.
4. Add workflow pre-checks for repo notebook existence.
5. Run DEV -> UAT -> PROD promotion with explicit validation checkpoints.
6. After stabilization, remove Shared fallback references from docs and config.

## 13. Concrete File Change Map (When You Execute Migration)
1. `dab/databricks.yml`
   - add `repo_nb_base` variable
   - change SRC task notebook_path values
2. `.github/workflows/ddda-clinical-platform-koios-DEV.yml`
   - add repo notebook existence validation
   - pass `repo_nb_base` in validate/deploy vars
3. `config/workflows/clinical_platform_pipeline_src.yaml`
   - update example notebook paths for consistency
4. `docs/KT/06_databricks_workflow_pipeline_technical_spec.md`
   - update deployment model section
5. `docs/KT/07_github_workflow_technical_spec.md`
   - update CI/CD flow and checks

## 14. Acceptance Criteria
Migration is complete when all are true:
1. Production job task notebook paths resolve to `/Workspace/Repos/...`.
2. GitHub workflow sync and deploy succeeds on all three branches.
3. One full successful run is observed per environment.
4. No permission regressions for users group visibility/run access.
5. Rollback procedure is tested and documented.

## 15. Final Recommendation
Adopt Hybrid first, then move to Full Repo-Path Runtime after 2 stable release cycles.

Reason:
- You get the simplified runtime source model you want.
- You retain controlled rollback and avoid high-impact outages from path/ACL drift during first cutover.
- You can remove Shared dependency incrementally with measurable risk control.
