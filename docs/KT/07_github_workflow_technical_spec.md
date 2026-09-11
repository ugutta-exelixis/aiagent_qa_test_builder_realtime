# GitHub Workflow - Technical Specification and KT Guide

## 1. Purpose
This document explains CI/CD behavior for Databricks deployment through GitHub Actions.

Workflow file:
- .github/workflows/ddda-clinical-platform-koios-DEV.yml

## 2. Triggering Conditions
The deployment job runs on:
- push to Koios-DEV-Master, Koios-UAT-Master, master
- merged pull_request targeting those branches

Conditional execution:
- deploy job runs only for push or merged PR.

## 3. Pipeline Stages
Main stages:
1. Checkout and Python setup.
2. Install Databricks CLI and jq.
3. Configure AWS credentials using OIDC role assumption.
4. Resolve environment/branch mapping to secret source and target env.
5. Fetch Databricks OAuth credentials and runtime defaults from Secrets Manager.
6. Sync Databricks Repo to branch.
7. Validate Databricks Asset Bundle with resolved vars.
8. Deploy Databricks Asset Bundle.
9. Verify SRC pipeline job exists and users permissions are present.

## 4. Branch-to-Environment Mapping
Branch mapping logic:
- Koios-DEV-Master -> target_env=dev
- Koios-UAT-Master -> target_env=uat
- master -> target_env=prod

Per branch, workflow sets:
- secrets manager source
- databricks branch and repo path
- cluster_id

## 5. Secret and Variable Handling
The workflow reads a secret JSON and extracts:
- Databricks host/client_id/client_secret
- SRC runtime defaults (bucket/schema/catalog/mode/object/etc.)

These values are passed as:
- bundle validate --var key=value
- bundle deploy --var key=value

Security features:
- client_id and client_secret are masked.
- AWS role is assumed at runtime (no static AWS key).

## 6. Databricks Validation and Deploy
Bundle commands are executed from repository path stored in secret Databricks_YML_Path:
- databricks bundle validate
- databricks bundle deploy

Critical deploy vars:
- cluster_id
- service_principal_client_id
- full set of SRC notebook runtime defaults

## 7. Post-Deploy Verification
Workflow verifies:
- SRC job with expected name exists in workspace.
- users group has at least CAN_VIEW/CAN_MANAGE_RUN/CAN_MANAGE/IS_OWNER.

If either check fails, workflow exits non-zero.

## 8. Failure and Recovery
Common failure categories:
- invalid/missing GitHub secret values
- AWS role assumption failure
- Databricks OAuth secret retrieval failure
- bundle validation errors
- missing repo path or wrong Databricks branch
- post-deploy permission mismatch

Recovery pattern:
1. Check failed step logs and branch mapping output.
2. Validate Secrets Manager payload schema keys.
3. Re-run workflow from same commit after secret/config fix.
4. If deploy succeeded but verify failed, remediate Databricks permissions and rerun verify.

## 9. KT Runbook
1. Review branch mapping and secret names with DevOps team.
2. Validate secret JSON fields expected by workflow.
3. Trigger test run on DEV branch.
4. Confirm bundle validate/deploy success and post-checks.
5. Document rollback/disable procedure for production safety.

## 10. Handover Checklist
- Team can explain every CI stage and env mapping.
- Team can update SRC default variables in secret JSON safely.
- Team can debug validate/deploy failures quickly.
- Team can confirm job existence and permissions after deployment.
