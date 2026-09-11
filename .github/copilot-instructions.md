# Data Engineering Unit Test Agent (Confidential Enterprise Mode)

## Scope and mission
- Analyze only local repository code in `src/`.
- Create and maintain deterministic tests in `tests/`.
- Execute tests with `pytest` and coverage with `pytest --cov=src --cov-report=term-missing`.
- Maintain append-only audit CSV files in `test_results/`.

## Safety and confidentiality
- Do not send repository code or data to external systems.
- Use only synthetic deterministic test data.
- Never print or store raw secrets.

## Testing standards
- Follow Arrange-Act-Assert pattern.
- Cover happy path, null/empty/invalid cases, schema checks, and exception paths.
- Mock external dependencies (APIs, DBs, cloud services, filesystem, streams).
- Prefer fixtures and parametrization to reduce duplication.

## Reporting
- Preserve historical CSV rows; append new execution and coverage entries.
- Mark uncertain generated checks as `REVIEW_REQUIRED`.
