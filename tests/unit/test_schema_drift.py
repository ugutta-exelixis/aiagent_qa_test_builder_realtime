"""Unit tests for Bronze ingestion schema-drift detection.

`schema_registry.check_and_update_schema` is the guard that keeps the Bronze
layer honest: it compares the incoming DataFrame column list against the last
active schema version and, when they differ, closes the previous version and
registers a new one. These tests pin that contract without a live cluster.
"""

import json

import pytest

from conftest import import_module_from_path


SCHEMA_REGISTRY_PATH = (
    r"databricks_bundle\drugdev\ingestion_framework\ingestion_engine\schema_registry.py"
)


class _FakeResult:
    """Stand-in for a Spark DataFrame returned by ``spark.sql``."""

    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return list(self._rows)


class _RecordingSpark:
    """Records every SQL statement and replays a scripted registry lookup."""

    def __init__(self, registry_rows=None):
        self.queries = []
        self._registry_rows = registry_rows or []

    def sql(self, query):
        self.queries.append(" ".join(query.split()))
        if "SELECT schema_json" in query:
            return _FakeResult(self._registry_rows)
        return _FakeResult([])


class _FakeDataFrame:
    def __init__(self, columns):
        self.columns = list(columns)


class _RecordingLogger:
    def __init__(self):
        self.messages = []

    def info(self, message, *args):
        self.messages.append(message % args if args else message)

    warning = info
    error = info


def _load_schema_registry(name):
    return import_module_from_path(name, SCHEMA_REGISTRY_PATH)


def test_schema_registry_table_is_built_from_spark_conf(stub_pyspark):
    registry = _load_schema_registry("bronze_schema_registry_table")

    assert registry.SCHEMA_REGISTRY_TBL == "`meta_catalog`.meta_schema.schema_registry"


def test_missing_metadata_conf_blocks_module_import(stub_pyspark):
    # Bronze ingestion must fail loudly at startup rather than silently writing
    # schema versions into a wrong (or default) catalog.
    stub_pyspark.conf.values["drugdev.METADATA_CATALOG"] = ""

    with pytest.raises(ValueError, match="drugdev.METADATA_CATALOG"):
        _load_schema_registry("bronze_schema_registry_missing_conf")


def test_first_time_dataset_registers_schema_version_one(stub_pyspark):
    registry = _load_schema_registry("bronze_schema_registry_first_version")
    spark = _RecordingSpark(registry_rows=[])
    logger = _RecordingLogger()

    registry.check_and_update_schema(
        spark, "ds-001", _FakeDataFrame(["study_id", "site_id"]), logger
    )

    select_sql, update_sql, insert_sql = spark.queries
    assert "SELECT schema_json, version" in select_sql
    assert "dataset_id = 'ds-001'" in select_sql
    # Even the first version deactivates prior rows so the registry can never
    # end up with two active versions for one dataset.
    assert update_sql.startswith("UPDATE `meta_catalog`.meta_schema.schema_registry")
    assert "SET is_active = false" in update_sql
    assert "'[\"site_id\", \"study_id\"]'" in insert_sql
    assert " 1, true," in insert_sql
    assert any("Version 1" in msg for msg in logger.messages)


def test_unchanged_schema_short_circuits_without_writes(stub_pyspark):
    registry = _load_schema_registry("bronze_schema_registry_no_drift")
    existing = json.dumps(["site_id", "study_id"])
    spark = _RecordingSpark(
        registry_rows=[{"schema_json": existing, "version": 3}]
    )
    logger = _RecordingLogger()

    registry.check_and_update_schema(
        spark, "ds-001", _FakeDataFrame(["study_id", "site_id"]), logger
    )

    # Only the lookup runs — no UPDATE/INSERT churn on an unchanged schema.
    assert len(spark.queries) == 1
    assert logger.messages == ["No schema change for ds-001"]


def test_column_order_alone_is_not_treated_as_drift(stub_pyspark):
    registry = _load_schema_registry("bronze_schema_registry_order")
    existing = json.dumps(["a_col", "b_col", "c_col"])
    spark = _RecordingSpark(
        registry_rows=[{"schema_json": existing, "version": 1}]
    )

    registry.check_and_update_schema(
        spark, "ds-002", _FakeDataFrame(["c_col", "a_col", "b_col"]), _RecordingLogger()
    )

    assert len(spark.queries) == 1


def test_added_column_bumps_version_and_deactivates_previous(stub_pyspark):
    registry = _load_schema_registry("bronze_schema_registry_drift")
    existing = json.dumps(["site_id", "study_id"])
    spark = _RecordingSpark(
        registry_rows=[{"schema_json": existing, "version": 4}]
    )
    logger = _RecordingLogger()

    registry.check_and_update_schema(
        spark,
        "ds-003",
        _FakeDataFrame(["study_id", "site_id", "enrollment_date"]),
        logger,
    )

    _, update_sql, insert_sql = spark.queries
    assert "SET is_active = false" in update_sql
    assert "dataset_id = 'ds-003'" in update_sql
    assert '\'["enrollment_date", "site_id", "study_id"]\'' in insert_sql
    assert " 5, true," in insert_sql
    assert any("Version 5" in msg for msg in logger.messages)


def test_single_quotes_in_column_names_are_sql_escaped(stub_pyspark):
    registry = _load_schema_registry("bronze_schema_registry_escaping")
    spark = _RecordingSpark(registry_rows=[])

    registry.check_and_update_schema(
        spark, "ds-004", _FakeDataFrame(["patient's_id"]), _RecordingLogger()
    )

    insert_sql = spark.queries[-1]
    assert "patient''s_id" in insert_sql
    assert "patient's_id" not in insert_sql
