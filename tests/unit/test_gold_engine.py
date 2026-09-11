"""Unit tests for the Gold engine helper layer.

These cover the deterministic building blocks that the Gold pipeline relies on:
SQL token resolution, execution-wave grouping, registry/table naming, JSON
metadata parsing, error compaction, and PII mask-function resolution.
"""

import pytest

from conftest import FakeSparkConf, import_module_from_path


GOLD_ENGINE_PATH = r"databricks_bundle\drugdev\gold_framework\gold_engine\gold_engine.py"


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def collect(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _RecordingSpark:
    """Fake SparkSession that records SQL and answers ``current_user()``."""

    def __init__(self, conf_values=None, current_user="svc-gold@example.com"):
        self.conf = FakeSparkConf(conf_values)
        self.queries = []
        self.current_user = current_user

    def sql(self, query):
        self.queries.append(" ".join(query.split()))
        if "current_user()" in query and "SELECT" in query.upper():
            return _Result([{"current_user": self.current_user}])
        return _Result([])


@pytest.fixture
def gold(stub_pyspark, monkeypatch):
    module = import_module_from_path("gold_engine_unit", GOLD_ENGINE_PATH)
    spark = _RecordingSpark()
    monkeypatch.setattr(module, "spark", spark)
    module._recording_spark = spark
    return module


def _conf(gold):
    return gold._get_conf()


# ── Configuration ──────────────────────────────────────────────────────────────

def test_get_conf_collects_all_required_keys(gold):
    conf = _conf(gold)

    assert conf["CATALOG"] == "drugdev-catalog"
    assert conf["GOLD_SCHEMA"] == "gold_schema"
    assert conf["RUN_DATE"] == "20260901"


def test_get_conf_reports_every_missing_key_at_once(gold):
    gold.spark.conf.values["drugdev.GOLD_SCHEMA"] = ""
    gold.spark.conf.values["drugdev.SILVER_SCHEMA"] = ""

    with pytest.raises(ValueError) as excinfo:
        gold._get_conf()

    message = str(excinfo.value)
    assert "drugdev.GOLD_SCHEMA" in message
    assert "drugdev.SILVER_SCHEMA" in message


# ── SQL token resolution and naming ────────────────────────────────────────────

def test_resolve_sql_tokens_expands_every_placeholder(gold):
    conf = _conf(gold)

    sql = gold._resolve_sql_tokens(
        "SELECT * FROM {silver_schema}.dim_study "
        "JOIN {gold_schema}.fct_queries "
        "JOIN {metadata_schema}.audit "
        "JOIN {registry_schema}.gold_object_registry "
        "WHERE {catalog} IS NOT NULL AND {metadata_catalog} IS NOT NULL",
        conf,
    )

    assert "`drugdev-catalog`.silver_schema.dim_study" in sql
    assert "`drugdev-catalog`.gold_schema.fct_queries" in sql
    assert "`meta_catalog`.meta_schema.audit" in sql
    assert "`meta_catalog`.registry_schema.gold_object_registry" in sql
    assert "{" not in sql


def test_resolve_sql_tokens_passes_through_empty_templates(gold):
    assert gold._resolve_sql_tokens("", _conf(gold)) == ""
    assert gold._resolve_sql_tokens(None, _conf(gold)) is None


def test_table_name_helpers_use_the_expected_catalogs(gold):
    conf = _conf(gold)

    assert gold._gold_table_fqn(conf, "dim_study") == "`drugdev-catalog`.gold_schema.dim_study"
    assert (
        gold._gold_object_registry_tbl(conf)
        == "`meta_catalog`.registry_schema.gold_object_registry"
    )
    assert (
        gold._pipeline_metrics_tbl(conf)
        == "`drugdev-catalog`.meta_schema.pipeline_execution_metrics"
    )


# ── Execution waves ────────────────────────────────────────────────────────────

def test_group_by_execution_order_builds_ordered_waves(gold):
    rows = [
        {"object_name": "dim_study", "execution_order": 1},
        {"object_name": "dim_site", "execution_order": 1},
        {"object_name": "fct_queries", "execution_order": 2},
    ]

    waves = gold._group_by_execution_order(rows)

    assert [[r["object_name"] for r in wave] for wave in waves] == [
        ["dim_study", "dim_site"],
        ["fct_queries"],
    ]


def test_group_by_execution_order_pushes_unordered_objects_last(gold):
    rows = [
        {"object_name": "vw_late", "execution_order": None},
        {"object_name": "dim_study", "execution_order": 2},
    ]

    waves = gold._group_by_execution_order(rows)

    assert [r["object_name"] for r in waves[0]] == ["dim_study"]
    assert [r["object_name"] for r in waves[-1]] == ["vw_late"]


# ── Metadata parsing ───────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, []),
        ("", []),
        ('["study_id", "site_id"]', ["study_id", "site_id"]),
        ('{"not": "a list"}', []),
        ("not-json", []),
    ],
)
def test_parse_json_list_is_defensive(gold, value, expected):
    assert gold._parse_json_list(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, {}),
        ("", {}),
        ({"domain": "ctms"}, {"domain": "ctms"}),
        ('{"domain": "ctms"}', {"domain": "ctms"}),
        ("[1, 2]", {}),
        ("not-json", {}),
    ],
)
def test_parse_json_dict_is_defensive(gold, value, expected):
    assert gold._parse_json_dict(value) == expected


def test_sql_escape_and_principal_quoting_block_injection(gold):
    assert gold._sql_escape("O'Brien") == "O''Brien"
    assert gold._quote_principal_identifier("group`name") == "`group``name`"


# ── Error compaction ───────────────────────────────────────────────────────────

def test_compact_error_message_drops_jvm_stacktrace(gold):
    exc = RuntimeError("Table not found\n\nJVM stacktrace:\nat org.apache.spark.Foo")

    assert gold._compact_error_message(exc) == "Table not found"


def test_compact_error_message_joins_and_marks_long_multiline_errors(gold):
    exc = ValueError("line one\nline two\nline three\nline four")

    assert gold._compact_error_message(exc) == "line one | line two | line three | ..."


def test_compact_error_message_truncates_and_falls_back_to_class_name(gold):
    assert gold._compact_error_message(ValueError("")) == "ValueError"

    compacted = gold._compact_error_message(RuntimeError("x" * 900), max_chars=50)
    assert len(compacted) == 50
    assert compacted.endswith("...")


# ── PII masking ────────────────────────────────────────────────────────────────

def test_unmask_predicate_defaults_to_the_standard_group(gold):
    assert gold._build_unmask_predicate() == "is_account_group_member('pii_unmasked_access')"


def test_unmask_predicate_supports_groups_users_and_service_principals(gold):
    gold.spark.conf.values["drugdev.PII_UNMASK_GROUP"] = (
        "clinical_admins, group:dq_admins, user:alice@example.com, sp:app-1234, clinical_admins"
    )

    predicate = gold._build_unmask_predicate()

    assert predicate == (
        "is_account_group_member('clinical_admins') OR "
        "is_account_group_member('dq_admins') OR "
        "lower(current_user()) = lower('alice@example.com') OR "
        "lower(current_user()) = lower('app-1234')"
    )


def test_unmask_grantees_strip_prefixes_and_append_runtime_identity(gold):
    gold.spark.conf.values["drugdev.PII_UNMASK_GROUP"] = "group:dq_admins, sp:app-1234, dq_admins"

    assert gold._get_unmask_grantees() == ["dq_admins", "app-1234", "svc-gold@example.com"]


def test_grant_unmask_issues_one_statement_per_grantee(gold):
    gold.spark.conf.values["drugdev.PII_UNMASK_GROUP"] = "dq_admins"

    gold._grant_unmask_on_table("`drugdev-catalog`.gold_schema.dim_participant")

    grants = [q for q in gold.spark.queries if q.startswith("GRANT UNMASK")]
    assert grants == [
        "GRANT UNMASK ON TABLE `drugdev-catalog`.gold_schema.dim_participant TO `dq_admins`",
        "GRANT UNMASK ON TABLE `drugdev-catalog`.gold_schema.dim_participant "
        "TO `svc-gold@example.com`",
    ]


def test_resolve_mask_function_honours_explicit_override(gold):
    assert gold._resolve_mask_function(_conf(gold), "custom.mask_fn", "string") == "custom.mask_fn"
    assert gold.spark.queries == []


@pytest.mark.parametrize(
    ("dtype", "expected_suffix"),
    [
        ("string", "mask_pii_string"),
        ("varchar(64)", "mask_pii_string"),
        ("int", "mask_pii_int"),
        ("bigint", "mask_pii_bigint"),
        ("double", "mask_pii_double"),
        ("date", "mask_pii_date"),
        ("timestamp", "mask_pii_timestamp"),
    ],
)
def test_resolve_mask_function_creates_type_specific_defaults(gold, dtype, expected_suffix):
    fqn = gold._resolve_mask_function(_conf(gold), "", dtype)

    assert fqn == f"`meta_catalog`.meta_schema.{expected_suffix}"
    assert any(q.startswith(f"CREATE OR REPLACE FUNCTION {fqn}") for q in gold.spark.queries)


def test_resolve_mask_function_handles_decimal_precision_and_scale(gold):
    # Regression guard: decimal masking parses the type with `re`, which must be
    # imported by the module.
    fqn = gold._resolve_mask_function(_conf(gold), "", "decimal(18,2)")

    assert fqn == "`meta_catalog`.meta_schema.mask_pii_decimal_18_2"
    ddl = next(q for q in gold.spark.queries if "mask_pii_decimal_18_2" in q)
    assert "val DECIMAL(18,2)" in ddl


@pytest.mark.parametrize("dtype", ["decimal(0,0)", "decimal(2,5)", "array<string>", "", None])
def test_resolve_mask_function_returns_empty_for_unsupported_types(gold, dtype):
    assert gold._resolve_mask_function(_conf(gold), "", dtype) == ""


def test_string_mask_function_can_be_overridden_by_spark_conf(gold):
    gold.spark.conf.values["drugdev.PII_MASK_STRING_FUNCTION"] = "shared.masks.redact"

    assert gold._resolve_mask_function(_conf(gold), "", "string") == "shared.masks.redact"
    assert gold.spark.queries == []
