"""Integration tests for the Silver pipeline.

The Silver layer is a three-module collaboration: ``silver_transformer`` reads
the registry contract, ``dqx_validator`` gates the data, and ``silver_pipeline``
orchestrates and governs the output.  These tests load the real modules together
and drive them through a scripted Spark session so the contracts *between* the
modules (registry JSON shape, DQ result rows, governance DDL) are verified.
"""

import json

import pytest

from harness import import_module_from_path, install_pyspark_stub


SILVER_ENGINE_DIR = (
    "databricks_bundle/drugdev/silver_framework/silver_engine"
)
SILVER_PIPELINE_PATH = SILVER_ENGINE_DIR + "/silver_pipeline.py"

_SIBLING_MODULES = (
    "silver_transformer",
    "dqx_validator",
    "sql_utils",
    "sanitize",
    "alert_notifier",
)


def _load_silver(monkeypatch, conf_values=None):
    """Install the Spark double, then import the whole silver_engine package."""
    from harness import REPO_ROOT

    spark = install_pyspark_stub(monkeypatch, conf_values)
    module = import_module_from_path(
        "silver_pipeline_it",
        SILVER_PIPELINE_PATH,
        extra_sys_path=[REPO_ROOT / SILVER_ENGINE_DIR],
        purge=_SIBLING_MODULES,
    )
    return module, spark


@pytest.fixture
def silver(monkeypatch):
    return _load_silver(monkeypatch)


SILVER_TABLE = "`drugdev-catalog`.silver_schema.silver_clinical_site"


# ── Registry contract: transformer reads what the pipeline writes ──────────────

def test_registry_rows_are_parsed_into_the_transformer_contract(silver):
    pipeline_mod, spark = silver
    transformation_logic = repr(
        {
            "standard": "SELECT `site id` AS site_id, `Country` AS country FROM src",
            "post_processing": "dedupe:\n  keys:\n    - site_id\n",
        }
    )
    spark.on(
        "FROM `drugdev-catalog`.registry_schema.drugdev_silver_registry",
        [
            {
                "vendor": "Veeva",
                "study_id": "XL092-303",
                "source_dataset_name": "veeva_site_extract",
                "mapping_rules": json.dumps({"site id": "site_id", "Country": "country"}),
                "transformation_logic": transformation_logic,
                "cross_entity_sql": None,
                "canonical_cols": json.dumps(["site_id", "country", "study_id"]),
            }
        ],
    )
    transformer = pipeline_mod.SilverTransformer(spark)

    rows = transformer._get_registry_rows("clinical", "site", study_id="XL092-303")

    assert len(rows) == 1
    row = rows[0]
    assert row["_mapping_rules"] == {"site id": "site_id", "Country": "country"}
    assert row["_standard_sql"].startswith("SELECT `site id` AS site_id")
    assert row["_post_processing"] == {"dedupe": {"keys": ["site_id"]}}
    assert row["_canonical_cols"] == ["site_id", "country", "study_id"]

    query = spark.queries_matching("drugdev_silver_registry")[0]
    assert "WHERE domain = 'clinical'" in query
    assert "AND entity = 'site'" in query
    assert "AND is_active = TRUE" in query
    assert "AND study_id = 'XL092-303'" in query
    assert query.endswith("ORDER BY study_id, vendor")


def test_registry_vendor_maps_onto_the_bronze_table_the_transformer_reads(silver):
    pipeline_mod, spark = silver
    spark.on(
        "drugdev_silver_registry",
        [
            {
                "vendor": "Veeva Vault",
                "study_id": "XL092-303",
                "source_dataset_name": "veeva_site_extract",
                "mapping_rules": None,
                "transformation_logic": None,
                "cross_entity_sql": None,
                "canonical_cols": None,
            }
        ],
    )
    transformer = pipeline_mod.SilverTransformer(spark)
    row = transformer._get_registry_rows("clinical", "site")[0]

    bronze = pipeline_mod.build_bronze_table_name(
        "clinical", row["vendor"], row["study_id"], "site"
    )

    assert bronze == "`drugdev-catalog`.bronze_schema.veevavault_xl092_303_site"
    assert row["_mapping_rules"] == {}
    assert row["_standard_sql"] == ""
    assert row["_canonical_cols"] == []


# ── DQ validation end to end ──────────────────────────────────────────────────

def _script_dq(spark, rules, total_rows=10, distinct=10, in_list_failures=1, duplicates=0,
               null_count=0):
    spark.on(
        "SELECT validation_rules",
        [{"validation_rules": json.dumps({"rules": rules})}],
    )
    spark.on("GROUP BY", [{"c": duplicates}])
    spark.on("COUNT(DISTINCT", [{"c": distinct}])
    spark.on(
        lambda q: "NOT IN (" in q and "COUNT(*) AS c" in q,
        [{"c": in_list_failures}],
    )
    spark.on(
        lambda q: q.startswith("SELECT DISTINCT") and "NOT IN (" in q,
        [{"status": "PENDING"}],
    )
    spark.on(
        lambda q: "COUNT(*) AS c" in q and "IS NULL" in q,
        [{"c": null_count}],
    )
    return spark.dataframe(
        [{"subject_id": f"S{i}", "status": "ACTIVE", "study_id": "XL092-303"} for i in range(total_rows)]
    )


RULES = [
    {"rule_id": "R_SUBJECT_NOT_NULL", "check": "not_null", "field": "subject_id", "severity": "HIGH"},
    {"rule_id": "R_SUBJECT_UNIQUE", "check": "unique", "field": "subject_id", "severity": "HIGH"},
    {
        "rule_id": "R_STATUS_IN_LIST",
        "check": "in_list",
        "field": "status",
        "values": ["ACTIVE", "INACTIVE"],
        "severity": "MEDIUM",
    },
    {
        "rule_id": "R_COMPOUND_UNIQUE",
        "check": "compound_unique",
        "field": "['study_id', 'subject_id']",
        "severity": "LOW",
    },
]


def test_dq_validation_runs_every_registry_rule_and_summarises_them(silver):
    pipeline_mod, spark = silver
    df = _script_dq(spark, RULES)
    validator = pipeline_mod.DQXValidator(spark)

    summary = validator.validate(
        df, "clinical", "site",
        metadata=[{"study_id": "XL092-303", "vendor": "Veeva", "schema_id": "sch-1"}],
    )

    assert summary["total"] == 4
    assert summary["passed"] == 3
    assert summary["failed"] == 1
    assert summary["pass_rate"] == 75.0

    statuses = {r["rule_id"]: r["status"] for r in summary["results"]}
    assert statuses == {
        "R_SUBJECT_NOT_NULL": "PASS",
        "R_SUBJECT_UNIQUE": "PASS",
        "R_STATUS_IN_LIST": "FAIL",
        "R_COMPOUND_UNIQUE": "PASS",
    }
    # Metadata from the transformer stage is stamped onto every result.
    assert {r["study_id"] for r in summary["results"]} == {"XL092-303"}
    assert {r["schema_id"] for r in summary["results"]} == {"sch-1"}


def test_dq_validation_uses_an_isolated_temp_view_and_cleans_it_up(silver):
    pipeline_mod, spark = silver
    df = _script_dq(spark, RULES)
    validator = pipeline_mod.DQXValidator(spark)

    validator.validate(df, "clinical", "site")

    view_name = df.registered_views[0]
    assert view_name.startswith("_dq_staging_")
    assert view_name not in spark.temp_views
    assert all(view_name in q for q in spark.queries_matching("FROM _dq_staging_"))


def test_dq_results_are_persisted_for_every_rule(silver):
    pipeline_mod, spark = silver
    df = _script_dq(spark, RULES)
    validator = pipeline_mod.DQXValidator(spark)

    validator.validate(
        df, "clinical", "site",
        metadata=[{"study_id": "XL092-303", "vendor": "Veeva", "schema_id": "sch-1"}],
    )

    results_tbl = "`drugdev-catalog`.meta_schema.dq_validation_results_log"
    persisted = spark.writes_to(results_tbl)
    assert len(persisted) == 4
    assert {w["mode"] for w in persisted} == {"append"}
    assert {w["format"] for w in persisted} == {"delta"}

    rows = {w["rows"][0]["rule_id"]: w["rows"][0] for w in persisted}
    assert rows["R_STATUS_IN_LIST"]["result"] == "FAILURE"
    assert rows["R_STATUS_IN_LIST"]["failed_record_count"] == 1
    assert rows["R_SUBJECT_UNIQUE"]["result"] == "SUCCESS"
    assert rows["R_SUBJECT_NOT_NULL"]["layer"] == "SILVER"
    assert rows["R_SUBJECT_NOT_NULL"]["record_count"] == 10
    assert rows["R_COMPOUND_UNIQUE"]["column_name"] == "['study_id', 'subject_id']"


def test_critical_rule_failure_blocks_the_silver_write(silver):
    pipeline_mod, spark = silver
    rules = [dict(RULES[0], severity="CRITICAL")]
    df = _script_dq(spark, rules, null_count=3)
    validator = pipeline_mod.DQXValidator(spark)

    with pytest.raises(pipeline_mod.DQCriticalFailureError) as exc:
        validator.validate(df, "clinical", "site", study_id="XL092-303")

    assert "R_SUBJECT_NOT_NULL" in str(exc.value)
    assert "clinical/site/XL092-303" in str(exc.value)
    # The failure is still recorded before the exception propagates.
    persisted = spark.writes_to("`drugdev-catalog`.meta_schema.dq_validation_results_log")
    assert persisted[0]["rows"][0]["result"] == "FAILURE"
    assert df.registered_views[0] not in spark.temp_views


def test_entity_without_registry_rules_short_circuits(silver):
    pipeline_mod, spark = silver
    spark.on("SELECT validation_rules", [])
    validator = pipeline_mod.DQXValidator(spark)

    summary = validator.validate(spark.dataframe([{"a": 1}]), "clinical", "site")

    assert summary == {"pass_rate": 100.0, "total": 0, "passed": 0, "failed": 0}
    assert spark.writes == []


# ── Governance applied to the Silver output ───────────────────────────────────

@pytest.fixture
def governed(monkeypatch):
    pipeline_mod, spark = _load_silver(monkeypatch)
    spark.on("SELECT current_user()", [{"current_user": "svc-silver@example.com"}])
    spark.register_table(
        SILVER_TABLE,
        spark.dataframe(
            [],
            columns=["site_id", "subject_name", "age"],
            dtypes=[("site_id", "string"), ("subject_name", "string"), ("age", "int")],
        ),
    )
    return pipeline_mod.SilverPipeline(spark), spark


def test_silver_output_gets_table_tags_and_unmask_grants(governed):
    pipeline, spark = governed

    pipeline._apply_silver_tags(
        SILVER_TABLE, ["site_id", "subject_name", "age"],
        "clinical", "Veeva", "site", "restricted", None,
    )

    tag_sql = spark.queries_matching("SET TAGS")[0]
    assert "'sensitivity' = 'RESTRICTED'" in tag_sql
    assert "'domain' = 'CLINICAL'" in tag_sql
    assert "'layer' = 'SILVER'" in tag_sql
    assert "'vendor' = 'Veeva'" in tag_sql
    assert "'entity' = 'site'" in tag_sql

    grants = spark.queries_matching("GRANT UNMASK")
    assert f"GRANT UNMASK ON TABLE {SILVER_TABLE} TO `pii_unmasked_access`" in grants
    assert f"GRANT UNMASK ON TABLE {SILVER_TABLE} TO `svc-silver@example.com`" in grants


def test_pii_columns_get_tags_and_type_matched_masks(governed):
    pipeline, spark = governed
    pii = json.dumps([
        {"column": "subject_name", "sensitivity": "restricted", "pii_category": "name"},
        {"column": "age", "sensitivity": "confidential", "pii_category": "demographic"},
        {"column": "not_in_table", "sensitivity": "restricted"},
    ])

    pipeline._apply_silver_tags(
        SILVER_TABLE, ["site_id", "subject_name", "age"],
        "clinical", "Veeva", "site", "confidential", pii,
    )

    col_tags = spark.queries_matching("ALTER COLUMN `subject_name` SET TAGS")[0]
    assert "'sensitivity' = 'RESTRICTED'" in col_tags
    assert "'pii_category' = 'NAME'" in col_tags

    # Mask functions are created per column type before being attached.
    assert spark.queries_matching(
        "CREATE OR REPLACE FUNCTION `meta_catalog`.meta_schema.mask_pii_string(val STRING)"
    )
    assert spark.queries_matching(
        "CREATE OR REPLACE FUNCTION `meta_catalog`.meta_schema.mask_pii_int(val INT)"
    )
    masks = spark.queries_matching("SET MASK")
    assert f"ALTER TABLE {SILVER_TABLE} ALTER COLUMN `subject_name` SET MASK `meta_catalog`.meta_schema.mask_pii_string" in masks
    assert f"ALTER TABLE {SILVER_TABLE} ALTER COLUMN `age` SET MASK `meta_catalog`.meta_schema.mask_pii_int" in masks

    # A registry column that is not in the table must never produce DDL.
    assert spark.queries_matching("not_in_table") == []


def test_unmask_predicate_covers_groups_and_named_principals(monkeypatch):
    pipeline_mod, spark = _load_silver(
        monkeypatch,
        {"drugdev.PII_UNMASK_GROUP": "clinical_admins, group:dq_stewards, user:alice@example.com"},
    )
    spark.on("SELECT current_user()", [{"current_user": "svc-silver@example.com"}])
    pipeline = pipeline_mod.SilverPipeline(spark)

    predicate = pipeline._build_unmask_predicate()

    assert predicate == (
        "is_account_group_member('clinical_admins') OR "
        "is_account_group_member('dq_stewards') OR "
        "lower(current_user()) = lower('alice@example.com')"
    )
    assert pipeline._get_unmask_grantees() == [
        "clinical_admins", "dq_stewards", "alice@example.com", "svc-silver@example.com",
    ]


# ── Post-write maintenance ────────────────────────────────────────────────────

def test_optimize_zorders_on_registry_supplied_columns(governed):
    pipeline, spark = governed

    pipeline.optimize(SILVER_TABLE, ["study_id", "site_id"])
    pipeline.optimize(SILVER_TABLE, [])

    assert spark.queries[-2] == f"OPTIMIZE {SILVER_TABLE} ZORDER BY (`study_id`, `site_id`)"
    assert spark.queries[-1] == f"OPTIMIZE {SILVER_TABLE}"


def test_optimize_rejects_injected_identifiers(governed):
    pipeline, _spark = governed

    with pytest.raises(ValueError, match="invalid characters"):
        pipeline.optimize("silver.t; DROP TABLE x", ["study_id"])


@pytest.mark.parametrize(
    "sid_key, entity, expected",
    [
        ("XL092-303", "site", "XL092_303_site_staging"),
        ("", "site", "Global_site_staging"),
        ("XL092/303", "adverse events", "XL092_303_adverse_events_staging"),
        ("---", "", "Global_entity_staging"),
    ],
)
def test_staging_view_names_are_sql_safe(governed, sid_key, entity, expected):
    pipeline, _spark = governed

    assert pipeline._safe_temp_view_name(sid_key, entity) == expected
