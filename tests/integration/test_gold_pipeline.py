"""Integration tests for the Gold pipeline.

``gold_engine.main`` is the orchestration entry point: it reads the Gold object
registry, groups objects into execution waves, dispatches each object to its
build strategy, applies governance and records execution metrics.  These tests
run that full loop against a scripted Spark session with a registry that spans
all three build strategies.
"""

import json
import sys
import types

import pytest

from harness import import_module_from_path, install_pyspark_stub


GOLD_ENGINE_PATH = "databricks_bundle/drugdev/gold_framework/gold_engine/gold_engine.py"

REGISTRY_TBL = "`meta_catalog`.registry_schema.gold_object_registry"
METRICS_TBL = "`drugdev-catalog`.meta_schema.pipeline_execution_metrics"
DIM_SITE_TBL = "`drugdev-catalog`.gold_schema.dim_site"
DIM_PARTICIPANT_TBL = "`drugdev-catalog`.gold_schema.dim_participant"

SITE_TAGS = json.dumps(
    {
        "domain": "clinical",
        "layer": "GOLD",
        "column_tags": {
            "site_name": {"sensitivity": "restricted", "pii_category": "name"},
            "not_in_output": {"sensitivity": "restricted"},
        },
    }
)


def _registry_row(**overrides):
    row = {
        "object_id": "obj-1",
        "object_name": "dim_site",
        "object_type": "TABLE",
        "build_strategy": "FULL_REFRESH",
        "class_path": None,
        "write_mode": "overwrite",
        "execution_order": 1,
        "dependencies": None,
        "sql_template": None,
        "partition_cols": None,
        "is_enabled": True,
        "tags": None,
    }
    row.update(overrides)
    return row


FULL_REFRESH_ROW = _registry_row(
    object_name="dim_site",
    sql_template="SELECT * FROM {silver_schema}.silver_site",
    partition_cols=json.dumps(["country"]),
    tags=SITE_TAGS,
)

PYSPARK_MODEL_ROW = _registry_row(
    object_id="obj-2",
    object_name="dim_participant",
    build_strategy="PYSPARK_MODEL",
    class_path="gold.models.dim_participant.DimParticipant",
    execution_order=1,
)

VIEW_DDL_ROW = _registry_row(
    object_id="obj-3",
    object_name="vw_enrollment",
    object_type="VIEW",
    build_strategy="VIEW_DDL",
    execution_order=2,
    sql_template=(
        "CREATE OR REPLACE VIEW {gold_schema}.vw_enrollment AS "
        "SELECT * FROM {gold_schema}.dim_site"
    ),
)

BROKEN_ROW = _registry_row(
    object_id="obj-4",
    object_name="fct_broken",
    build_strategy="NOT_A_STRATEGY",
    execution_order=None,
)


def _install_model_module(monkeypatch, spark, build_rows):
    """Register a fake `models.dim_participant` for the PYSPARK_MODEL strategy."""
    package = types.ModuleType("models")
    package.__path__ = []
    module = types.ModuleType("models.dim_participant")

    class DimParticipant:
        calls = []

        def build(self, spark_session, silver, meta):
            DimParticipant.calls.append({"silver": silver, "meta": meta})
            return spark_session.dataframe(build_rows)

    DimParticipant.calls = []
    module.DimParticipant = DimParticipant
    package.dim_participant = module

    monkeypatch.setitem(sys.modules, "models", package)
    monkeypatch.setitem(sys.modules, "models.dim_participant", module)
    return DimParticipant


def _make_gold(monkeypatch, registry_rows, conf_values=None):
    spark = install_pyspark_stub(monkeypatch, conf_values)
    spark.on("gold_object_registry", list(registry_rows))
    spark.on("SELECT current_user()", [{"current_user": "svc-gold@example.com"}])
    spark.on(
        "silver_site",
        [{"site_id": "S1", "site_name": "Boston Clinic", "country": "US"}],
    )
    spark.register_table(
        DIM_SITE_TBL,
        spark.dataframe(
            [],
            columns=["site_id", "site_name", "country"],
            dtypes=[("site_id", "string"), ("site_name", "string"), ("country", "string")],
        ),
    )
    model_cls = _install_model_module(
        monkeypatch, spark, [{"subject_id": "P1", "study_id": "XL092-303"}]
    )
    module = import_module_from_path("gold_engine_it", GOLD_ENGINE_PATH)
    return module, spark, model_cls


@pytest.fixture
def gold(monkeypatch):
    return _make_gold(monkeypatch, [FULL_REFRESH_ROW, PYSPARK_MODEL_ROW, VIEW_DDL_ROW])


# ── Orchestration ──────────────────────────────────────────────────────────────

def test_all_enabled_objects_are_fetched_in_execution_order(gold):
    gold_engine, spark, _model = gold

    gold_engine.main("all")

    registry_sql = spark.queries_matching("gold_object_registry")[0]
    assert f"FROM {REGISTRY_TBL}" in registry_sql
    assert "WHERE is_enabled = true" in registry_sql
    assert "ORDER BY execution_order, object_name" in registry_sql


def test_each_build_strategy_produces_its_own_gold_object(gold):
    gold_engine, spark, model_cls = gold

    gold_engine.main("all")

    # FULL_REFRESH: tokens resolved, result overwritten into the Gold table.
    assert "SELECT * FROM `drugdev-catalog`.silver_schema.silver_site" in spark.queries
    site_write = spark.writes_to(DIM_SITE_TBL)[0]
    assert site_write["mode"] == "overwrite"
    assert site_write["format"] == "delta"
    assert site_write["partitions"] == ["country"]
    assert site_write["options"] == {"overwriteSchema": "true"}

    # PYSPARK_MODEL: class_path resolved through the models.* fallback.
    assert len(model_cls.calls) == 1
    participant_write = spark.writes_to(DIM_PARTICIPANT_TBL)[0]
    assert participant_write["rows"] == [{"subject_id": "P1", "study_id": "XL092-303"}]

    # VIEW_DDL: DDL executed, nothing written.
    assert (
        "CREATE OR REPLACE VIEW `drugdev-catalog`.gold_schema.vw_enrollment AS "
        "SELECT * FROM `drugdev-catalog`.gold_schema.dim_site" in spark.queries
    )
    assert spark.writes_to("`drugdev-catalog`.gold_schema.vw_enrollment") == []


def test_later_waves_run_after_earlier_waves_complete(gold):
    gold_engine, spark, _model = gold

    gold_engine.main("all")

    wave1_end = max(
        spark.queries.index(q) for q in spark.queries_matching("silver_site")
    )
    wave2_start = min(
        spark.queries.index(q) for q in spark.queries_matching("CREATE OR REPLACE VIEW")
    )
    assert wave1_end < wave2_start


def test_every_object_execution_is_recorded_in_pipeline_metrics(gold):
    gold_engine, spark, _model = gold

    gold_engine.main("all")

    metrics = [w["rows"][0] for w in spark.writes_to(METRICS_TBL)]
    by_task = {m["task_name"]: m for m in metrics}
    assert set(by_task) == {"dim_site", "dim_participant", "vw_enrollment"}
    assert {m["status"] for m in metrics} == {"SUCCESS"}
    assert {m["layer"] for m in metrics} == {"GOLD"}
    assert {m["job_name"] for m in metrics} == {"gold_pipeline"}
    assert by_task["dim_site"]["records_written"] == 1
    assert by_task["vw_enrollment"]["records_written"] == 0
    assert len({m["run_id"] for m in metrics}) == 1


def test_pipeline_run_id_from_spark_conf_is_reused_for_all_objects(monkeypatch):
    gold_engine, spark, _model = _make_gold(
        monkeypatch,
        [FULL_REFRESH_ROW, VIEW_DDL_ROW],
        {"drugdev.PIPELINE_RUN_ID": " run-2026-09-01 "},
    )

    gold_engine.main("all")

    run_ids = {w["rows"][0]["run_id"] for w in spark.writes_to(METRICS_TBL)}
    assert run_ids == {"run-2026-09-01"}


def test_single_object_mode_filters_the_registry(monkeypatch):
    gold_engine, spark, _model = _make_gold(monkeypatch, [FULL_REFRESH_ROW])

    gold_engine.main("object", object_name="dim_site")

    assert "AND object_name = 'dim_site'" in spark.queries_matching("gold_object_registry")[0]
    assert len(spark.writes_to(METRICS_TBL)) == 1


def test_single_object_mode_requires_an_object_name(gold):
    gold_engine, _spark, _model = gold

    with pytest.raises(ValueError, match="object_name is required"):
        gold_engine.main("object")


def test_empty_registry_is_a_no_op(monkeypatch):
    gold_engine, spark, _model = _make_gold(monkeypatch, [])

    assert gold_engine.main("all") is None
    assert spark.writes == []


# ── Failure handling ───────────────────────────────────────────────────────────

def test_failed_object_does_not_stop_the_remaining_waves(monkeypatch):
    gold_engine, spark, _model = _make_gold(
        monkeypatch, [BROKEN_ROW, FULL_REFRESH_ROW, VIEW_DDL_ROW]
    )

    with pytest.raises(RuntimeError, match=r"1 object\(s\) failed"):
        gold_engine.main("all")

    metrics = {w["rows"][0]["task_name"]: w["rows"][0] for w in spark.writes_to(METRICS_TBL)}
    assert metrics["fct_broken"]["status"] == "FAILED"
    assert metrics["fct_broken"]["error_type"] == "ValueError"
    assert "Unknown build_strategy 'NOT_A_STRATEGY'" in metrics["fct_broken"]["error_message"]
    # The broken object has execution_order=None (wave 99) so earlier waves still ran.
    assert metrics["dim_site"]["status"] == "SUCCESS"
    assert metrics["vw_enrollment"]["status"] == "SUCCESS"


def test_fail_fast_stops_before_the_next_wave(monkeypatch):
    broken_first_wave = _registry_row(
        object_id="obj-5",
        object_name="fct_broken",
        build_strategy="NOT_A_STRATEGY",
        execution_order=1,
    )
    gold_engine, spark, _model = _make_gold(monkeypatch, [broken_first_wave, VIEW_DDL_ROW])

    with pytest.raises(RuntimeError, match=r"1 object\(s\) failed"):
        gold_engine.main("all", fail_fast=True)

    assert spark.queries_matching("CREATE OR REPLACE VIEW") == []
    tasks = {w["rows"][0]["task_name"] for w in spark.writes_to(METRICS_TBL)}
    assert tasks == {"fct_broken"}


def test_fail_fast_can_be_forced_from_spark_conf(monkeypatch):
    broken_first_wave = _registry_row(
        object_id="obj-5",
        object_name="fct_broken",
        build_strategy="NOT_A_STRATEGY",
        execution_order=1,
    )
    gold_engine, spark, _model = _make_gold(
        monkeypatch,
        [broken_first_wave, VIEW_DDL_ROW],
        {"drugdev.GOLD_FAIL_FAST": "true"},
    )

    with pytest.raises(RuntimeError):
        gold_engine.main("all", fail_fast=False)

    assert spark.queries_matching("CREATE OR REPLACE VIEW") == []


def test_missing_sql_template_is_reported_per_object(monkeypatch):
    no_sql = _registry_row(object_id="obj-6", object_name="fct_no_sql", sql_template=None)
    gold_engine, spark, _model = _make_gold(monkeypatch, [no_sql])

    with pytest.raises(RuntimeError):
        gold_engine.main("all")

    metric = spark.writes_to(METRICS_TBL)[0]["rows"][0]
    assert metric["status"] == "FAILED"
    assert "requires sql_template for 'fct_no_sql'" in metric["error_message"]


# ── Governance on Gold outputs ────────────────────────────────────────────────

def test_gold_table_receives_tags_grants_and_column_masks(gold):
    gold_engine, spark, _model = gold

    gold_engine.main("all")

    assert f"ALTER TABLE {DIM_SITE_TBL} SET TAGS ('domain' = 'clinical', 'layer' = 'GOLD')" in spark.queries

    grants = spark.queries_matching(f"GRANT UNMASK ON TABLE {DIM_SITE_TBL}")
    assert f"GRANT UNMASK ON TABLE {DIM_SITE_TBL} TO `pii_unmasked_access`" in grants
    assert f"GRANT UNMASK ON TABLE {DIM_SITE_TBL} TO `svc-gold@example.com`" in grants

    col_tags = spark.queries_matching("ALTER COLUMN `site_name` SET TAGS")[0]
    assert "'sensitivity' = 'RESTRICTED'" in col_tags
    assert "'pii_category' = 'NAME'" in col_tags

    masks = spark.queries_matching("SET MASK")
    assert len(masks) == 1
    assert masks[0].startswith(f"ALTER TABLE {DIM_SITE_TBL} ALTER COLUMN `site_name` SET MASK")
    assert "mask_pii_string" in masks[0]
    assert spark.queries_matching(
        "CREATE OR REPLACE FUNCTION `meta_catalog`.meta_schema.mask_pii_string"
    )

    # A column_tags entry absent from the built output must not emit DDL.
    assert spark.queries_matching("not_in_output") == []


def test_objects_without_tags_still_get_unmask_grants(monkeypatch):
    untagged = _registry_row(
        object_id="obj-7",
        object_name="dim_site",
        sql_template="SELECT * FROM {silver_schema}.silver_site",
    )
    gold_engine, spark, _model = _make_gold(monkeypatch, [untagged])

    gold_engine.main("all")

    assert spark.queries_matching(f"GRANT UNMASK ON TABLE {DIM_SITE_TBL}")
    assert spark.queries_matching("SET TAGS") == []
