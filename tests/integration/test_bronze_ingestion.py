"""Integration tests for the Bronze ingestion path.

These wire the real ingestion framework modules together — YAML config loading,
dataset-registry upsert and schema-drift registration — against a scripted Spark
session, so the metadata contract between the stages is verified as a whole.
"""

import json

import pytest

from harness import ScriptedSpark, import_module_from_path, install_pyspark_stub


INGESTION_METADATA_LOADER = (
    r"databricks_bundle\drugdev\ingestion_framework\metadata_service\metadata_loader.py"
)
SCHEMA_REGISTRY_PATH = (
    r"databricks_bundle\drugdev\ingestion_framework\ingestion_engine\schema_registry.py"
)

CONFIG_YAML = """
domain_name: clinical
data_product_name: ctms
owner_team: data-engineering
owner_email: de@example.com
datasets:
  - dataset_name: veeva_xl092_303_site
    dataset_version: 1
    criticality: HIGH
    contains_pii: false
    data_classification: CONFIDENTIAL
    lifecycle_status: ACTIVE
    retention_days: 2555
    is_active: true
    frequency: DAILY
    source_path: s3://${source_bucket}/ctms/site/
    metadata_table: ${METADATA_CATALOG}.${REGISTRY_SCHEMA}.dataset_registry
  - dataset_name: veeva_xl092_303_participant
    dataset_version: 2
    criticality: CRITICAL
    contains_pii: true
    data_classification: RESTRICTED
    lifecycle_status: ACTIVE
    retention_days: 3650
    is_active: true
    frequency: DAILY
    source_path: s3://${SOURCE_BUCKET}/ctms/participant/
"""


class _Logger:
    def __init__(self):
        self.messages = []

    def info(self, message, *args):
        self.messages.append(message % args if args else message)

    warning = info
    error = info


@pytest.fixture
def bronze(monkeypatch, tmp_path):
    """Load the ingestion metadata loader against a scripted Spark session."""
    config_file = tmp_path / "drugdev_ingestion_config.yaml"
    config_file.write_text(CONFIG_YAML, encoding="utf-8")

    spark = install_pyspark_stub(monkeypatch)
    spark.conf.set("drugdev.YML_CONFIG_PATH", str(config_file))

    module = import_module_from_path("bronze_metadata_loader", INGESTION_METADATA_LOADER)
    return module, spark, config_file


# ── Config loading ─────────────────────────────────────────────────────────────

def test_metadata_tables_are_derived_from_spark_conf(bronze):
    loader, _spark, _cfg = bronze

    assert loader.DATASET_REGISTRY_TBL == "`meta_catalog`.registry_schema.dataset_registry"
    assert loader.INGESTION_CONFIG_TBL == "`meta_catalog`.meta_schema.ingestion_config"
    assert loader.DATASET_TAGS_TBL == "`meta_catalog`.registry_schema.dataset_tags"
    assert loader.DATASET_DEPS_TBL == "`meta_catalog`.registry_schema.dataset_dependencies"
    assert loader.DQ_SLA_CONFIG_TBL == "`meta_catalog`.meta_schema.dq_sla_config"


def test_load_config_resolves_every_placeholder_from_spark_conf(bronze):
    loader, _spark, _cfg = bronze

    config = loader.load_config("test")

    site, participant = config["datasets"]
    assert config["domain_name"] == "clinical"
    assert site["source_path"] == "s3://test-source-bucket/ctms/site/"
    assert site["metadata_table"] == "meta_catalog.registry_schema.dataset_registry"
    # Both spellings of the bucket variable must resolve.
    assert participant["source_path"] == "s3://test-source-bucket/ctms/participant/"


def test_load_config_rejects_unresolved_placeholders(bronze, tmp_path):
    loader, spark, _cfg = bronze
    broken = tmp_path / "broken.yaml"
    broken.write_text("datasets:\n  - source_path: s3://${UNKNOWN_BUCKET}/x\n", encoding="utf-8")
    spark.conf.set("drugdev.YML_CONFIG_PATH", str(broken))

    with pytest.raises(ValueError, match=r"Unresolved config placeholders"):
        loader.load_config("test")


# ── Dataset registry upsert ────────────────────────────────────────────────────

def test_dataset_registry_reuses_existing_ids_and_mints_new_ones(bronze):
    loader, spark, _cfg = bronze
    spark.on(
        "SELECT dataset_id, domain_name",
        [
            {
                "dataset_id": "existing-site-id",
                "domain_name": "clinical",
                "data_product_name": "ctms",
                "dataset_name": "veeva_xl092_303_site",
            }
        ],
    )
    config = loader.load_config("test")

    dataset_id_map = loader.load_dataset_registry(config)

    assert dataset_id_map["veeva_xl092_303_site"] == "existing-site-id"
    new_id = dataset_id_map["veeva_xl092_303_participant"]
    assert new_id and new_id != "existing-site-id"


def test_dataset_registry_stages_typed_rows_and_merges_them(bronze):
    loader, spark, _cfg = bronze
    spark.on("SELECT dataset_id, domain_name", [])
    config = loader.load_config("test")

    loader.load_dataset_registry(config)

    staged = spark.created_dataframes[0]["rows"]
    assert [r["dataset_name"] for r in staged] == [
        "veeva_xl092_303_site",
        "veeva_xl092_303_participant",
    ]
    # Types coming out of YAML must be coerced before the MERGE.
    assert staged[1]["contains_pii"] is True
    assert staged[1]["retention_days"] == 3650
    assert staged[0]["owner_team"] == "data-engineering"

    merge_sql = spark.queries_matching("MERGE INTO")
    assert len(merge_sql) == 1
    assert "MERGE INTO `meta_catalog`.registry_schema.dataset_registry t" in merge_sql[0]
    assert "USING dataset_registry_source s" in merge_sql[0]
    assert "ON t.dataset_id = s.dataset_id" in merge_sql[0]
    assert "dataset_registry_source" in spark.temp_views


def test_dataset_registry_skips_merge_when_config_has_no_datasets(bronze):
    loader, spark, _cfg = bronze
    spark.on("SELECT dataset_id, domain_name", [])
    config = loader.load_config("test")
    config["datasets"] = []

    assert loader.load_dataset_registry(config) == {}
    assert spark.queries_matching("MERGE INTO") == []


# ── Schema drift registration ──────────────────────────────────────────────────

def test_new_dataset_flows_from_config_into_schema_registry(monkeypatch, tmp_path):
    """A dataset registered in Bronze gets a v1 schema entry keyed by its id."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(CONFIG_YAML, encoding="utf-8")

    spark = install_pyspark_stub(monkeypatch)
    spark.conf.set("drugdev.YML_CONFIG_PATH", str(config_file))
    spark.on("SELECT dataset_id, domain_name", [])

    loader = import_module_from_path("bronze_metadata_loader_flow", INGESTION_METADATA_LOADER)
    registry = import_module_from_path("bronze_schema_registry_flow", SCHEMA_REGISTRY_PATH)

    config = loader.load_config("test")
    dataset_id_map = loader.load_dataset_registry(config)
    dataset_id = dataset_id_map["veeva_xl092_303_site"]

    ingested = spark.dataframe(
        [{"site_id": "S1", "study_id": "XL092-303", "country": "US"}]
    )
    registry.check_and_update_schema(spark, dataset_id, ingested, _Logger())

    insert_sql = spark.queries_matching("INSERT INTO `meta_catalog`.meta_schema.schema_registry")
    assert len(insert_sql) == 1
    assert f"'{dataset_id}'" in insert_sql[0]
    assert json.dumps(["country", "site_id", "study_id"]) in insert_sql[0]
    assert " 1, true," in insert_sql[0]


def test_schema_drift_on_second_run_supersedes_the_active_version(monkeypatch, tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(CONFIG_YAML, encoding="utf-8")

    spark = install_pyspark_stub(monkeypatch)
    spark.conf.set("drugdev.YML_CONFIG_PATH", str(config_file))
    spark.on(
        "SELECT schema_json, version",
        [{"schema_json": json.dumps(["country", "site_id", "study_id"]), "version": 1}],
    )

    registry = import_module_from_path("bronze_schema_registry_drift_flow", SCHEMA_REGISTRY_PATH)
    drifted = spark.dataframe(
        [{"site_id": "S1", "study_id": "XL092-303", "country": "US", "site_status": "ACTIVE"}]
    )

    registry.check_and_update_schema(spark, "ds-site", drifted, _Logger())

    assert len(spark.queries_matching("SET is_active = false")) == 1
    insert_sql = spark.queries_matching("INSERT INTO")[0]
    assert "site_status" in insert_sql
    assert " 2, true," in insert_sql
