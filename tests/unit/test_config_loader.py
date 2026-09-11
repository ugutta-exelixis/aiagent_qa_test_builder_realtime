import pytest

from conftest import import_module_from_path


def _fake_spark(conf_overrides=None):
    values = {
        "drugdev.METADATA_CATALOG": "metadata-catalog",
        "drugdev.METADATA_SCHEMA": "metadata_schema",
        "drugdev.REGISTRY_SCHEMA": "registry_schema",
        "drugdev.CATALOG": "drugdev-catalog",
        "drugdev.BRONZE_SCHEMA": "bronze_schema",
        "drugdev.SILVER_SCHEMA": "silver_schema",
        "drugdev.GOLD_SCHEMA": "gold_schema",
        "drugdev.environment": "unit",
        "drugdev.RUN_DATE": "20260901",
    }
    if conf_overrides:
        values.update(conf_overrides)

    class Conf:
        def get(self, key, default=None):
            return values.get(key, default)

    class Spark:
        conf = Conf()

    return Spark()


def test_gold_config_loader_reads_required_conf_and_run_date():
    config_loader = import_module_from_path(
        "gold_config_loader",
        r"databricks_bundle\drugdev\gold_framework\gold_engine\utils\config_loader.py",
    )

    conf = config_loader._get_conf(_fake_spark())

    assert conf["METADATA_CATALOG"] == "metadata-catalog"
    assert conf["CATALOG"] == "drugdev-catalog"
    assert conf["RUN_DATE"] == "20260901"


def test_gold_config_loader_raises_when_spark_is_missing():
    config_loader = import_module_from_path(
        "gold_config_loader_missing_spark",
        r"databricks_bundle\drugdev\gold_framework\gold_engine\utils\config_loader.py",
    )

    with pytest.raises(ValueError, match="spark is required"):
        config_loader._get_conf(None)


def test_gold_config_loader_reports_missing_required_keys():
    config_loader = import_module_from_path(
        "gold_config_loader_missing_key",
        r"databricks_bundle\drugdev\gold_framework\gold_engine\utils\config_loader.py",
    )

    with pytest.raises(ValueError, match="drugdev.GOLD_SCHEMA"):
        config_loader._get_conf(_fake_spark({"drugdev.GOLD_SCHEMA": ""}))


def test_gold_config_loader_schema_helpers_return_configured_values():
    config_loader = import_module_from_path(
        "gold_config_loader_helpers",
        r"databricks_bundle\drugdev\gold_framework\gold_engine\utils\config_loader.py",
    )
    spark = _fake_spark()

    assert config_loader._quoted_catalog(spark) == "`drugdev-catalog`"
    assert config_loader._bronze_schema(spark) == "bronze_schema"
    assert config_loader._silver_schema(spark) == "silver_schema"
    assert config_loader._gold_schema(spark) == "gold_schema"
    assert config_loader._common_schema(spark) == "registry_schema"
