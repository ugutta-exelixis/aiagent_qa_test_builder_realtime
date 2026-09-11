import importlib.util
import sys
import types
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]


def import_module_from_path(module_name: str, relative_path: str, extra_sys_path=()):
    module_path = REPO_ROOT / relative_path
    previous_path = list(sys.path)
    sys.modules.pop(module_name, None)
    try:
        for path in reversed([str(REPO_ROOT), *map(str, extra_sys_path)]):
            if path not in sys.path:
                sys.path.insert(0, path)
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path[:] = previous_path


class FakeSparkConf:
    DEFAULTS = {
        "drugdev.METADATA_CATALOG": "meta_catalog",
        "drugdev.METADATA_SCHEMA": "meta_schema",
        "drugdev.REGISTRY_SCHEMA": "registry_schema",
        "drugdev.CATALOG": "drugdev-catalog",
        "drugdev.BRONZE_SCHEMA": "bronze_schema",
        "drugdev.SILVER_SCHEMA": "silver_schema",
        "drugdev.GOLD_SCHEMA": "gold_schema",
        "drugdev.environment": "unit",
        "drugdev.CONFIG_PATH": "unit-config.yml",
        "drugdev.YML_SILVER_CONFIG_PATH": "silver.yml",
        "drugdev.YML_SILVER_VENDOR_CONFIG_PATH": "vendor.yml",
        "drugdev.YML_SILVER_ENTITY_CONFIG_PATH": "entity.yml",
        "drugdev.source_bucket": "unit-bucket",
        "drugdev.RUN_DATE": "20260901",
    }

    def __init__(self, values=None):
        self.values = dict(self.DEFAULTS)
        if values:
            self.values.update(values)

    def get(self, key, default=None):
        return self.values.get(key, default)


class FakeSparkSession:
    def __init__(self, conf_values=None):
        self.conf = FakeSparkConf(conf_values)

    def sql(self, query):
        raise AssertionError(f"Unexpected Spark SQL call in unit test: {query}")


class FakeSparkBuilder:
    def __init__(self, spark):
        self.spark = spark

    def getOrCreate(self):
        return self.spark


@pytest.fixture
def stub_pyspark(monkeypatch):
    fake_spark = FakeSparkSession()

    pyspark_mod = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")
    functions_mod = types.ModuleType("pyspark.sql.functions")
    window_mod = types.ModuleType("pyspark.sql.window")
    dbutils_mod = types.ModuleType("pyspark.dbutils")
    types_mod = types.ModuleType("pyspark.sql.types")

    class SparkSessionClass:
        builder = FakeSparkBuilder(fake_spark)

        @staticmethod
        def getActiveSession():
            return fake_spark

    class Row(dict):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.__dict__.update(kwargs)

    class DBUtils:
        def __init__(self, spark):
            self.spark = spark

    class Window:
        pass

    for type_name in (
        "StructType",
        "StructField",
        "StringType",
        "IntegerType",
        "LongType",
        "DoubleType",
        "FloatType",
        "BooleanType",
        "TimestampType",
        "DateType",
    ):
        setattr(types_mod, type_name, type(type_name, (), {}))

    sql_mod.SparkSession = SparkSessionClass
    sql_mod.Row = Row
    sql_mod.functions = functions_mod
    window_mod.Window = Window
    dbutils_mod.DBUtils = DBUtils

    monkeypatch.setitem(sys.modules, "pyspark", pyspark_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql.window", window_mod)
    monkeypatch.setitem(sys.modules, "pyspark.dbutils", dbutils_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", types_mod)
    return fake_spark
