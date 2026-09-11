"""Shared test harness for the pipeline integration tests.

The Databricks frameworks in this repo are written against a live Spark session
that is created at *module import time*.  Running them on a real cluster from CI
is not practical, so these tests wire the real framework modules together on top
of a scripted, in-memory Spark double.

That still exercises genuine integration behaviour: real metadata parsing, real
SQL construction, real orchestration/branching and real error handling — only
the Spark execution boundary is replaced.

This module is deliberately *not* named ``conftest.py`` so it can be imported
explicitly (``from harness import ...``) without clashing with the unit-test
conftest of the same basename.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


DEFAULT_CONF = {
    "drugdev.METADATA_CATALOG": "meta_catalog",
    "drugdev.METADATA_SCHEMA": "meta_schema",
    "drugdev.REGISTRY_SCHEMA": "registry_schema",
    "drugdev.CATALOG": "drugdev-catalog",
    "drugdev.BRONZE_SCHEMA": "bronze_schema",
    "drugdev.SILVER_SCHEMA": "silver_schema",
    "drugdev.GOLD_SCHEMA": "gold_schema",
    "drugdev.environment": "test",
    "drugdev.CONFIG_PATH": "test-config.yml",
    "drugdev.YML_CONFIG_PATH": "test-ingestion.yml",
    "drugdev.YML_SILVER_CONFIG_PATH": "test-silver.yml",
    "drugdev.source_bucket": "test-source-bucket",
    "drugdev.raw_bucket": "test-raw-bucket",
    "drugdev.RUN_DATE": "20260901",
}


# ── Spark doubles ──────────────────────────────────────────────────────────────

class FakeRow(dict):
    """Dict with attribute access, mirroring the parts of ``Row`` we rely on."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(name) from exc

    def asDict(self, recursive=False):
        return dict(self)


class FakeDataType:
    def __init__(self, simple_string):
        self._simple_string = simple_string

    def simpleString(self):
        return self._simple_string


class FakeField:
    def __init__(self, name, dtype):
        self.name = name
        self.dataType = FakeDataType(dtype)


class FakeSchema:
    def __init__(self, fields):
        self.fields = fields


class FakeWriter:
    """Records the writer chain instead of touching storage."""

    def __init__(self, df, sink):
        self._df = df
        self._sink = sink
        self._format = None
        self._mode = None
        self._options = {}
        self._partitions = []

    def format(self, fmt):
        self._format = fmt
        return self

    def mode(self, mode):
        self._mode = mode
        return self

    def option(self, key, value):
        self._options[key] = value
        return self

    def partitionBy(self, *cols):
        self._partitions = list(cols)
        return self

    def _record(self, **extra):
        entry = {
            "format": self._format,
            "mode": self._mode,
            "options": dict(self._options),
            "partitions": list(self._partitions),
            "rows": [dict(r) for r in self._df.rows],
            "columns": list(self._df.columns),
        }
        entry.update(extra)
        self._sink.append(entry)

    def saveAsTable(self, table_name):
        self._record(table=table_name)

    def save(self, path):
        self._record(path=path)


class FakeDataFrame:
    def __init__(self, rows=None, columns=None, dtypes=None, writes=None, spark=None):
        self.rows = [FakeRow(r) if isinstance(r, dict) else r for r in (rows or [])]
        if columns is not None:
            self.columns = list(columns)
        elif self.rows:
            self.columns = list(self.rows[0].keys())
        else:
            self.columns = []
        self._dtypes = dtypes
        self._writes = writes if writes is not None else []
        self._spark = spark
        self.registered_views = []

    # ── shape ────────────────────────────────────────────────────────────────
    @property
    def dtypes(self):
        return self._dtypes or [(c, "string") for c in self.columns]

    @property
    def schema(self):
        return FakeSchema([FakeField(name, dtype) for name, dtype in self.dtypes])

    # ── actions ──────────────────────────────────────────────────────────────
    def count(self):
        return len(self.rows)

    def collect(self):
        return list(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def take(self, n):
        return self.rows[:n]

    # ── transformations (identity — SQL text is what we assert on) ────────────
    def cache(self):
        return self

    def filter(self, _condition):
        return self

    def select(self, *_cols):
        return self

    def createOrReplaceTempView(self, name):
        self.registered_views.append(name)
        if self._spark is not None:
            self._spark.temp_views[name] = self

    @property
    def write(self):
        return FakeWriter(self, self._writes)


class FakeConf:
    def __init__(self, values=None):
        self.values = dict(DEFAULT_CONF)
        if values:
            self.values.update(values)

    def get(self, key, default=None):
        value = self.values.get(key, default)
        return default if value is None else value

    def set(self, key, value):
        self.values[key] = value


class FakeCatalog:
    def __init__(self, spark):
        self._spark = spark

    def dropTempView(self, name):
        self._spark.temp_views.pop(name, None)


class FakeReader:
    def __init__(self, spark):
        self._spark = spark

    def table(self, name):
        return self._spark.table(name)


class ScriptedSpark:
    """A Spark session double that records SQL and replays scripted answers.

    Handlers are registered with :meth:`on` and matched in registration order.
    Anything unmatched returns an empty DataFrame, which keeps "fire and forget"
    DDL statements (ALTER TABLE, GRANT, MERGE ...) simple to assert on via
    :attr:`queries`.
    """

    def __init__(self, conf_values=None):
        self.conf = FakeConf(conf_values)
        self.queries = []
        self.writes = []
        self.temp_views = {}
        self.tables = {}
        self.created_dataframes = []
        self._handlers = []

    # ── scripting ────────────────────────────────────────────────────────────
    def on(self, matcher, response):
        """Register a response for queries matching ``matcher``.

        ``matcher`` is a substring (case-insensitive) or a predicate callable.
        ``response`` is a FakeDataFrame, a list of row dicts, an exception
        instance to raise, or a callable taking the normalized query.
        """
        self._handlers.append((matcher, response))
        return self

    def register_table(self, fqn, dataframe):
        self.tables[fqn] = dataframe
        return self

    # ── Spark surface ────────────────────────────────────────────────────────
    def sql(self, query):
        normalized = " ".join(str(query).split())
        self.queries.append(normalized)

        for matcher, response in self._handlers:
            if callable(matcher):
                matched = matcher(normalized)
            else:
                matched = matcher.lower() in normalized.lower()
            if not matched:
                continue
            if isinstance(response, BaseException):
                raise response
            if callable(response):
                response = response(normalized)
            if isinstance(response, list):
                return self.dataframe(response)
            return response

        return self.dataframe([])

    def table(self, fqn):
        if fqn in self.tables:
            return self.tables[fqn]
        raise ValueError(f"Table or view not found: {fqn}")

    @property
    def read(self):
        return FakeReader(self)

    @property
    def catalog(self):
        return FakeCatalog(self)

    def createDataFrame(self, rows, schema=None):
        materialized = [dict(r) for r in rows]
        self.created_dataframes.append({"rows": materialized, "schema": schema})
        return self.dataframe(materialized)

    # ── factory ──────────────────────────────────────────────────────────────
    def dataframe(self, rows=None, columns=None, dtypes=None):
        return FakeDataFrame(
            rows=rows, columns=columns, dtypes=dtypes, writes=self.writes, spark=self
        )

    # ── assertions helpers ───────────────────────────────────────────────────
    def queries_matching(self, needle):
        return [q for q in self.queries if needle.lower() in q.lower()]

    def writes_to(self, table_name):
        return [w for w in self.writes if w.get("table") == table_name]


# ── pyspark stubbing / module loading ──────────────────────────────────────────

def _make_stub_type(name):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    return type(name, (), {"__init__": __init__, "__repr__": lambda self: name})


class _FakeColumn:
    """Minimal Column stand-in so ``F.lit(...).cast(...).alias(...)`` chains work."""

    def __init__(self, label):
        self.label = label

    def cast(self, dtype):
        return _FakeColumn(f"cast({self.label} as {dtype})")

    def alias(self, name):
        return _FakeColumn(name)

    def __repr__(self):  # pragma: no cover - debugging aid
        return f"Column({self.label})"


def install_pyspark_stub(monkeypatch, conf_values=None):
    """Install a fake ``pyspark`` package and return the ScriptedSpark session."""
    spark = ScriptedSpark(conf_values)

    pyspark_mod = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")
    functions_mod = types.ModuleType("pyspark.sql.functions")
    window_mod = types.ModuleType("pyspark.sql.window")
    dbutils_mod = types.ModuleType("pyspark.dbutils")
    types_mod = types.ModuleType("pyspark.sql.types")

    class SparkSessionClass:
        class builder:  # noqa: N801 - mirrors the pyspark API surface
            @staticmethod
            def getOrCreate():
                return spark

        @staticmethod
        def getActiveSession():
            return spark

    class StorageLevel:
        MEMORY_AND_DISK = "MEMORY_AND_DISK"
        DISK_ONLY = "DISK_ONLY"

    def _row(**kwargs):
        return FakeRow(**kwargs)

    for fn_name in (
        "col",
        "lit",
        "expr",
        "current_timestamp",
        "current_date",
        "coalesce",
        "upper",
        "lower",
        "trim",
        "md5",
        "concat_ws",
        "to_date",
        "when",
        "count",
        "max",
        "min",
        "row_number",
    ):
        setattr(
            functions_mod,
            fn_name,
            (lambda name: lambda *a, **k: _FakeColumn(f"{name}({a})"))(fn_name),
        )

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
        "DecimalType",
        "ArrayType",
        "MapType",
    ):
        setattr(types_mod, type_name, _make_stub_type(type_name))

    pyspark_mod.StorageLevel = StorageLevel
    pyspark_mod.sql = sql_mod
    sql_mod.SparkSession = SparkSessionClass
    sql_mod.Row = _row
    sql_mod.DataFrame = FakeDataFrame
    sql_mod.functions = functions_mod
    sql_mod.types = types_mod
    window_mod.Window = _make_stub_type("Window")
    dbutils_mod.DBUtils = _make_stub_type("DBUtils")

    monkeypatch.setitem(sys.modules, "pyspark", pyspark_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql.window", window_mod)
    monkeypatch.setitem(sys.modules, "pyspark.dbutils", dbutils_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", types_mod)
    return spark


def import_module_from_path(module_name: str, relative_path: str, extra_sys_path=(), purge=()):
    """Import a framework module by file path, isolated from earlier imports.

    ``purge`` names sibling modules (imported by the target via plain
    ``import x``) that must be evicted from ``sys.modules`` first, otherwise
    they would keep the Spark session captured during an earlier test.
    """
    module_path = REPO_ROOT / relative_path
    previous_path = list(sys.path)
    sys.modules.pop(module_name, None)
    for name in purge:
        sys.modules.pop(name, None)
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
