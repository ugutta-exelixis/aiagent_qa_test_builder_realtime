"""Unit tests for the Silver transformer helper layer.

These cover the deterministic pieces used on every Bronze → Silver run: fully
qualified name construction, Bronze/Silver table naming conventions, column
cleaning, transformation-logic parsing, and the SQL expression splitter that
keeps CASE blocks and nested function calls intact.
"""

import pytest

from conftest import REPO_ROOT, import_module_from_path


SILVER_ENGINE_DIR = (
    REPO_ROOT / "databricks_bundle" / "drugdev" / "silver_framework" / "silver_engine"
)
SILVER_TRANSFORMER_PATH = (
    r"databricks_bundle\drugdev\silver_framework\silver_engine\silver_transformer.py"
)


@pytest.fixture
def transformer_module(stub_pyspark):
    return import_module_from_path(
        "silver_transformer_unit",
        SILVER_TRANSFORMER_PATH,
        extra_sys_path=[SILVER_ENGINE_DIR],
    )


@pytest.fixture
def transformer(transformer_module, stub_pyspark):
    return transformer_module.SilverTransformer(stub_pyspark)


# ── Configuration ──────────────────────────────────────────────────────────────

def test_conf_is_read_from_spark_conf(transformer_module):
    conf = transformer_module._get_conf()

    assert conf["BRONZE_SCHEMA"] == "bronze_schema"
    assert conf["SILVER_SCHEMA"] == "silver_schema"
    assert conf["CONFIG_PATH"] == "unit-config.yml"
    assert conf["RUN_DATE"] == "20260901"


def test_conf_lists_all_missing_keys(transformer_module, stub_pyspark):
    stub_pyspark.conf.values["drugdev.BRONZE_SCHEMA"] = ""
    stub_pyspark.conf.values["drugdev.CONFIG_PATH"] = ""

    with pytest.raises(ValueError) as excinfo:
        transformer_module._get_conf()

    message = str(excinfo.value)
    assert "drugdev.BRONZE_SCHEMA" in message
    assert "drugdev.CONFIG_PATH" in message


# ── Naming conventions ─────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    ("layer", "expected"),
    [
        ("bronze", "`drugdev-catalog`.bronze_schema"),
        ("SILVER", "`drugdev-catalog`.silver_schema"),
        ("Common", "`drugdev-catalog`.meta_schema"),
    ],
)
def test_build_fqn_maps_layers_to_schemas(transformer_module, layer, expected):
    assert transformer_module.build_fqn("ctms", layer) == expected


def test_build_fqn_appends_table_when_provided(transformer_module):
    assert (
        transformer_module.build_fqn("ctms", "silver", "dim_site")
        == "`drugdev-catalog`.silver_schema.dim_site"
    )


def test_build_fqn_rejects_unknown_layers(transformer_module):
    with pytest.raises(ValueError, match="not recognised"):
        transformer_module.build_fqn("ctms", "raw")


def test_bronze_table_name_normalizes_vendor_and_study(transformer_module):
    assert transformer_module.build_bronze_table_name(
        "clinical", "Medidata Rave", "XL092-303", "participant"
    ) == "`drugdev-catalog`.bronze_schema.medidatarave_xl092_303_participant"


def test_bronze_table_name_truncates_long_vendor_names(transformer_module):
    assert transformer_module.build_bronze_table_name(
        "clinical", "Very-Long Vendor Name Inc", "XL092-303", "site"
    ) == "`drugdev-catalog`.bronze_schema.verylongvend_xl092_303_site"


def test_bronze_table_name_uses_global_token_for_cross_study_datasets(transformer_module):
    assert transformer_module.build_bronze_table_name(
        "clinical", "Veeva", "_GLOBAL", "country"
    ) == "`drugdev-catalog`.bronze_schema.veeva_global_country"


def test_silver_table_name_switches_between_staging_and_final(transformer_module):
    assert transformer_module.build_silver_table_name("CTMS", "site") == (
        "`drugdev-catalog`.silver_schema.site"
    )
    assert transformer_module.build_silver_table_name("CTMS", "site", staging=True) == (
        "`drugdev-catalog`.silver_schema.ctms_site_staging"
    )


# ── Column cleaning ────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  study_id  ", "study_id"),
        ("study\nid", "studyid"),
        ("study\u00a0id", "studyid"),
        ("\ufeffstudy_id\u200b", "study_id"),
        ("study_id", "study_id"),
    ],
)
def test_clean_col_removes_invisible_whitespace(transformer_module, raw, expected):
    assert transformer_module.SilverTransformer._clean_col(raw) == expected


# ── transformation_logic parsing ───────────────────────────────────────────────

def test_parse_transformation_logic_returns_empty_for_blank_input(transformer):
    assert transformer._parse_transformation_logic("") == ("", None)
    assert transformer._parse_transformation_logic(None) == ("", None)


def test_parse_transformation_logic_reads_structured_dict(transformer):
    raw = (
        "{'standard': 'cast(study_id as string) as study_id', "
        "'post_processing': {'deduplicate': True}}"
    )

    standard_sql, post_processing = transformer._parse_transformation_logic(raw)

    assert standard_sql == "cast(study_id as string) as study_id"
    assert post_processing == {"deduplicate": True}


def test_parse_transformation_logic_parses_yaml_post_processing_block(transformer):
    raw = "{'standard': 'upper(country) as country', 'post_processing': 'deduplicate: true'}"

    standard_sql, post_processing = transformer._parse_transformation_logic(raw)

    assert standard_sql == "upper(country) as country"
    assert post_processing == {"deduplicate": True}


def test_parse_transformation_logic_falls_back_to_legacy_sql_string(transformer):
    raw = "cast(site_id as string) as site_id"

    assert transformer._parse_transformation_logic(raw) == (raw, None)


def test_parse_transformation_logic_caches_repeat_lookups(transformer):
    raw = "cast(site_id as string) as site_id"

    first = transformer._parse_transformation_logic(raw)
    second = transformer._parse_transformation_logic(raw)

    assert first is second
    assert raw in transformer._transformation_cache


# ── SQL expression splitting ───────────────────────────────────────────────────

def test_split_sql_expressions_handles_plain_comma_separated_list(transformer_module):
    assert transformer_module._split_sql_expressions(
        "cast(a as string) as a, upper(b) as b"
    ) == ["cast(a as string) as a", "upper(b) as b"]


def test_split_sql_expressions_keeps_case_blocks_intact(transformer_module):
    blob = (
        "cast(study_id as string) as study_id,\n"
        "case when status = 'A' then 'Active, Enrolled' else 'Other' end as status_label,\n"
        "concat(first_name, ', ', last_name) as full_name"
    )

    expressions = transformer_module._split_sql_expressions(blob)

    assert len(expressions) == 3
    assert expressions[1].startswith("case when status = 'A'")
    assert expressions[1].endswith("as status_label")
    assert expressions[2] == "concat(first_name, ', ', last_name) as full_name"


def test_split_sql_expressions_handles_nested_case_statements(transformer_module):
    blob = (
        "case when a = 1 then case when b = 2 then 'x' else 'y' end else 'z' end as nested,\n"
        "b as plain"
    )

    expressions = transformer_module._split_sql_expressions(blob)

    assert len(expressions) == 2
    assert expressions[0].endswith("as nested")
    assert expressions[1] == "b as plain"


def test_split_sql_expressions_preserves_backtick_identifiers(transformer_module):
    expressions = transformer_module._split_sql_expressions(
        "`col, with comma` as safe_col, other_col"
    )

    assert expressions == ["`col, with comma` as safe_col", "other_col"]


@pytest.mark.parametrize("blob", ["", "   ", ",", " , , "])
def test_split_sql_expressions_ignores_empty_fragments(transformer_module, blob):
    assert transformer_module._split_sql_expressions(blob) == []
