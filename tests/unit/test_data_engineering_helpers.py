import math

import pytest
import yaml

from conftest import REPO_ROOT, import_module_from_path


def test_sanitize_col_normalizes_delta_unsafe_characters():
    sanitize = import_module_from_path(
        "silver_sanitize",
        r"databricks_bundle\drugdev\silver_framework\silver_engine\sanitize.py",
    )

    assert sanitize.sanitize_col("Disc. Rollback Approved?") == "disc_rollback_approved"
    assert sanitize.sanitize_col("_Study Site") == "_study_site"
    assert sanitize.sanitize_col(" ,;{}\r\n\t=.?`()") == "col"


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        ("kitten", "sitting", 3),
        ("study", "study", 0),
        ("", "abc", 3),
        ("abc", "", 3),
    ],
)
def test_levenshtein_distance(left, right, expected):
    levenshtein = import_module_from_path(
        "raw_levenshtein",
        r"ingest_koios_raw\scripts\levenshtein.py",
    )

    assert levenshtein.distance_levenshtein(left, right) == expected


def test_closest_matching_item_returns_lowest_distance_candidate():
    levenshtein = import_module_from_path(
        "manual_levenshtein",
        r"ingest_koios_manual\scripts\levenshtein.py",
    )

    assert levenshtein.get_closest_matching_item(
        "participant_visit", ["study", "participant", "participant_visits"]
    ) == "participant_visits"
    assert levenshtein.get_closest_matching_item("anything", []) is None


def test_generic_csv_to_json_writes_records_json(tmp_path):
    converter = import_module_from_path(
        "generic_csv_to_json",
        r"ingest_koios_raw\scripts\generic_csv_to_json.py",
    )
    source = tmp_path / "input.csv"
    destination = tmp_path / "output.json"
    source.write_text("study_id,status\nXL092-303,active\n", encoding="utf-8")

    converter.convert_to_json(source, destination)

    assert yaml.safe_load(destination.read_text(encoding="utf-8")) == [
        {"study_id": "XL092-303", "status": "active"}
    ]


def test_gold_objects_helpers_normalize_empty_and_json_values():
    gold = import_module_from_path(
        "gold_objects_yaml_generator",
        r"databricks_bundle\drugdev\gold_framework\configs\gold_objects_yaml_generator.py",
    )

    assert gold._split_csv("study_id, site_id, ,country") == ["study_id", "site_id", "country"]
    assert gold._split_csv(float("nan")) == []
    assert gold._to_int("7") == 7
    assert gold._to_int("bad", default=42) == 42
    assert gold._to_bool("yes") is True
    assert gold._to_bool("0") is False
    assert gold._safe_tags('{"domain":"ctms"}') == {"domain": "ctms"}
    assert gold._safe_tags("[1, 2]") == {}


def test_gold_objects_column_tags_keep_supported_metadata_only():
    gold = import_module_from_path(
        "gold_objects_yaml_generator_tags",
        r"databricks_bundle\drugdev\gold_framework\configs\gold_objects_yaml_generator.py",
    )

    assert gold._safe_column_tags(
        '{"subject_id":{"sensitivity":"restricted","pii_category":"subject_identifier","ignored":"x"},'
        '"empty":{},"  ":"bad"}'
    ) == {
        "subject_id": {
            "sensitivity": "RESTRICTED",
            "pii_category": "SUBJECT_IDENTIFIER",
        }
    }


def test_ingestion_config_generator_converts_scalar_and_list_values():
    generator_mod = import_module_from_path(
        "ingestion_yaml_generator",
        r"databricks_bundle\drugdev\ingestion_framework\configs\yaml_generator.py",
    )
    generator = generator_mod.ConfigGenerator()

    assert generator._convert_value("true", "is_active") is True
    assert generator._convert_value("FALSE", "contains_pii") is False
    assert generator._convert_value("365", "retention_days") == 365
    assert generator._convert_value("id, study_id ,", "primary_keys") == ["id", "study_id"]
    assert generator._convert_value("", "watermark_column") is None
    assert generator._convert_value(math.nan, "watermark_column") is None


def test_silver_yaml_helpers_parse_compound_fields_and_pii_metadata():
    silver = import_module_from_path(
        "silver_yaml_generator",
        r"databricks_bundle\drugdev\silver_framework\configs\silver_yaml_generator.py",
    )

    assert silver._split_csv("study_id, subject_id") == ["study_id", "subject_id"]
    assert silver._parse_rule_field('["subject_id", "study_id"]', "compound_unique") == [
        "subject_id",
        "study_id",
    ]
    assert silver._parse_rule_field("status", "not_null") == "status"
    assert silver._parse_pii_columns(
        '[{"column":"subject_id","pii_category":"subject_identifier","sensitivity":"restricted"},'
        '"site_id"]'
    ) == [
        {
            "column": "subject_id",
            "pii_category": "subject_identifier",
            "sensitivity": "RESTRICTED",
        },
        "site_id",
    ]
    assert silver._to_bool("Y") is True
    assert silver._to_bool("no") is False


def test_silver_entity_alignment_reports_orphan_mappings():
    silver = import_module_from_path(
        "silver_yaml_generator_alignment",
        r"databricks_bundle\drugdev\silver_framework\configs\silver_yaml_generator.py",
    )

    result = silver._validate_entity_alignment(
        datasets=[{"dataset_name": "dataset_a"}],
        mapping_rules={"dataset_a": {}, "orphan_dataset": {}},
        entity_val_rules={"validation_only": []},
    )

    assert result["dataset_names"] == {"dataset_a"}
    assert result["only_in_mappings"] == ["orphan_dataset"]
    assert result["only_in_validation"] == ["validation_only"]
