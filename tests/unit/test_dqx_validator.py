import pytest

from conftest import import_module_from_path


def test_dq_config_defaults_are_stable(stub_pyspark):
    dqx = import_module_from_path(
        "dqx_validator_defaults",
        r"databricks_bundle\drugdev\silver_framework\silver_engine\dqx_validator.py",
    )

    assert dqx.get_dq_config() == {
        "min_pass_rate_pct": 98.0,
        "max_critical_failures": 0,
        "max_high_failures": 3,
    }
    assert dqx.get_min_pass_rate() == 98.0


def test_dqx_registry_table_names_use_spark_conf(stub_pyspark):
    dqx = import_module_from_path(
        "dqx_validator_tables",
        r"databricks_bundle\drugdev\silver_framework\silver_engine\dqx_validator.py",
    )

    assert dqx.DQ_RESULTS() == "`drugdev-catalog`.meta_schema.dq_validation_results_log"
    assert dqx.REGISTRY() == "`drugdev-catalog`.registry_schema.drugdev_silver_registry"


@pytest.mark.parametrize(
    ("raw_fields", "expected"),
    [
        (None, []),
        ("", []),
        ([" subject_id ", "", "study_id"], ["subject_id", "study_id"]),
        ('["subject_id", "study_id"]', ["subject_id", "study_id"]),
        ("[''subject_id'', ''study_id'']", ["subject_id", "study_id"]),
        ("subject_id, study_id", ["subject_id", "study_id"]),
        ("subject_id", ["subject_id"]),
    ],
)
def test_dqx_normalize_compound_fields_accepts_config_formats(stub_pyspark, raw_fields, expected):
    dqx = import_module_from_path(
        "dqx_validator_compound_fields",
        r"databricks_bundle\drugdev\silver_framework\silver_engine\dqx_validator.py",
    )
    validator = dqx.DQXValidator(spark=object())

    assert validator._normalize_compound_fields(raw_fields) == expected


def test_get_quarantine_bucket_exposes_missing_secret_helper(stub_pyspark):
    dqx = import_module_from_path(
        "dqx_validator_quarantine",
        r"databricks_bundle\drugdev\silver_framework\silver_engine\dqx_validator.py",
    )

    with pytest.raises(NameError, match="_get_secret"):
        dqx.get_quarantine_bucket()
