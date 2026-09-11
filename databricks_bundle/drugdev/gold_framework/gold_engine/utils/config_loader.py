# ── Spark conf ─────────────────────────────────────────────────────────────────
from datetime import datetime
import logging
logger = logging.getLogger(__name__)
def _get_conf(spark):
    if spark is None:
        raise ValueError("spark is required for _get_conf")

    required_keys = {
        "drugdev.METADATA_CATALOG",
        "drugdev.METADATA_SCHEMA",
        "drugdev.REGISTRY_SCHEMA",
        "drugdev.CATALOG",
        "drugdev.BRONZE_SCHEMA",
        "drugdev.SILVER_SCHEMA",
        "drugdev.GOLD_SCHEMA",
        "drugdev.environment",
    }
    conf = {}
    missing = []
    for key in required_keys:
        val = spark.conf.get(key, "")
        if not val:
            missing.append(key)
        conf[key.split(".")[-1]] = val

    if missing:
        raise ValueError(f"Missing required Spark conf(s): {', '.join(missing)}")

    conf["METADATA_CATALOG"] = spark.conf.get("drugdev.METADATA_CATALOG")
    conf["METADATA_SCHEMA"]  = spark.conf.get("drugdev.METADATA_SCHEMA")
    conf["REGISTRY_SCHEMA"]  = spark.conf.get("drugdev.REGISTRY_SCHEMA")
    conf["CATALOG"]          = spark.conf.get("drugdev.CATALOG")
    conf["BRONZE_SCHEMA"]    = spark.conf.get("drugdev.BRONZE_SCHEMA")
    conf["SILVER_SCHEMA"]    = spark.conf.get("drugdev.SILVER_SCHEMA")
    conf["GOLD_SCHEMA"]      = spark.conf.get("drugdev.GOLD_SCHEMA")
    conf["environment"]      = spark.conf.get("drugdev.environment")
    conf["RUN_DATE"]         = spark.conf.get(
        "drugdev.RUN_DATE", datetime.now().strftime("%Y%m%d")
    )
    logger.info(f"Spark conf: {conf}")
    return conf


def _quoted_catalog(spark) -> str:
    """
    Backtick-quoted catalog name for SQL identifiers.
    Unity Catalog names can contain hyphens; SQL requires backtick quoting.
    """
    config = _get_conf(spark)
    cat = config.get("CATALOG", "")
    return f"`{cat}`"

def _silver_schema(spark=None) -> str:
    if spark is None:
        raise ValueError("spark is required for _silver_schema")
    conf = _get_conf(spark)
    return conf.get("SILVER_SCHEMA", f"silver")

def _bronze_schema(spark=None) -> str:
    if spark is None:
        raise ValueError("spark is required for _bronze_schema")
    conf = _get_conf(spark)
    return conf.get("BRONZE_SCHEMA", f"bronze")

def _gold_schema(spark=None) -> str:
    if spark is None:
        raise ValueError("spark is required for _gold_schema")
    conf = _get_conf(spark)
    return conf.get("GOLD_SCHEMA", f"gold")

def _common_schema(spark=None) -> str:
    if spark is None:
        raise ValueError("spark is required for _common_schema")
    conf = _get_conf(spark)
    return conf.get("REGISTRY_SCHEMA", f"common")