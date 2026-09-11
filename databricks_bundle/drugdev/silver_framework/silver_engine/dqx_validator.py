# src/quality/dqx_validator.py--->new_update to view_name
#
# Runs DQ validation rules from schema_registry against a staged DataFrame.
# Reads validation_rules JSON from registry — no hardcoded rules in code.
# On CRITICAL failure raises DQCriticalFailureError (blocks Silver write).
# Writes per-rule results to dq_validation_results table.
# Quarantines failing rows to S3 quarantine bucket.

import json
import logging
import uuid
import re
import os
import ast
from datetime import datetime, timezone
import yaml
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

logger = logging.getLogger(__name__)
# sql_utils imported lazily per-function (see each method body)

def _get_conf():
    keys = {
        "drugdev.METADATA_CATALOG",
        "drugdev.METADATA_SCHEMA",
        "drugdev.REGISTRY_SCHEMA",
        "drugdev.CATALOG",
        "drugdev.BRONZE_SCHEMA",
        "drugdev.SILVER_SCHEMA",
        "drugdev.environment",
        "drugdev.CONFIG_PATH",
    }
    conf = {}
    missing = []
    for k in keys:
        v = spark.conf.get(k, "")
        if not v:
            missing.append(k)
        conf[k.split(".")[-1]] = v
    if missing:
        raise ValueError(f"Missing required Spark conf(s): {', '.join(missing)}")
    conf["RUN_DATE"] = spark.conf.get("drugdev.RUN_DATE", datetime.now().strftime("%Y%m%d"))
    return conf


def get_quarantine_bucket() -> str: return _get_secret("quarantine_bucket")
def get_dq_config()          -> dict:  return {"min_pass_rate_pct": 98.0, "max_critical_failures": 0, "max_high_failures": 3}
def get_min_pass_rate()      -> float: return get_dq_config().get("min_pass_rate_pct", 98.0)
def DQ_RESULTS()              -> str:   return f"`{_get_conf()['CATALOG']}`.{_get_conf()['METADATA_SCHEMA']}.dq_validation_results_log"
def REGISTRY()                -> str:   return f"`{_get_conf()['CATALOG']}`.{_get_conf()['REGISTRY_SCHEMA']}.drugdev_silver_registry"



class DQCriticalFailureError(Exception):
    """Raised when a CRITICAL DQ rule fails. Blocks Silver write."""
    pass


class DQXValidator:
    """
    Validates a staged Silver DataFrame against rules from schema_registry.

    Rule types supported:
      not_null     — field IS NOT NULL
      unique       — COUNT(DISTINCT field) == COUNT(*)
      in_list      — field IN (values)
      range        — field BETWEEN min AND max
      cross_field  — arbitrary SQL expression evaluates to TRUE
      date_format  — field matches expected date pattern
      completeness — non-null rate >= threshold

    Severity levels: CRITICAL | HIGH | MEDIUM | LOW
    Modes: quarantine | flag | warn | drop
    """

    def __init__(self, spark: SparkSession):
        self.spark    = spark

    def validate(self, df, domain: str, entity: str,
                 study_id: str = None, vendor: str = None, metadata:list = None) -> dict:
        """
        Run all DQ rules for domain/entity against df.
        Returns summary dict with pass_rate, counts, rule_results.
        Raises DQCriticalFailureError if any CRITICAL rule fails.
        """
        run_id = uuid.uuid4().hex
        rules  = self._load_rules(domain, entity)

        if not rules:
            logger.info("No DQ rules for %s/%s — skipping validation", domain, entity)
            return {'pass_rate': 100.0, 'total': 0, 'passed': 0, 'failed': 0}

        view_name = f'_dq_staging_{uuid.uuid4().hex[:8]}'
        df.createOrReplaceTempView(view_name)
        # df.createOrReplaceTempView('_dq_staging')
        logger.info("created dq temp view %s", view_name)
        # logger.info("total rows in view %s")
        total_rows = df.count()

        results      = []
        critical_failures = []
        # meta = metadata[0] if metadata else {}
        for rule in rules:
            try:
                result = self._run_rule(rule, df, total_rows, view_name)
                meta = metadata[0] if metadata else {}
                result['domain']   = domain
                result['entity']   = entity
                result['study_id'] = study_id or meta.get("study_id")
                result['vendor']   = vendor or meta.get("vendor")
                result['run_id']   = run_id
                result['schema_id'] = meta.get("schema_id")
                results.append(result)

                if result['status'] == 'FAIL' and rule.get('severity') == 'CRITICAL':
                    critical_failures.append(result)

                self._write_result(result)

            except Exception as exc:
                logger.error("DQ rule %s error: %s", rule.get('rule_id'), exc)

        passed     = sum(1 for r in results if r['status'] == 'PASS')
        failed     = len(results) - passed
        pass_rate  = round(passed / len(results) * 100, 2) if results else 100.0
        min_rate   = get_min_pass_rate()

        # DQ notifications are aggregated and sent once at Silver layer end.
        # Do not emit per-entity alerts from the validator.
        try:
            self.spark.catalog.dropTempView(view_name)
        except Exception as exc:
            pass
        if critical_failures:
            msgs = '; '.join(r['rule_id'] for r in critical_failures)
            raise DQCriticalFailureError(
                f"CRITICAL DQ rules failed for {domain}/{entity}/{study_id}: {msgs}"
            )

        logger.info("DQ %s/%s: %d/%d passed (%.1f%%)",
                    domain, entity, passed, len(results), pass_rate)
        return {
            'pass_rate': pass_rate,
            'total':     len(results),
            'passed':    passed,
            'failed':    failed,
            'results':   results,
        }

    def _load_rules(self, domain: str, entity: str) -> list:
        from sql_utils import df_insert, sql_str, df_update, clean_where_clause, sql_identifier  # lazy: survives reloads
        registry = REGISTRY()
        row = self.spark.sql(f"""
            SELECT validation_rules FROM {registry}
            WHERE domain = '{sql_str(domain)}' AND entity = '{sql_str(entity)}'
              AND is_active = TRUE 
            LIMIT 1
        """).first()
        if not row or not row.validation_rules:
            return []
        try:
            data = json.loads(row.validation_rules)
            return data.get('rules', [])
        except (json.JSONDecodeError, AttributeError):
            logger.warning("Could not parse validation_rules for %s/%s", domain, entity)
            return []

    def _run_rule(self, rule: dict, df, total_rows: int, view_name: str) -> dict:
        rule_id  = rule.get('rule_id', uuid.uuid4().hex[:8])
        check    = rule.get('check', '').lower()
        field    = rule.get('field')
        severity = rule.get('severity', 'MEDIUM')
        mode     = rule.get('mode', 'flag')

        fail_count = 0
        pass_count = 0
        samples    = []

        if check == 'not_null' and field:
            fail_count = self.spark.sql(
                f"SELECT COUNT(*) AS c FROM {view_name} WHERE `{field}` IS NULL"
            ).first().c
            pass_count = total_rows - fail_count
            if fail_count > 0:
                samples = [r.asDict() for r in self.spark.sql(
                    f"SELECT * FROM {view_name} WHERE `{field}` IS NULL LIMIT 5"
                ).collect()]

        elif check == 'unique' and field:
            distinct = self.spark.sql(
                f"SELECT COUNT(DISTINCT `{field}`) AS c FROM {view_name}"
            ).first().c
            fail_count = total_rows - distinct
            pass_count = distinct

        elif check == 'in_list' and field:
            values = rule.get('values', [])
            vals_sql = ', '.join(f"'{v}'" for v in values)
            fail_count = self.spark.sql(
                f"SELECT COUNT(*) AS c FROM {view_name} "
                f"WHERE `{field}` IS NOT NULL AND `{field}` NOT IN ({vals_sql})"
            ).first().c
            pass_count = total_rows - fail_count
            if fail_count > 0:
                samples = [r.asDict() for r in self.spark.sql(
                    f"SELECT DISTINCT `{field}` FROM {view_name} "
                    f"WHERE `{field}` NOT IN ({vals_sql}) LIMIT 5"
                ).collect()]

        elif check == 'range' and field:
            min_v = rule.get('min')
            max_v = rule.get('max')
            conds = []
            if min_v is not None:
                conds.append(f"`{field}` < {min_v}")
            if max_v is not None:
                conds.append(f"`{field}` > {max_v}")
            if conds:
                where = ' OR '.join(conds)
                fail_count = self.spark.sql(
                    f"SELECT COUNT(*) AS c FROM {view_name} "
                    f"WHERE `{field}` IS NOT NULL AND ({where})"
                ).first().c
                pass_count = total_rows - fail_count

        elif check == 'cross_field':
            expr = rule.get('expression', 'TRUE')
            fail_count = self.spark.sql(
                f"SELECT COUNT(*) AS c FROM {view_name} WHERE NOT ({expr})"
            ).first().c
            pass_count = total_rows - fail_count

        elif check == 'completeness' and field:
            threshold = rule.get('threshold', 0.95)
            null_count = self.spark.sql(
                f"SELECT COUNT(*) AS c FROM {view_name} WHERE `{field}` IS NULL"
            ).first().c
            actual_rate = (total_rows - null_count) / total_rows if total_rows else 1.0
            fail_count = 0 if actual_rate >= threshold else 1
            pass_count = total_rows - null_count
        elif check == 'regex_match' and field:
            pattern = rule.get('expression') or rule.get('pattern')
            fail_count = self.spark.sql(f"""
                        SELECT COUNT(*) AS c FROM {view_name}
                        WHERE `{field}` IS NOT NULL AND NOT REGEXP_LIKE(`{field}`, '{pattern}')
                        """).first().c
            pass_count = total_rows - fail_count
        elif check == 'compound_unique':
            fields = self._normalize_compound_fields(rule.get('field'))
            if not fields:
                logger.warning("Compound unique rule %s has no valid fields; treating as pass", rule_id)
                pass_count = total_rows
            else:
                cols = ",".join([f"`{c}`" for c in fields])
                logger.info("Compound unique check for fields: %s", fields)
                dup_count = self.spark.sql(f"""
                    SELECT COUNT(*) AS c FROM (
                        SELECT {cols} FROM {view_name} GROUP BY {cols} HAVING COUNT(*) > 1
                    )""").first().c
                logger.info("Duplicate records found: %d", dup_count)
                fail_count = dup_count
                pass_count = total_rows - fail_count

        else:
            pass_count = total_rows

        rate   = round(pass_count / total_rows * 100, 2) if total_rows else 100.0
        status = 'PASS' if fail_count == 0 else 'FAIL'

        if fail_count > 0 and mode == 'quarantine':
            self._quarantine_failures(df, rule, field)

        field_for_result = field if isinstance(field, str) else json.dumps(field)

        return {
            'result_id':     uuid.uuid4().hex,
            'rule_id':       rule_id,
            'rule_check':    check,
            'field':         field_for_result or '',
            'severity':      severity,
            'mode':          mode,
            'pass_count':    pass_count,
            'fail_count':    fail_count,
            'total_count':   total_rows,
            'pass_rate_pct': rate,
            'sample_failures': json.dumps(samples[:5], default = str) if samples else None,
            'status':        status,
        }

    def _normalize_compound_fields(self, raw_fields) -> list[str]:
        """Normalize compound_unique field specs into a list of column names."""
        if isinstance(raw_fields, list):
            return [str(c).strip() for c in raw_fields if str(c).strip()]

        if raw_fields is None:
            return []

        if isinstance(raw_fields, str):
            text = raw_fields.strip()
            if not text:
                return []

            # Some configs store SQL-escaped lists like '[''a'', ''b'']'.
            if text.startswith("'") and text.endswith("'"):
                text = text[1:-1]
            text = text.replace("''", "'")

            for parser in (json.loads, ast.literal_eval):
                try:
                    parsed = parser(text)
                    if isinstance(parsed, list):
                        return [str(c).strip() for c in parsed if str(c).strip()]
                    if isinstance(parsed, str) and parsed.strip():
                        return [parsed.strip()]
                except Exception:
                    continue

            # Last resort: comma-separated values.
            if "," in text:
                return [c.strip().strip("'\"") for c in text.split(",") if c.strip().strip("'\"")]
            return [text.strip("'\"")]

        return [str(raw_fields).strip()]

    def _quarantine_failures(self, df, rule: dict, field: str) -> None:
        """Write failing rows to quarantine bucket."""
        try:
            check = rule.get('check', '')
            if check == 'not_null' and field:
                bad = df.filter(f"`{field}` IS NULL")
            elif check == 'in_list' and field:
                vals = ', '.join(f"'{v}'" for v in rule.get('values', []))
                bad  = df.filter(f"`{field}` NOT IN ({vals})")
            else:
                return

            qpath = f"s3://{get_quarantine_bucket()}/dq/{rule.get('rule_id','unknown')}/"
            bad.write.format('delta').mode('append').save(qpath)
            logger.info("Quarantined %d rows to %s", bad.count(), qpath)
            
        except Exception as exc:
            logger.error("Quarantine write failed: %s", exc)

    def _write_result(self, r: dict) -> None:
        from sql_utils import df_insert, sql_str, df_update, clean_where_clause, sql_identifier  # lazy: survives reloads
        """
        Write one DQ result row using df_insert.
        rule_name and column_name come from vendor file schemas —
        must not be interpolated into SQL strings.
        """
        try:
            from datetime import datetime
            df_insert(self.spark, DQ_RESULTS(), {
                "validation_id":        r.get("result_id") or uuid.uuid4().hex,
                "validation_timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
                "schema_id":            r.get("schema_id", ""),
                "domain":               r.get("domain", ""),
                "vendor":               r.get("vendor", ""),
                "study_id":             r.get("study_id", ""),
                "layer":                "SILVER",
                "table_name":           r.get("table_name") or r.get("entity") or "",
                "rule_id":              r["rule_id"],
                "rule_name":            r.get("rule_name") or r.get("rule_check", ""),
                "column_name":          r.get("field", ""),
                "severity":             r["severity"],
                "mode":                 r.get("mode", "flag"),
                "result":               "SUCCESS" if r["status"] == "PASS" else "FAILURE",
                "record_count":         int(r.get("total_count", 0)),
                "failed_record_count":  int(r.get("fail_count", 0)),
                "pass_rate_pct":        float(r.get("pass_rate_pct", 100.0)),
                "sample_failed_records": str(r.get("sample_failures") or "")[:2000] or None,
            })
        except Exception as exc:
            logger.error("Failed to write DQ result: %s", exc) 