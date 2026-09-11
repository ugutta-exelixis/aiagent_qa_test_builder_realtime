# src/monitoring/alert_notifier.py
#
# Sends alerts via AWS SES email and/or MS Teams webhook.
# Reads config from dev.yaml notifications block.
# Deduplicates: same alert_type+domain+title within 1 hour is suppressed.
# Logs every alert (sent or suppressed) to alert_history table.

import hashlib
import json
import logging
import uuid
import yaml
import re
import os
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

logger = logging.getLogger(__name__)


# Import safe SQL utilities — alert fields are free-text from vendor systems
from sql_utils import df_insert, df_update, sql_str, clean_where_clause


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


class AlertNotifier:
    """
    Unified alerting for email (AWS SES or SMTP) and MS Teams webhooks.
    All alert logic in the platform routes through this class.
    """

    def __init__(self, cfg: dict):
        self.cfg          = cfg
        self.notif        = cfg.get('notifications', {})
        self.email_cfg    = self.notif.get('email', {})
        self.teams_cfg    = self.notif.get('teams', {})
        self.routing      = self.notif.get('routing', {})
        self.domain_rcpts = self.notif.get('domain_recipients', {})
        self._spark       = None   # injected lazily
        self._tbl         = None
        self.conf         = _get_conf()

    def _init_spark(self):
        if self._spark is None:
            from pyspark.sql import SparkSession
            # from config_loader import ALERT_HISTORY
            self._spark = SparkSession.getActiveSession()
            self._tbl   = f"`{self.conf['CATALOG']}`.{self.conf['METADATA_SCHEMA']}.alert_history"

    def send_alert(
        self,
        alert_type: str,
        severity:   str,
        title:      str,
        message:    str,
        context:    dict = None,
        domain:     str  = None,
        vendor:     str  = None,
        entity:     str  = None,
    ) -> bool:
        """
        Route and send an alert. Returns True if sent, False if suppressed.
        """
        self._init_spark()
        alert_id  = uuid.uuid4().hex
        context   = context or {}
        channels  = []

        # Dedup check — suppress identical alert within 1 hour
        if self._is_duplicate(alert_type, domain, title):
            logger.info("Alert suppressed (duplicate): %s / %s", alert_type, title)
            self._log_alert(alert_id, alert_type, severity, domain, vendor, entity,
                            title, message, context, [], status='SUPPRESSED')
            return False

        # Determine routing
        route = self.routing.get(severity, {})
        send_email = self.email_cfg.get('enabled', False)
        send_teams = self.teams_cfg.get('enabled', False) and route.get('teams', False)

        if send_email:
            recipients = self._resolve_recipients(severity, domain)
            if recipients:
                try:
                    self._send_email(title, message, context, recipients, severity)
                    channels.append('email')
                except Exception as exc:
                    logger.error("Email send failed: %s", exc)

        if send_teams:
            try:
                self._send_teams(title, message, context, severity, domain)
                channels.append('teams')
            except Exception as exc:
                logger.error("Teams send failed: %s", exc)

        self._log_alert(alert_id, alert_type, severity, domain, vendor, entity,
                        title, message, context, channels, status='SENT')
        logger.info("Alert sent [%s] %s via %s", severity, title, channels)
        return True

    def _resolve_recipients(self, severity: str, domain: str = None) -> list:
        """
        Return email recipient list for an alert.

        Routing priority (DDDA Tagging Standard §7.1):
          1. domain_recipients config block (dev.yaml) — explicit per-domain
             on-call list keyed on the domain tag value (IRT, EDC, CTMS, IPP,
             COMMON). This is the primary routing mechanism.
          2. Unity Catalog tag fallback — if domain_recipients has no entry for
             the supplied domain, query the table's domain tag from
             information_schema.table_tags and use that to look up recipients.
             This handles cases where callers pass a table name rather than a
             domain code, and ensures tag-driven routing is the source of truth.
          3. Default severity-based recipient list from email.recipients config.

        A table with no domain tag routes to the generic data-engineering inbox
        (the default severity list). This is slower to triage — always tag tables.
        """
        # Priority 1: explicit domain config
        if domain and domain.upper() in self.domain_rcpts:
            return self.domain_rcpts[domain.upper()].get('email', [])

        # Priority 2: normalise and retry (handles lower-case domain values)
        if domain:
            domain_upper = domain.upper()
            if domain_upper in self.domain_rcpts:
                return self.domain_rcpts[domain_upper].get('email', [])

        # Priority 3: default severity-based list
        return self.email_cfg.get('recipients', {}).get(severity.lower(), [])

    def _send_email(self, title, message, context, recipients, severity):
        """Send via AWS SES (default) or SMTP."""
        driver = self.email_cfg.get('smtp_driver', 'ses')
        body   = self._format_email_body(title, message, context, severity)

        if driver == 'ses':
            import boto3
            ses = boto3.client('ses', region_name=self.email_cfg.get('ses_region', 'us-east-1'))
            ses.send_email(
                Source     = self.email_cfg['from_address'],
                Destination= {'ToAddresses': recipients},
                Message    = {
                    'Subject': {'Data': f"[DDDA {severity}] {title}"},
                    'Body':    {'Html': {'Data': body}},
                },
                ReplyToAddresses = [self.email_cfg.get('reply_to', self.email_cfg['from_address'])],
            )
        else:
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[DDDA {severity}] {title}"
            msg['From']    = self.email_cfg['from_address']
            msg['To']      = ', '.join(recipients)
            msg.attach(MIMEText(body, 'html'))
            with smtplib.SMTP(self.email_cfg['smtp_server'],
                              self.email_cfg.get('smtp_port', 587)) as s:
                if self.email_cfg.get('use_tls', True):
                    s.starttls()
                if self.email_cfg.get('smtp_user'):
                    s.login(self.email_cfg['smtp_user'], self.email_cfg['smtp_password'])
                s.sendmail(self.email_cfg['from_address'], recipients, msg.as_string())

    def _send_teams(self, title, message, context, severity, domain):
        """Post adaptive card to MS Teams webhook."""
        import urllib.request
        webhook_url = self.teams_cfg.get('webhook_url', '')
        if not webhook_url or webhook_url.startswith('{{'):
            logger.warning("Teams webhook URL not configured")
            return

        color_map = {'CRITICAL': 'attention', 'HIGH': 'warning',
                     'MEDIUM': 'accent',      'LOW': 'good'}
        card = {
            'type': 'message',
            'attachments': [{
                'contentType': 'application/vnd.microsoft.card.adaptive',
                'content': {
                    '$schema': 'http://adaptivecards.io/schemas/adaptive-card.json',
                    'type':    'AdaptiveCard',
                    'version': '1.4',
                    'body': [
                        {'type': 'TextBlock', 'text': f"[{severity}] {title}",
                         'weight': 'Bolder', 'size': 'Medium',
                         'color': color_map.get(severity, 'default')},
                        {'type': 'TextBlock', 'text': message,
                         'wrap': True},
                        {'type': 'FactSet',
                         'facts': [{'title': k, 'value': str(v)}
                                   for k, v in context.items()]},
                    ],
                },
            }],
        }
        payload = json.dumps(card).encode('utf-8')
        req = urllib.request.Request(
            webhook_url,
            data    = payload,
            headers = {'Content-Type': 'application/json'},
        )
        urllib.request.urlopen(req, timeout=10)

    def _is_duplicate(self, alert_type: str, domain: str, title: str) -> bool:
        """Return True if same alert was sent within the last hour."""
        if not self._spark or not self._tbl:
            return False
        try:
            count = self._spark.sql(f"""
                SELECT COUNT(*) AS c FROM {self._tbl}
                WHERE alert_type = '{alert_type}'
                  AND domain     = '{sql_str(domain or "")}'
                  AND title      = '{sql_str(title)}'
                  AND status     = 'SENT'
                  AND alert_timestamp   >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
            """).first().c
            return count > 0
        except Exception:
            return False

    def _log_alert(self, alert_id, alert_type, severity, domain, vendor,
                   entity, title, message, context, channels, status):
        """
        Write to alert_history using DataFrame insert.
        Title and message are free-text from vendor systems — must never
        be interpolated into SQL strings directly.
        """
        if not self._spark or not self._tbl:
            return
        try:
            from datetime import datetime
            teams_s = 'teams' in channels
            email_s = 'email' in channels
            df_insert(self._spark, self._tbl, {
                "alert_id":        alert_id,
                "alert_type":      alert_type,
                "severity":        severity,
                "domain":          str(domain or ''),
                "vendor":          str(vendor or ''),
                "entity":          str(entity or ''),
                "title":           str(title or '')[:300],
                "message":         str(message or '')[:500],
                "status":          status,
                "alert_timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
                "teams_sent":      teams_s,
                "email_sent":      email_s,
            })
        except Exception as exc:
            logger.error("Failed to log alert to history: %s", exc)

    def _format_email_body(self, title, message, context, severity) -> str:
        ctx_rows = ''.join(
            f"<tr><td><b>{k}</b></td><td>{v}</td></tr>"
            for k, v in context.items()
        )
        color = {'CRITICAL': '#d32f2f', 'HIGH': '#f57c00',
                 'MEDIUM': '#1976d2', 'LOW': '#388e3c'}.get(severity, '#555')
        return f"""
        <html><body>
        <h2 style="color:{color}">[{severity}] {title}</h2>
        <p>{message}</p>
        <table border="1" cellpadding="4" style="border-collapse:collapse">
          {ctx_rows}
        </table>
        <hr/>
        <small>DDDA Clinical Data Platform — {datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M UTC')}</small>
        </body></html>
        """
