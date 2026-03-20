from __future__ import annotations

from dataclasses import dataclass


# Подключения. Заполняются прямо здесь.
ZBX_URL = ""
ZBX_USER = ""
ZBX_PASSWORD = ""

GRAFANA_URL = ""
GRAFANA_USER = ""
GRAFANA_PASSWORD = ""
GRAFANA_ORGIDS: tuple[int, ...] = ()
GRAFANA_AUDIT_ORGIDS: tuple[int, ...] = ()


# Scope нового контура.
SCOPE_AS: tuple[str, ...] = ()
SCOPE_ENV: str = ""


# Runtime.
HTTP_TIMEOUT_SEC: int = 90
MONITORED_HOSTS_ONLY: bool = False
ENABLE_GRAFANA: bool = True
OUTPUT_DIR: str = "v2_output"
OUTPUT_PREFIX: str = "scope_audit_v2"
AUDIT_LOG_PREFIX: str = "audit_log_v2"
GRAFANA_REPORT_PREFIX: str = "grafana_audit_v2"
GRAFANA_ORG_AUDIT_PREFIX: str = "grafana_org_audit"
GRAFANA_ORG_AUDIT_LOG_PREFIX: str = "grafana_org_audit_log"
MAPPING_PLAN_PREFIX: str = "mapping_plan_v2"
IMPACT_PLAN_PREFIX: str = "impact_plan_v2"
BACKUP_PREFIX: str = "scope_backup_v2"
GROUP_SAMPLE_HOSTS: int = 10
SAVE_JSON_INVENTORY: bool = True


# Входные файлы для plan/backup/verify.
SOURCE_AUDIT_JSON: str = ""
SOURCE_MAPPING_PLAN_XLSX: str = ""
SOURCE_IMPACT_PLAN_JSON: str = ""
SOURCE_BACKUP_FILE: str = ""


# Теги и исключения.
TAG_AS: str = "AS"
TAG_ASN: str = "ASN"
TAG_ENV: str = "ENV"
TAG_GAS: str = "GAS"
TAG_GUEST_NAME: str = "GUEST-NAME"

UNKNOWN_TAG_VALUE: str = "UNKNOWN"
UNKNOWN_GROUP_NAME: str = "UNKNOWN"
EXCLUDE_UNKNOWN_FROM_STATS: bool = True

ENV_PROD_LABEL: str = "PROD"
ENV_NONPROD_LABEL: str = "NONPROD"
PROD_ENV_VALUES: tuple[str, ...] = ("PROD",)

MAPPING_MIN_INTERSECTION: int = 2
MAPPING_MIN_OLD_COVERAGE: float = 0.20
MAPPING_MIN_NEW_COVERAGE: float = 0.20
MAPPING_FORBID_ENV_MISMATCH: bool = True

EXCLUDED_GROUP_PATTERNS: tuple[str, ...] = (
    r"^Maintenance-dc-enable$",
    r"^Maintenance-",
    r"^(BNK|DOM)-(LINUX|WINDOWS)(-|$)",
    r"^(BNK|DOM)-(POSTGRES|POSTGRESQL|PG)(-|$)",
)

@dataclass
class ZabbixConnection:
    api_url: str
    username: str
    password: str


@dataclass
class GrafanaConnection:
    base_url: str
    username: str
    password: str


def load_zabbix_connection() -> ZabbixConnection:
    return ZabbixConnection(
        api_url=(ZBX_URL or "").strip(),
        username=(ZBX_USER or "").strip(),
        password=(ZBX_PASSWORD or ""),
    )


def load_grafana_connection() -> GrafanaConnection:
    return GrafanaConnection(
        base_url=(GRAFANA_URL or "").strip(),
        username=(GRAFANA_USER or "").strip(),
        password=(GRAFANA_PASSWORD or ""),
    )
