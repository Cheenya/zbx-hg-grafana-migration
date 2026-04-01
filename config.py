from __future__ import annotations

from dataclasses import dataclass


# Zabbix. Заполняется руками.
ZBX_URL = ""
ZBX_USER = ""
ZBX_PASSWORD = ""
ZBX_API_TOKEN = ""

# Grafana. Заполняется руками.
GRAFANA_URL = ""
GRAFANA_USER = ""
GRAFANA_PASSWORD = ""


# Scope для Zabbix / обычного audit.
SCOPE_AS: tuple[str, ...] = ()
SCOPE_ENV: str = ""
SCOPE_GAS: tuple[str, ...] = ()

# ORG по доменам/именам хоста. Если совпадений нет, идёт fallback на группы хоста.
ORG_DOMAIN_SUFFIXES: dict[str, tuple[str, ...]] = {
    "BNK": ("rosgap.com", "bnkrf.ru"),
    "DOM": ("ahuel1.ru", "dom.ru"),
}

# Привязка Grafana org к SCOPE_AS в audit_scope.py.
GRAFANA_ORGIDS: tuple[int, ...] = ()

# Отдельный Grafana-only audit по org.
GRAFANA_AUDIT_ORGIDS: tuple[int, ...] = ()


# Runtime.
HTTP_TIMEOUT_SEC: int = 90
MONITORED_HOSTS_ONLY: bool = False
ENABLE_GRAFANA: bool = True
GROUP_SAMPLE_HOSTS: int = 10
SAVE_JSON_INVENTORY: bool = True

# Grafana apply.
GRAFANA_APPLY_CHANGES: bool = False
GRAFANA_ZABBIX_DATASOURCE_TYPES: tuple[str, ...] = ("alexanderzobnin-zabbix-datasource",)

# Zabbix apply.
ZABBIX_APPLY_CHANGES: bool = False

# Каталог и имена артефактов.
OUTPUT_DIR: str = "v2_output"
OUTPUT_PREFIX: str = "scope_audit_v2"
AUDIT_LOG_PREFIX: str = "audit_log_v2"
GRAFANA_REPORT_PREFIX: str = "grafana_audit_v2"
GRAFANA_ORG_AUDIT_PREFIX: str = "grafana_org_audit"
GRAFANA_ORG_AUDIT_LOG_PREFIX: str = "grafana_org_audit_log"
GRAFANA_PLAN_PREFIX: str = "grafana_plan"
GRAFANA_APPLY_PREFIX: str = "grafana_apply"
ZABBIX_APPLY_PREFIX: str = "zabbix_apply"
MAPPING_PLAN_PREFIX: str = "mapping_plan_v2"
IMPACT_PLAN_PREFIX: str = "impact_plan_v2"
BACKUP_PREFIX: str = "scope_backup_v2"

# Входные файлы. Если пусто, скрипты берут самый свежий файл из OUTPUT_DIR.
SOURCE_AUDIT_JSON: str = ""
SOURCE_GRAFANA_ORG_JSON: str = ""
SOURCE_MAPPING_PLAN_XLSX: str = ""
SOURCE_IMPACT_PLAN_JSON: str = ""
SOURCE_BACKUP_FILE: str = ""
SOURCE_GRAFANA_PLAN_XLSX: str = ""


# Теги.
TAG_AS: str = "AS"
TAG_ENV: str = "ENV"
TAG_GAS: str = "GAS"
TAG_GUEST_NAME: str = "GUEST-NAME"

# UNKNOWN.
UNKNOWN_TAG_VALUE: str = "UNKNOWN"
UNKNOWN_GROUP_NAME: str = "UNKNOWN"
EXCLUDE_UNKNOWN_FROM_STATS: bool = True

# ENV.
ENV_PROD_LABEL: str = "PROD"
ENV_NONPROD_LABEL: str = "NONPROD"
PROD_ENV_VALUES: tuple[str, ...] = ("PROD",)
LEGACY_ENV_TOKENS: tuple[str, ...] = (
    "PROD",
    "NONPROD",
    "DEV",
    "TEST",
    "STAGE",
    "UAT",
    "QA",
    "PREPROD",
)

# Пороги mapping.
MAPPING_MIN_INTERSECTION: int = 2
MAPPING_MIN_OLD_COVERAGE: float = 0.20
MAPPING_MIN_NEW_COVERAGE: float = 0.20
MAPPING_FORBID_ENV_MISMATCH: bool = True

# Исключаемые группы.
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
    api_token: str = ""


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
        api_token=(ZBX_API_TOKEN or "").strip(),
    )


def load_grafana_connection() -> GrafanaConnection:
    return GrafanaConnection(
        base_url=(GRAFANA_URL or "").strip(),
        username=(GRAFANA_USER or "").strip(),
        password=(GRAFANA_PASSWORD or ""),
    )
