from __future__ import annotations

from dataclasses import dataclass


# Подключения v2. Заполняются прямо здесь.
ZBX_URL = ""
ZBX_USER = ""
ZBX_PASSWORD = ""

GRAFANA_URL = ""
GRAFANA_USER = ""
GRAFANA_PASSWORD = ""
GRAFANA_TOKEN = ""


# Scope нового контура.
SCOPE_AS: tuple[str, ...] = ()
SCOPE_ENVS: tuple[str, ...] = ()


# Runtime v2.
HTTP_TIMEOUT_SEC: int = 90
MONITORED_HOSTS_ONLY: bool = False
ENABLE_GRAFANA: bool = True
OUTPUT_DIR: str = "v2_output"
OUTPUT_PREFIX: str = "scope_audit_v2"
GROUP_SAMPLE_HOSTS: int = 10
SAVE_JSON_INVENTORY: bool = True


# Теги и исключения.
TAG_AS: str = "AS"
TAG_ASN: str = "ASN"
TAG_ENV: str = "ENV"
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
    token: str


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
        token=(GRAFANA_TOKEN or "").strip(),
    )
