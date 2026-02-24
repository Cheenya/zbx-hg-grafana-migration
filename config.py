# -*- coding: utf-8 -*-
"""config.py — общий конфиг для наших скриптов Zabbix/Grafana.

Сюда вынесены ВСЕ настраиваемые параметры:
- подключение к Zabbix API (URL/логин/пароль)
- подключение к Grafana API (URL/token)
- имена тегов (AS/ASN/ENV и т.д.)
- правила UNKNOWN
- пороги для эталона (частотное соответствие)
- параметры выгрузки в Excel
- исключения хост-групп (regex)
- runtime-параметры (timeouts, лимиты, backup)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# Можно заполнить здесь напрямую, чтобы не использовать env или вводить интерактивно
ZBX_URL = ""
ZBX_USER = ""
ZBX_PASSWORD = ""

# Grafana (логин/пароль)
GRAFANA_URL = ""
GRAFANA_USER = ""
GRAFANA_PASSWORD = ""
# Опционально: token (если используете service account)
GRAFANA_TOKEN = ""


@dataclass
class ZabbixConnection:
    """Параметры подключения к Zabbix API."""

    api_url: str
    username: str
    password: str


@dataclass
class GrafanaConnection:
    """Параметры подключения к Grafana HTTP API."""

    base_url: str
    username: str
    password: str
    token: str


@dataclass
class TagNamesCfg:
    """Имена тегов, по которым строятся новые хост-группы и AS-разбиение."""

    AS: str = "AS"
    ASN: str = "ASN"
    ENV: str = "ENV"
    GAS: str = "GAS"
    GUEST_NAME: str = "GUEST-NAME"


@dataclass
class UnknownRules:
    """Что считаем UNKNOWN/шумом."""

    unknown_tag_value: str = "UNKNOWN"  # если AS==UNKNOWN или ASN==UNKNOWN
    unknown_group_name: str = "UNKNOWN"  # если хост состоит в группе с таким именем
    exclude_unknown_from_stats: bool = True  # True = не участвуют в эталоне


@dataclass
class MappingRules:
    """Пороги для эталона.

    Примечание: правила NEW/OLD для эталона задаются в основном скрипте.
    Исключения групп — в Runtime.excluded_group_patterns.
    """

    # Эталон: сколько кандидатов сохраняем для каждой new-группы
    top_n_candidates: int = 5

    # Отсечение мусора
    min_intersection: int = 2  # минимум общих хостов
    min_precision: float = 0.20  # intersection / hosts_in_new

    # ENV-политика: PROD/NONPROD считаем истинной по тегу ENV.
    # Если пары old/new явно противоречат по среде (PROD<->NONPROD и т.п.) — их надо ЗАПРЕЩАТЬ.
    forbid_env_mismatch: bool = True

    # Известные значения ENV (можно расширять)
    env_tokens: tuple[str, ...] = ("PROD", "NONPROD", "DEV", "STAGE", "TEST", "UAT", "PREPROD")


@dataclass
class ExcelOutput:
    """Параметры Excel-отчёта."""

    output_xlsx: str = "hostgroup_mapping_audit.xlsx"
    sheet_name_max: int = 31
    sheet_unknown: str = "UNKNOWN_HOSTS"
    sheet_mapping: str = "MAPPING"  # сводная таблица OLD->NEW
    sheet_grafana: str = "GRAFANA"  # опциональный общий лист (если нужен)
    max_sheets_per_workbook: int = 200
    migration_plan_path: Optional[str] = None  # если None, путь строится от output_xlsx


@dataclass
class Runtime:
    """Параметры выполнения."""

    monitored_hosts_only: bool = True
    http_timeout_sec: int = 90
    limit_as: Optional[int] = None  # для тестового прогона, например 20
    create_backup_on_audit: bool = False
    audit_scope_as: tuple[str, ...] = ("dom_itmon",)  # список AS для scoped-скриптов
    enable_grafana_audit: bool = True
    save_zabbix_seed_on_audit: bool = True
    zabbix_seed_path: Optional[str] = None  # если None, путь строится от output_xlsx

    # Исключённые группы (regex): не участвуют в обработке
    # Для точного имени используйте якоря ^...$
    excluded_group_patterns: tuple[str, ...] = (
        # Точное имя / сервисная группа
        r"^Maintenance-dc-enable$",

        # Всё служебное Maintenance-*
        r"^Maintenance-",

        # OS/guest-name legacy-группы (чтобы не попадали в эталон)
        r"^(BNK|DOM)-(LINUX|WINDOWS)(-|$)",

        # БД/PG группы (если их не нужно учитывать как AS)
        r"^(BNK|DOM)-(POSTGRES|POSTGRESQL|PG)(-|$)",
    )


@dataclass
class Config:
    """Единый объект конфигурации."""

    tags: TagNamesCfg = field(default_factory=TagNamesCfg)
    unknown: UnknownRules = field(default_factory=UnknownRules)
    mapping: MappingRules = field(default_factory=MappingRules)
    excel: ExcelOutput = field(default_factory=ExcelOutput)
    runtime: Runtime = field(default_factory=Runtime)
    # Grafana параметры читаем отдельной функцией (token лучше не хранить в CONFIG)


def load_connection_from_env_or_prompt(interactive: bool = True) -> ZabbixConnection:
    """Возвращает параметры подключения.

    Сейчас (локальный запуск) берём URL/логин/пароль ТОЛЬКО из переменных модуля:
      - ZBX_URL
      - ZBX_USER
      - ZBX_PASSWORD

    Никаких env и интерактивного ввода.
    Параметр `interactive` оставлен только для совместимости сигнатуры.
    """

    return ZabbixConnection(
        api_url=(ZBX_URL or "").strip(),
        username=(ZBX_USER or "").strip(),
        password=(ZBX_PASSWORD or ""),
    )


def load_grafana_from_module() -> GrafanaConnection:
    """Возвращает параметры Grafana.

    Сейчас (локальный запуск) берём ТОЛЬКО из переменных модуля:
      - GRAFANA_URL
      - GRAFANA_USER
      - GRAFANA_PASSWORD
      - GRAFANA_TOKEN (опционально)
    """

    return GrafanaConnection(
        base_url=(GRAFANA_URL or "").strip(),
        username=(GRAFANA_USER or "").strip(),
        password=(GRAFANA_PASSWORD or ""),
        token=(GRAFANA_TOKEN or "").strip(),
    )


# ЕДИНАЯ ТОЧКА ДЛЯ ИЗМЕНЕНИЯ ПАРАМЕТРОВ
CONFIG = Config()
