#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Сбор подтверждённых данных из Zabbix по заранее заданному дереву систем/ролей/хостов.

Что собираем только по факту наличия данных в Zabbix:
- systemd/services
- mounted filesystems
- block devices / diskstats
- network interfaces
- SSL endpoints
- абсолютные пути, которые реально встречаются в item name / key / lastvalue

Вход:
- monitoring_config.py
- hosts_tree.json

Выход:
- zabbix_inventory_report.json
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PATH_RE = re.compile(r'(/[A-Za-z0-9._@%+=:,~\-]+(?:/[A-Za-z0-9._@%+=:,~\-]+)*)')
FS_SIZE_RE = re.compile(r'^vfs\.fs\.size\[(?P<mount>.*?),(?P<mode>.*?)\]$')
VFS_DEV_RE = re.compile(r'^vfs\.dev\.(?P<kind>read|write)\[(?P<device>.*?)\]$')
NET_IF_RE = re.compile(r'^net\.if\.[^(\[]+\[(?P<iface>.*?)(?:,.*)?\]$')
WEB_CERT_RE = re.compile(r'^web\.certificate\.get\[(?P<target>.+?)\]$')
SYSTEMD_RE = re.compile(r'^systemd\.unit\.(?:get|info)\[(?P<unit>.*?)(?:,(?P<field>.*?))?\]$')
PROC_NUM_RE = re.compile(r'^proc\.num\[(?P<args>.*)\]$')

SERVICE_HINTS = [
    'zabbix', 'grafana', 'victoria', 'victoriametrics',
    'vminsert', 'vmselect', 'vmstorage', 'vmagent', 'vmalert', 'vmauth',
    'postgres', 'postgresql', 'pgsql'
]

IGNORE_PATH_PREFIXES = (
    '/proc/', '/sys/', '/dev/', '/run/', '/tmp/'
)


class ZabbixAPIError(RuntimeError):
    pass


def load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Cannot load module from {module_path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def api_call(url: str, method: str, params: dict[str, Any] | list[Any], auth: str | None, timeout: int, verify_ssl: bool) -> Any:
    payload: dict[str, Any] = {
        'jsonrpc': '2.0',
        'method': method,
        'params': params,
        'id': 1,
    }
    if auth is not None:
        payload['auth'] = auth

    response = requests.post(
        url,
        json=payload,
        timeout=timeout,
        verify=verify_ssl,
        headers={'Content-Type': 'application/json-rpc'},
    )
    response.raise_for_status()
    data = response.json()
    if 'error' in data:
        raise ZabbixAPIError(f"{method} failed: {data['error'].get('message')} / {data['error'].get('data')}")
    return data['result']


def zbx_login(url: str, user: str, password: str, timeout: int, verify_ssl: bool) -> str:
    return api_call(
        url,
        'user.login',
        {'username': user, 'password': password},
        auth=None,
        timeout=timeout,
        verify_ssl=verify_ssl,
    )


def zbx_logout(url: str, auth: str, timeout: int, verify_ssl: bool) -> None:
    try:
        api_call(url, 'user.logout', [], auth=auth, timeout=timeout, verify_ssl=verify_ssl)
    except Exception:
        pass


def load_host_tree(path: Path) -> dict[str, dict[str, list[str]]]:
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise ValueError('Host tree JSON must be an object')
    return data


def flatten_tree(tree: dict[str, dict[str, list[str]]]) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for system_name, roles in tree.items():
        if not isinstance(roles, dict):
            raise ValueError(f'System {system_name} must contain an object of roles')
        for role_name, hosts in roles.items():
            if not isinstance(hosts, list):
                raise ValueError(f'Role {system_name}/{role_name} must contain an array of hosts')
            for host in hosts:
                result.append({'system': system_name, 'role': role_name, 'host_ref': str(host)})
    return result


def find_host(url: str, auth: str, host_ref: str, timeout: int, verify_ssl: bool) -> dict[str, Any] | None:
    exact_host = api_call(
        url,
        'host.get',
        {
            'output': ['hostid', 'host', 'name', 'status'],
            'filter': {'host': [host_ref]},
            'selectParentTemplates': ['templateid', 'name', 'host'],
            'selectTags': 'extend',
            'selectGroups': ['groupid', 'name'],
            'limit': 1,
        },
        auth=auth,
        timeout=timeout,
        verify_ssl=verify_ssl,
    )
    if exact_host:
        return exact_host[0]

    search_name = api_call(
        url,
        'host.get',
        {
            'output': ['hostid', 'host', 'name', 'status'],
            'search': {'name': host_ref},
            'searchByAny': True,
            'selectParentTemplates': ['templateid', 'name', 'host'],
            'selectTags': 'extend',
            'selectGroups': ['groupid', 'name'],
        },
        auth=auth,
        timeout=timeout,
        verify_ssl=verify_ssl,
    )
    for candidate in search_name:
        if candidate.get('name') == host_ref:
            return candidate
    return None


def get_items(url: str, auth: str, hostid: str, timeout: int, verify_ssl: bool) -> list[dict[str, Any]]:
    return api_call(
        url,
        'item.get',
        {
            'output': ['itemid', 'name', 'key_', 'lastvalue', 'lastclock', 'units', 'value_type', 'status', 'state'],
            'hostids': [hostid],
            'filter': {'status': 0},
            'webitems': True,
            'sortfield': 'name',
        },
        auth=auth,
        timeout=timeout,
        verify_ssl=verify_ssl,
    )


def parse_csv_arg(raw: str) -> list[str]:
    if not raw:
        return []
    return [part.strip().strip('"').strip("'") for part in raw.split(',')]


def safe_path(path: str) -> bool:
    if not path.startswith('/'):
        return False
    if path in {'/', '/var', '/etc', '/usr', '/opt'}:
        return False
    if any(path.startswith(prefix) for prefix in IGNORE_PATH_PREFIXES):
        return False
    return True


def extract_paths(*chunks: str) -> list[str]:
    found: set[str] = set()
    for chunk in chunks:
        if not chunk:
            continue
        for match in PATH_RE.finditer(chunk):
            path = match.group(1)
            if safe_path(path):
                found.add(path)
    return sorted(found)


def add_unique(rows: list[dict[str, Any]], row: dict[str, Any], dedupe_key: tuple[Any, ...], seen: set[tuple[Any, ...]]) -> None:
    if dedupe_key in seen:
        return
    seen.add(dedupe_key)
    rows.append(row)


def classify_path(path: str) -> str:
    low = path.lower()
    if low.startswith('/var/log/') or low.endswith(('.log', '.out', '.err')):
        return 'log_like'
    if low.startswith('/etc/') or low.endswith(('.conf', '.ini', '.yaml', '.yml', '.json', '.toml', '.cnf', '.service')):
        return 'config_like'
    if '/cert' in low or low.endswith(('.crt', '.pem', '.key', '.cer')) or low.startswith('/etc/ssl/') or low.startswith('/etc/pki/'):
        return 'certificate_like'
    if '/bin/' in low or '/sbin/' in low:
        return 'binary_like'
    if any(token in low for token in ['/var/lib/', 'data', 'pg_wal', 'tablespace', 'victoria-metrics-data']):
        return 'data_like'
    return 'other'


def collect_confirmed_data(items: list[dict[str, Any]]) -> dict[str, Any]:
    services: list[dict[str, Any]] = []
    filesystems: list[dict[str, Any]] = []
    block_devices: list[dict[str, Any]] = []
    interfaces: list[dict[str, Any]] = []
    ssl_targets: list[dict[str, Any]] = []
    paths: list[dict[str, Any]] = []
    raw_matches: list[dict[str, Any]] = []

    seen_services: set[tuple[Any, ...]] = set()
    seen_fs: set[tuple[Any, ...]] = set()
    seen_dev: set[tuple[Any, ...]] = set()
    seen_iface: set[tuple[Any, ...]] = set()
    seen_ssl: set[tuple[Any, ...]] = set()
    seen_path: set[tuple[Any, ...]] = set()
    seen_raw: set[tuple[Any, ...]] = set()

    for item in items:
        name = str(item.get('name', ''))
        key_ = str(item.get('key_', ''))
        lastvalue = str(item.get('lastvalue', ''))
        source = {
            'itemid': item.get('itemid'),
            'name': name,
            'key': key_,
            'lastvalue': lastvalue,
            'lastclock': item.get('lastclock'),
        }
        combined = f"{name}\n{key_}\n{lastvalue}"
        low = combined.lower()

        m = SYSTEMD_RE.match(key_)
        if m:
            unit = m.group('unit').strip().strip('"').strip("'")
            if any(hint in unit.lower() for hint in SERVICE_HINTS):
                add_unique(
                    services,
                    {
                        'unit': unit,
                        'evidence': source,
                    },
                    (unit, key_),
                    seen_services,
                )

        if PROC_NUM_RE.match(key_) and any(hint in low for hint in SERVICE_HINTS):
            add_unique(
                services,
                {
                    'unit': name,
                    'evidence': source,
                },
                (name, key_),
                seen_services,
            )

        m = FS_SIZE_RE.match(key_)
        if m:
            mount = m.group('mount').strip().strip('"').strip("'")
            mode = m.group('mode').strip().strip('"').strip("'")
            add_unique(
                filesystems,
                {
                    'mountpoint': mount,
                    'metric_mode': mode,
                    'value': lastvalue,
                    'units': item.get('units', ''),
                    'evidence': source,
                },
                (mount, mode, key_),
                seen_fs,
            )

        m = VFS_DEV_RE.match(key_)
        if m:
            device = m.group('device').strip().strip('"').strip("'")
            kind = m.group('kind')
            add_unique(
                block_devices,
                {
                    'device': device,
                    'metric_kind': kind,
                    'value': lastvalue,
                    'units': item.get('units', ''),
                    'evidence': source,
                },
                (device, kind, key_),
                seen_dev,
            )

        m = NET_IF_RE.match(key_)
        if m:
            iface = parse_csv_arg(m.group('iface'))[0] if parse_csv_arg(m.group('iface')) else m.group('iface')
            add_unique(
                interfaces,
                {
                    'interface': iface,
                    'value': lastvalue,
                    'units': item.get('units', ''),
                    'evidence': source,
                },
                (iface, key_),
                seen_iface,
            )

        m = WEB_CERT_RE.match(key_)
        if m:
            target = m.group('target')
            add_unique(
                ssl_targets,
                {
                    'target': target,
                    'value': lastvalue,
                    'evidence': source,
                },
                (target, key_),
                seen_ssl,
            )

        discovered_paths = extract_paths(name, key_, lastvalue)
        if discovered_paths:
            for path in discovered_paths:
                add_unique(
                    paths,
                    {
                        'path': path,
                        'kind': classify_path(path),
                        'evidence': source,
                    },
                    (path, key_),
                    seen_path,
                )

        if any(hint in low for hint in SERVICE_HINTS):
            add_unique(
                raw_matches,
                {
                    'text': name,
                    'key': key_,
                    'value': lastvalue,
                },
                (name, key_, lastvalue),
                seen_raw,
            )

    return {
        'services': sorted(services, key=lambda x: x['unit']),
        'filesystems': sorted(filesystems, key=lambda x: (x['mountpoint'], x['metric_mode'])),
        'block_devices': sorted(block_devices, key=lambda x: (x['device'], x['metric_kind'])),
        'network_interfaces': sorted(interfaces, key=lambda x: x['interface']),
        'ssl_targets': sorted(ssl_targets, key=lambda x: x['target']),
        'paths': sorted(paths, key=lambda x: x['path']),
        'raw_matches': raw_matches,
    }


def summarize_confirmed(confirmed: dict[str, Any]) -> dict[str, Any]:
    return {
        'services_count': len(confirmed['services']),
        'filesystems_count': len(confirmed['filesystems']),
        'block_devices_count': len(confirmed['block_devices']),
        'network_interfaces_count': len(confirmed['network_interfaces']),
        'ssl_targets_count': len(confirmed['ssl_targets']),
        'paths_count': len(confirmed['paths']),
    }


def build_report(url: str, auth: str, host_tree: dict[str, dict[str, list[str]]], timeout: int, verify_ssl: bool) -> dict[str, Any]:
    flat = flatten_tree(host_tree)
    report: dict[str, Any] = {
        'generated_at_utc': datetime.now(timezone.utc).isoformat(),
        'source': 'Zabbix API',
        'systems': {},
    }

    for entry in flat:
        system_name = entry['system']
        role_name = entry['role']
        host_ref = entry['host_ref']

        report['systems'].setdefault(system_name, {})
        report['systems'][system_name].setdefault(role_name, {})

        host_info = find_host(url, auth, host_ref, timeout, verify_ssl)
        if host_info is None:
            report['systems'][system_name][role_name][host_ref] = {
                'found_in_zabbix': False,
                'host_ref': host_ref,
                'error': 'Host not found in Zabbix',
            }
            continue

        items = get_items(url, auth, host_info['hostid'], timeout, verify_ssl)
        confirmed = collect_confirmed_data(items)
        summary = summarize_confirmed(confirmed)

        report['systems'][system_name][role_name][host_ref] = {
            'found_in_zabbix': True,
            'host': {
                'hostid': host_info['hostid'],
                'host': host_info.get('host'),
                'name': host_info.get('name'),
                'status': host_info.get('status'),
                'templates': sorted([t.get('name') or t.get('host') for t in host_info.get('parentTemplates', [])]),
                'groups': sorted([g.get('name') for g in host_info.get('groups', [])]),
                'tags': host_info.get('tags', []),
            },
            'summary': summary,
            'confirmed': confirmed,
        }

    return report


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    config_path = base_dir / 'monitoring_config.py'

    if not config_path.exists():
        print(f'Config file not found: {config_path}', file=sys.stderr)
        return 2

    config = load_module(config_path, 'monitoring_config')

    zabbix_url = getattr(config, 'ZABBIX_URL')
    zabbix_user = getattr(config, 'ZABBIX_USER')
    zabbix_password = getattr(config, 'ZABBIX_PASSWORD')
    verify_ssl = bool(getattr(config, 'VERIFY_SSL', False))
    timeout = int(getattr(config, 'TIMEOUT', 30))
    host_tree_path = base_dir / getattr(config, 'HOST_TREE_PATH', 'hosts_tree.json')
    output_json_path = base_dir / getattr(config, 'OUTPUT_JSON_PATH', 'zabbix_inventory_report.json')

    if not host_tree_path.exists():
        print(f'Host tree file not found: {host_tree_path}', file=sys.stderr)
        return 3

    host_tree = load_host_tree(host_tree_path)

    auth = None
    try:
        auth = zbx_login(zabbix_url, zabbix_user, zabbix_password, timeout, verify_ssl)
        report = build_report(zabbix_url, auth, host_tree, timeout, verify_ssl)
        output_json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f'[OK] Report written to: {output_json_path}')
        return 0
    except Exception as exc:
        print(f'[ERROR] {exc}', file=sys.stderr)
        return 1
    finally:
        if auth:
            zbx_logout(zabbix_url, auth, timeout, verify_ssl)


if __name__ == '__main__':
    raise SystemExit(main())
