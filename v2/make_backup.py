from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Sequence

import config
from api_clients import ZabbixAPI
from backup_io import save_backup
from backup_model import (
    ActionBackup,
    BackupData,
    BackupMeta,
    HostBackup,
    HostGroupBackup,
    MaintenanceBackup,
    UserBackup,
    UserGroupBackup,
)
from common import build_scope_part, normalize_values


def load_impact_plan(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_backup_path(scope_as: Sequence[str], scope_env: str) -> str:
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    scope_part = build_scope_part(scope_as, scope_env)
    return os.path.join(config.OUTPUT_DIR, f"{config.BACKUP_PREFIX}_{scope_part}_{timestamp}.json.gz")


def _extract_ids(rows: Iterable[Dict[str, Any]], key_name: str) -> List[str]:
    return [str(row.get(key_name) or "").strip() for row in rows if str(row.get(key_name) or "").strip()]


def _assert_full_coverage(entity_name: str, requested_ids: Sequence[str], fetched_rows: Sequence[Dict[str, Any]], key_name: str) -> None:
    requested = {str(item).strip() for item in requested_ids if str(item).strip()}
    fetched = {str(row.get(key_name) or "").strip() for row in fetched_rows if str(row.get(key_name) or "").strip()}
    missing = sorted(requested.difference(fetched))
    if missing:
        raise RuntimeError(f"Backup coverage error for {entity_name}: missing ids: {', '.join(missing)}")


def fetch_hostgroups(api: ZabbixAPI, groupids: Sequence[str]) -> List[Dict[str, Any]]:
    if not groupids:
        return []
    return api.call("hostgroup.get", {"output": "extend", "groupids": list(groupids)})


def fetch_hosts(api: ZabbixAPI, hostids: Sequence[str]) -> List[Dict[str, Any]]:
    if not hostids:
        return []
    return api.call(
        "host.get",
        {
            "output": "extend",
            "hostids": list(hostids),
            "selectGroups": "extend",
            "selectTags": "extend",
        },
    )


def fetch_actions(api: ZabbixAPI, actionids: Sequence[str]) -> List[Dict[str, Any]]:
    if not actionids:
        return []
    return api.call(
        "action.get",
        {
            "output": "extend",
            "actionids": list(actionids),
            "selectOperations": "extend",
            "selectRecoveryOperations": "extend",
            "selectUpdateOperations": "extend",
            "selectFilter": "extend",
        },
    )


def fetch_usergroups(api: ZabbixAPI, usrgrpids: Sequence[str]) -> List[Dict[str, Any]]:
    if not usrgrpids:
        return []
    return api.call(
        "usergroup.get",
        {
            "output": "extend",
            "usrgrpids": list(usrgrpids),
            "selectHostGroupRights": "extend",
            "selectTemplateGroupRights": "extend",
            "selectTagFilters": "extend",
            "selectUsers": "extend",
        },
    )


def fetch_users(api: ZabbixAPI, userids: Sequence[str]) -> List[Dict[str, Any]]:
    if not userids:
        return []
    return api.call(
        "user.get",
        {
            "output": "extend",
            "userids": list(userids),
            "selectMedias": "extend",
            "selectUsrgrps": "extend",
        },
    )


def fetch_maintenances(api: ZabbixAPI, maintenanceids: Sequence[str]) -> List[Dict[str, Any]]:
    if not maintenanceids:
        return []
    return api.call(
        "maintenance.get",
        {
            "output": "extend",
            "maintenanceids": list(maintenanceids),
            "selectGroups": "extend",
            "selectHosts": "extend",
            "selectTags": "extend",
            "selectTimeperiods": "extend",
        },
    )


def create_backup(api: ZabbixAPI, impact_plan_path: str, backup_path: str) -> BackupData:
    raw_impact_plan = load_impact_plan(impact_plan_path)
    summary = raw_impact_plan.get("summary") or {}
    backup_scope = raw_impact_plan.get("backup_scope") or {}
    scope_as = normalize_values(summary.get("scope_as") or [])
    scope_env = str(summary.get("scope_env") or "").strip()
    if not scope_env:
        legacy_scope_envs = summary.get("scope_envs") or []
        if legacy_scope_envs:
            scope_env = str(legacy_scope_envs[0] or "").strip()

    if not scope_as:
        raise RuntimeError("Impact plan scope_as is empty. Build impact plan first.")

    scoped_hostgroups = backup_scope.get("hostgroups") or []
    hostgroup_ids = _extract_ids(scoped_hostgroups, "groupid")
    hostids = normalize_values(backup_scope.get("hostids") or [])
    actionids = normalize_values(backup_scope.get("actionids") or [])
    usergroupids = normalize_values(backup_scope.get("usergroupids") or [])
    userids = normalize_values(backup_scope.get("userids") or [])
    maintenanceids = normalize_values(backup_scope.get("maintenanceids") or [])

    hostgroups = fetch_hostgroups(api, hostgroup_ids)
    hosts = fetch_hosts(api, hostids)
    actions = fetch_actions(api, actionids)
    usergroups = fetch_usergroups(api, usergroupids)
    users = fetch_users(api, userids)
    maintenances = fetch_maintenances(api, maintenanceids)

    _assert_full_coverage("hostgroups", hostgroup_ids, hostgroups, "groupid")
    _assert_full_coverage("hosts", hostids, hosts, "hostid")
    _assert_full_coverage("actions", actionids, actions, "actionid")
    _assert_full_coverage("usergroups", usergroupids, usergroups, "usrgrpid")
    _assert_full_coverage("users", userids, users, "userid")
    _assert_full_coverage("maintenances", maintenanceids, maintenances, "maintenanceid")

    inventory_kind_by_groupid = {
        str(item.get("groupid") or ""): str(item.get("kind") or "")
        for item in scoped_hostgroups
        if str(item.get("groupid") or "").strip()
    }

    data = BackupData(
        meta=BackupMeta(
            created_at=datetime.now().isoformat(timespec="seconds"),
            impact_plan_path=impact_plan_path,
            zabbix_url=str(getattr(api, "api_url", "")),
            scope_as=scope_as,
            scope_env=scope_env,
        ),
        impact_plan=raw_impact_plan,
        hostgroups=[
            HostGroupBackup(
                groupid=str(group.get("groupid") or ""),
                name=str(group.get("name") or ""),
                kind=inventory_kind_by_groupid.get(str(group.get("groupid") or ""), ""),
                raw=group,
            )
            for group in hostgroups
            if group.get("groupid") is not None
        ],
        hosts=[
            HostBackup(
                hostid=str(host.get("hostid") or ""),
                raw=host,
                groups=host.get("groups") or [],
                tags=host.get("tags") or [],
            )
            for host in hosts
            if host.get("hostid") is not None
        ],
        actions=[
            ActionBackup(actionid=str(action.get("actionid") or ""), raw=action)
            for action in actions
            if action.get("actionid") is not None
        ],
        usergroups=[
            UserGroupBackup(
                usrgrpid=str(usergroup.get("usrgrpid") or ""),
                name=str(usergroup.get("name") or ""),
                raw=usergroup,
            )
            for usergroup in usergroups
            if usergroup.get("usrgrpid") is not None
        ],
        users=[
            UserBackup(
                userid=str(user.get("userid") or ""),
                username=str(user.get("username") or user.get("alias") or ""),
                name=str(user.get("name") or ""),
                surname=str(user.get("surname") or ""),
                raw=user,
            )
            for user in users
            if user.get("userid") is not None
        ],
        maintenances=[
            MaintenanceBackup(
                maintenanceid=str(maintenance.get("maintenanceid") or ""),
                name=str(maintenance.get("name") or ""),
                raw=maintenance,
            )
            for maintenance in maintenances
            if maintenance.get("maintenanceid") is not None
        ],
    )

    save_backup(data, backup_path)
    return data


def main() -> int:
    impact_plan_path = str(config.SOURCE_IMPACT_PLAN_JSON or "").strip()
    if not impact_plan_path:
        raise RuntimeError("Set v2/config.py SOURCE_IMPACT_PLAN_JSON to the JSON file from v2.build_impact_plan.")

    impact_plan = load_impact_plan(impact_plan_path)
    impact_summary = impact_plan.get("summary") or {}
    scope_env = str(impact_summary.get("scope_env") or "").strip()
    if not scope_env:
        legacy_scope_envs = impact_summary.get("scope_envs") or []
        if legacy_scope_envs:
            scope_env = str(legacy_scope_envs[0] or "").strip()
    backup_path = build_backup_path(
        normalize_values(impact_summary.get("scope_as") or []),
        scope_env,
    )

    connection = config.load_zabbix_connection()
    api = ZabbixAPI(connection.api_url, timeout_sec=int(config.HTTP_TIMEOUT_SEC))
    api.login(connection.username, connection.password)

    print(f"Building backup from impact plan: {impact_plan_path}")
    create_backup(api, impact_plan_path, backup_path)
    print(f"Backup saved: {backup_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
