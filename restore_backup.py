#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""restore_backup.py — откат Zabbix из бэкапа."""

from __future__ import annotations

import argparse
from typing import Any, Dict

import urllib3  # type: ignore

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from api_clients import ZabbixAPI
from backup_io import load_backup
from config import CONFIG, load_connection_from_env_or_prompt


def _filter_keys(data: Dict[str, Any], allowed: set) -> Dict[str, Any]:
    return {k: v for k, v in (data or {}).items() if k in allowed}


def restore_backup(api: ZabbixAPI, backup_path: str) -> None:
    data = load_backup(backup_path)

    action_allowed = {
        "name",
        "eventsource",
        "evaltype",
        "status",
        "esc_period",
        "def_shortdata",
        "def_longdata",
        "r_shortdata",
        "r_longdata",
        "recovery_msg",
        "acknowledge_msg",
        "pause_suppressed",
        "notify_if_canceled",
        "filter",
        "operations",
        "recovery_operations",
        "update_operations",
    }
    usergroup_allowed = {
        "name",
        "users",
        "hostgroup_rights",
        "templategroup_rights",
        "tag_filters",
        "gui_access",
        "users_status",
        "debug_mode",
    }
    user_allowed = {
        "username",
        "name",
        "surname",
        "usrgrps",
        "medias",
        "roleid",
        "type",
        "lang",
        "theme",
        "timezone",
        "autologin",
        "autologout",
        "refresh",
        "rows_per_page",
        "url",
    }

    # 1) Пользователи
    for u in data.users:
        payload: Dict[str, Any] = {"userid": u.userid}
        if u.raw:
            payload.update(_filter_keys(u.raw, user_allowed))
        else:
            if u.username:
                payload["username"] = u.username
            payload["name"] = u.name
            payload["surname"] = u.surname
            payload["usrgrps"] = u.usrgrps
            payload["medias"] = u.medias
        api.call("user.update", payload)

    # 2) Группы пользователей
    for ug in data.usergroups:
        payload = {"usrgrpid": ug.usrgrpid}
        if ug.raw:
            payload.update(_filter_keys(ug.raw, usergroup_allowed))
        else:
            payload.update(
                {
                    "name": ug.name,
                    "users": ug.users,
                    "hostgroup_rights": ug.hostgroup_rights,
                    "tag_filters": ug.tag_filters,
                }
            )
        api.call("usergroup.update", payload)

    # 3) Действия
    for a in data.actions:
        payload = {"actionid": a.actionid}
        if a.raw:
            payload.update(_filter_keys(a.raw, action_allowed))
        else:
            payload.update(
                {
                    "filter": a.filter,
                    "operations": a.operations,
                    "recovery_operations": a.recovery_operations,
                    "update_operations": a.update_operations,
                }
            )
        api.call("action.update", payload)

    # 4) Хосты
    for h in data.hosts:
        groups = []
        for g in h.groups:
            gid = g.get("groupid")
            if gid is not None:
                groups.append({"groupid": str(gid)})
        payload = {"hostid": h.hostid, "groups": groups, "tags": h.tags}
        api.call("host.update", payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Restore Zabbix from backup")
    parser.add_argument("backup", help="Backup file (.json or .json.gz)")
    args = parser.parse_args()

    conn = load_connection_from_env_or_prompt(interactive=False)
    api = ZabbixAPI(conn.api_url, timeout_sec=int(CONFIG.runtime.http_timeout_sec))
    api.login(conn.username, conn.password)

    print(f"Restoring backup: {args.backup}")
    restore_backup(api, args.backup)
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
