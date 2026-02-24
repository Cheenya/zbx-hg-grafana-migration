#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""restore_backup.py — откат Zabbix из бэкапа."""

from __future__ import annotations

import argparse
from typing import Any, Dict, Optional

import urllib3  # type: ignore

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests  # type: ignore

from backup_io import load_backup


def _filter_keys(data: Dict[str, Any], allowed: set) -> Dict[str, Any]:
    return {k: v for k, v in (data or {}).items() if k in allowed}
from config import CONFIG, load_connection_from_env_or_prompt


class ZabbixAPI:
    def __init__(self, api_url: str, timeout: int = 60):
        self.api_url = api_url
        self.timeout = timeout
        self.auth: Optional[str] = None
        self._id = 1

    def call(self, method: str, params: Dict[str, Any]) -> Any:
        payload: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._id,
        }
        self._id += 1
        if self.auth is not None:
            payload["auth"] = self.auth

        try:
            r = requests.post(
                self.api_url,
                json=payload,
                timeout=int(CONFIG.runtime.http_timeout_sec),
                verify=False,
            )
            r.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"Zabbix API error ({method}): {e}") from e
        data = r.json()

        if "error" in data:
            raise RuntimeError(f"Zabbix API error ({method}): {data['error']}")
        return data["result"]

    def login(self, username: str, password: str) -> None:
        self.auth = self.call("user.login", {"username": username, "password": password})


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
    api = ZabbixAPI(conn.api_url)
    api.login(conn.username, conn.password)

    print(f"Restoring backup: {args.backup}")
    restore_backup(api, args.backup)
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
