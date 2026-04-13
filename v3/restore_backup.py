from __future__ import annotations

from typing import Any, Dict, Set

import config
from api_clients import ZabbixAPI
from backup_io import load_backup
from common import normalize_scope_env, normalize_values, resolve_input_artifact


def _filter_keys(data: Dict[str, Any], allowed: Set[str]) -> Dict[str, Any]:
    return {key: value for key, value in (data or {}).items() if key in allowed}


def restore_backup(api: ZabbixAPI, backup_path: str) -> None:
    data = load_backup(backup_path)
    current_scope_as = normalize_values(config.SCOPE_AS)
    current_scope_env = normalize_scope_env(config.SCOPE_ENV)
    current_scope_gas = normalize_values(config.SCOPE_GAS)

    if str(data.meta.zabbix_url or "").strip() and str(data.meta.zabbix_url).strip() != str(getattr(api, "api_url", "")).strip():
        raise RuntimeError(
            f"Backup Zabbix URL mismatch: backup={data.meta.zabbix_url} current={getattr(api, 'api_url', '')}"
        )
    if current_scope_as and sorted(str(item).strip().lower() for item in data.meta.scope_as) != sorted(
        str(item).strip().lower() for item in current_scope_as
    ):
        raise RuntimeError(
            f"Backup scope_as mismatch: backup={data.meta.scope_as} current={current_scope_as}"
        )
    if str(data.meta.scope_env or "").strip() != str(current_scope_env or "").strip():
        raise RuntimeError(
            f"Backup scope_env mismatch: backup={data.meta.scope_env} current={current_scope_env}"
        )
    if current_scope_gas and sorted(str(item).strip().upper() for item in data.meta.scope_gas) != sorted(
        str(item).strip().upper() for item in current_scope_gas
    ):
        raise RuntimeError(
            f"Backup scope_gas mismatch: backup={data.meta.scope_gas} current={current_scope_gas}"
        )

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
    maintenance_allowed = {
        "name",
        "description",
        "maintenance_type",
        "active_since",
        "active_till",
        "timeperiods",
        "tags_evaltype",
        "tags",
        "groups",
        "hosts",
    }

    for user in data.users:
        payload: Dict[str, Any] = {"userid": user.userid}
        payload.update(_filter_keys(user.raw, user_allowed))
        api.call("user.update", payload)

    for usergroup in data.usergroups:
        payload: Dict[str, Any] = {"usrgrpid": usergroup.usrgrpid}
        payload.update(_filter_keys(usergroup.raw, usergroup_allowed))
        api.call("usergroup.update", payload)

    for action in data.actions:
        payload: Dict[str, Any] = {"actionid": action.actionid}
        payload.update(_filter_keys(action.raw, action_allowed))
        api.call("action.update", payload)

    for maintenance in data.maintenances:
        payload: Dict[str, Any] = {"maintenanceid": maintenance.maintenanceid}
        payload.update(_filter_keys(maintenance.raw, maintenance_allowed))
        api.call("maintenance.update", payload)

    for host in data.hosts:
        groups = []
        for group in host.groups:
            groupid = group.get("groupid")
            if groupid is not None:
                groups.append({"groupid": str(groupid)})
        payload = {"hostid": host.hostid, "groups": groups, "tags": host.tags}
        api.call("host.update", payload)


def main() -> int:
    backup_path = resolve_input_artifact(
        config.SOURCE_BACKUP_FILE,
        config.BACKUP_PREFIX,
        ".json.gz",
        scope_as=config.SCOPE_AS,
        scope_env=config.SCOPE_ENV,
        scope_gas=config.SCOPE_GAS,
        label="backup file",
        strict_scope_match=True,
    )

    connection = config.load_zabbix_connection()
    api = ZabbixAPI(connection.api_url, timeout_sec=int(config.HTTP_TIMEOUT_SEC))
    api.authenticate(connection.username, connection.password, connection.api_token)

    if not str(config.SOURCE_BACKUP_FILE or "").strip():
        print(f"Using latest backup file: {backup_path}")
    print(f"Restoring backup: {backup_path}")
    restore_backup(api, backup_path)
    print("Restore completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
