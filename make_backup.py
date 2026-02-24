#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""make_backup.py — создание бэкапа Zabbix для отката миграции."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import urllib3  # type: ignore

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from api_clients import ZabbixAPI
from backup_io import save_backup
from backup_model import ActionBackup, BackupData, BackupMeta, HostBackup, UserBackup, UserGroupBackup
from config import CONFIG, load_connection_from_env_or_prompt
from scope_utils import build_scope_backup_path, normalize_scope

import re


# =========================
# Утилиты
# =========================
EXCLUDED_GROUP_PATTERNS = [re.compile(p) for p in getattr(CONFIG.runtime, "excluded_group_patterns", ())]


def is_excluded_group(name: str) -> bool:
    if not name:
        return False
    return any(rx.search(name) for rx in EXCLUDED_GROUP_PATTERNS)


def get_tag_value(tags: List[Dict[str, Any]], tag_name: str) -> Optional[str]:
    for t in tags or []:
        if t.get("tag") == tag_name:
            v = t.get("value")
            if v is None:
                return None
            v = str(v).strip()
            return v if v else None
    return None


def is_as_new_group(name: str, as_val: Optional[str]) -> bool:
    if not as_val:
        return False
    n = str(name)
    parts = [p for p in n.split("/") if p != ""]
    if len(parts) < 3:
        return False
    prefix, kind, as_seg = parts[0], parts[1], parts[2]
    if prefix not in ("BNK", "DOM"):
        return False
    if kind != "AS":
        return False
    return str(as_seg).strip().lower() == str(as_val).strip().lower()


def is_old_legacy_group(name: str) -> bool:
    n = str(name)
    if "/" in n:
        return False
    return re.match(r"^(BNK|DOM)-", n) is not None


def _recursive_collect_groupids(obj: Any, hits: Set[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "groupid" and v is not None:
                hits.add(str(v))
            else:
                _recursive_collect_groupids(v, hits)
    elif isinstance(obj, list):
        for x in obj:
            _recursive_collect_groupids(x, hits)


def extract_action_recipients(action: Dict[str, Any]) -> Tuple[Set[str], Set[str]]:
    usrgrpids: Set[str] = set()
    userids: Set[str] = set()

    def scan_ops(ops: Any) -> None:
        for op in ops or []:
            for grp in (op.get("opmessage_grp") or []):
                ugid = grp.get("usrgrpid")
                if ugid is not None:
                    usrgrpids.add(str(ugid))
            for usr in (op.get("opmessage_usr") or []):
                uid = usr.get("userid")
                if uid is not None:
                    userids.add(str(uid))

    scan_ops(action.get("operations"))
    scan_ops(action.get("recovery_operations"))
    scan_ops(action.get("update_operations"))

    return usrgrpids, userids


# =========================
# Загрузчики
# =========================
def fetch_hosts(api: ZabbixAPI) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "output": ["hostid", "host", "name"],
        "selectGroups": ["groupid", "name"],
        "selectTags": ["tag", "value"],
    }
    if CONFIG.runtime.monitored_hosts_only:
        params["monitored_hosts"] = True
    return api.call("host.get", params)


def fetch_trigger_actions(api: ZabbixAPI) -> List[Dict[str, Any]]:
    return api.call(
        "action.get",
        {
            "output": "extend",
            "selectOperations": "extend",
            "selectRecoveryOperations": "extend",
            "selectUpdateOperations": "extend",
            "selectFilter": "extend",
            "filter": {"eventsource": 0},
        },
    )


def fetch_usergroups_with_rights(api: ZabbixAPI) -> List[Dict[str, Any]]:
    return api.call(
        "usergroup.get",
        {
            "output": "extend",
            "selectHostGroupRights": "extend",
            "selectTagFilters": "extend",
            "selectUsers": "extend",
        },
    )


def fetch_users(api: ZabbixAPI) -> List[Dict[str, Any]]:
    return api.call(
        "user.get",
        {
            "output": "extend",
            "selectMedias": "extend",
            "selectUsrgrps": ["usrgrpid", "name"],
        },
    )


# =========================
# Сборка бэкапа
# =========================
def build_backup_filename(scope_as: Optional[Iterable[str]], base_path: str) -> str:
    return build_scope_backup_path(base_path, scope_as)


def create_backup(
    api: Any,
    scope_as: Optional[Iterable[str]],
    output_path: str,
    hosts: Optional[List[Dict[str, Any]]] = None,
    actions: Optional[List[Dict[str, Any]]] = None,
    usergroups: Optional[List[Dict[str, Any]]] = None,
    users: Optional[List[Dict[str, Any]]] = None,
) -> BackupData:
    if not scope_as:
        raise RuntimeError("Backup scope is empty. Set CONFIG.runtime.audit_scope_as in config.py.")
    scope_lower = {str(x).strip().lower() for x in scope_as} if scope_as else None

    hosts = hosts if hosts is not None else fetch_hosts(api)
    actions = actions if actions is not None else fetch_trigger_actions(api)
    usergroups = usergroups if usergroups is not None else fetch_usergroups_with_rights(api)
    users = users if users is not None else fetch_users(api)

    hosts_in_scope: List[Dict[str, Any]] = []
    as_values_in_scope: Set[str] = set()
    asn_values_in_scope: Set[str] = set()
    relevant_groupids: Set[str] = set()

    for h in hosts:
        tags = h.get("tags") or []
        as_val = get_tag_value(tags, CONFIG.tags.AS)
        if not as_val:
            continue
        if scope_lower and str(as_val).strip().lower() not in scope_lower:
            continue
        hosts_in_scope.append(h)
        as_values_in_scope.add(str(as_val))
        asn_val = get_tag_value(tags, CONFIG.tags.ASN)
        if asn_val:
            asn_values_in_scope.add(str(asn_val))

        for g in (h.get("groups") or []):
            gname = g.get("name")
            gid = g.get("groupid")
            if not gname or not gid:
                continue
            if is_excluded_group(gname):
                continue
            if is_as_new_group(gname, as_val) or is_old_legacy_group(gname):
                relevant_groupids.add(str(gid))

    # actions in scope
    actions_selected: List[Dict[str, Any]] = []
    for a in actions:
        if not relevant_groupids:
            break
        cond_ids: Set[str] = set()
        flt = a.get("filter") or {}
        for c in flt.get("conditions") or []:
            if c.get("value") is None:
                continue
            if str(c.get("conditiontype")) == "0":
                cond_ids.add(str(c.get("value")))
            else:
                if str(c.get("value")).isdigit():
                    cond_ids.add(str(c.get("value")))
        op_ids: Set[str] = set()
        for ops_key in ("operations", "recovery_operations", "update_operations"):
            _recursive_collect_groupids(a.get(ops_key), op_ids)
        if cond_ids.intersection(relevant_groupids) or op_ids.intersection(relevant_groupids):
            actions_selected.append(a)

    # usergroups in scope
    usergroups_selected: List[Dict[str, Any]] = []
    if scope_lower is None:
        scope_as_values = {str(x).strip().lower() for x in as_values_in_scope if str(x).strip()}
    else:
        scope_as_values = {str(x).strip().lower() for x in (scope_as or []) if str(x).strip()}
    recipient_ugids: Set[str] = set()
    recipient_uids: Set[str] = set()
    for a in actions_selected:
        ugids, uids = extract_action_recipients(a)
        recipient_ugids.update(ugids)
        recipient_uids.update(uids)

    for ug in usergroups:
        rights = ug.get("hostgroup_rights") or []
        touched = [r for r in rights if str(r.get("groupid") or r.get("id") or r.get("hostgroupid") or "") in relevant_groupids]

        tag_filters = ug.get("tag_filters") or []
        tf_mentions = False
        for tf in tag_filters:
            t = tf.get("tag") or tf.get("tag_name") or tf.get("tagname")
            v = tf.get("value") if tf.get("value") is not None else tf.get("tagvalue")
            if not t or v is None:
                continue
            if t == CONFIG.tags.AS and str(v).strip().lower() in scope_as_values:
                tf_mentions = True
                break
            if t == CONFIG.tags.ASN and str(v) in asn_values_in_scope:
                tf_mentions = True
                break

        if touched or tf_mentions or (str(ug.get("usrgrpid")) in recipient_ugids):
            usergroups_selected.append(ug)

    # users in scope (по участию в usergroups + получатели действий)
    userids_needed: Set[str] = set()
    for ug in usergroups_selected:
        for u in (ug.get("users") or []):
            if u.get("userid") is not None:
                userids_needed.add(str(u.get("userid")))

    for a in actions_selected:
        ugids, uids = extract_action_recipients(a)
        userids_needed.update(uids)
        for ugid in ugids:
            for ug in usergroups_selected:
                if str(ug.get("usrgrpid")) != str(ugid):
                    continue
                for u in (ug.get("users") or []):
                    if u.get("userid") is not None:
                        userids_needed.add(str(u.get("userid")))

    userids_needed.update(recipient_uids)

    users_selected = [u for u in users if str(u.get("userid")) in userids_needed]

    meta = BackupMeta(
        created_at=datetime.now().isoformat(timespec="seconds"),
        scope_as=[str(x) for x in (scope_as or [])],
        zabbix_url=str(getattr(api, "api_url", "")),
    )

    data = BackupData(
        meta=meta,
        hosts=[
            HostBackup(
                hostid=str(h.get("hostid")),
                groups=h.get("groups") or [],
                tags=h.get("tags") or [],
            )
            for h in hosts_in_scope
        ],
        actions=[
            ActionBackup(
                actionid=str(a.get("actionid")),
                raw=a,
                filter=a.get("filter") or {},
                operations=a.get("operations") or [],
                recovery_operations=a.get("recovery_operations") or [],
                update_operations=a.get("update_operations") or [],
            )
            for a in actions_selected
            if a.get("actionid") is not None
        ],
        usergroups=[
            UserGroupBackup(
                usrgrpid=str(ug.get("usrgrpid")),
                name=str(ug.get("name") or ""),
                raw=ug,
                users=[{"userid": str(u.get("userid"))} for u in (ug.get("users") or []) if u.get("userid") is not None],
                hostgroup_rights=ug.get("hostgroup_rights") or [],
                tag_filters=ug.get("tag_filters") or [],
            )
            for ug in usergroups_selected
            if ug.get("usrgrpid") is not None
        ],
        users=[
            UserBackup(
                userid=str(u.get("userid")),
                username=str(u.get("username") or u.get("alias") or ""),
                name=str(u.get("name") or ""),
                surname=str(u.get("surname") or ""),
                raw=u,
                medias=u.get("medias") or [],
                usrgrps=[{"usrgrpid": str(g.get("usrgrpid"))} for g in (u.get("usrgrps") or []) if g.get("usrgrpid") is not None],
            )
            for u in users_selected
            if u.get("userid") is not None
        ],
    )

    save_backup(data, output_path)
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description="Zabbix backup (scope by AS list)")
    parser.add_argument("--out", dest="output", help="Backup filename (.json or .json.gz)")
    args = parser.parse_args()

    scope_as = normalize_scope(CONFIG.runtime.audit_scope_as)
    if not scope_as:
        raise RuntimeError("Backup scope is empty. Set CONFIG.runtime.audit_scope_as in config.py.")

    conn = load_connection_from_env_or_prompt(interactive=False)
    api = ZabbixAPI(conn.api_url, timeout_sec=int(CONFIG.runtime.http_timeout_sec))
    api.login(conn.username, conn.password)

    output_path = args.output
    if not output_path:
        output_path = build_backup_filename(scope_as, base_path=CONFIG.excel.output_xlsx)

    print(f"Creating backup: {output_path}")
    create_backup(api, scope_as or None, output_path)
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
