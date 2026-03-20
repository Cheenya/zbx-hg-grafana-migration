from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Sequence, Set

import config
from api_clients import ZabbixAPI
from common import (
    canonical_env_value,
    extract_action_groupids,
    extract_action_recipients,
    extract_active_media_sendto,
    get_tag_value,
    is_excluded_group,
    is_new_group_for_as,
    is_old_group,
    join_sorted,
    normalize_lower_set,
    normalize_scope_envs,
    normalize_values,
    resolve_tagfilter_tag,
    resolve_tagfilter_value,
    sample_host_names,
)


def fetch_hostgroups(api: ZabbixAPI) -> List[Dict[str, Any]]:
    return api.call("hostgroup.get", {"output": ["groupid", "name"]})


def fetch_hosts(api: ZabbixAPI) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "output": ["hostid", "host", "name", "status"],
        "selectGroups": ["groupid", "name"],
        "selectTags": ["tag", "value"],
    }
    if config.MONITORED_HOSTS_ONLY:
        params["monitored_hosts"] = True
    return api.call("host.get", params)


def fetch_actions(api: ZabbixAPI) -> List[Dict[str, Any]]:
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


def fetch_usergroups(api: ZabbixAPI) -> List[Dict[str, Any]]:
    return api.call(
        "usergroup.get",
        {
            "output": ["usrgrpid", "name"],
            "selectHostGroupRights": "extend",
            "selectTagFilters": "extend",
            "selectUsers": ["userid", "username", "alias", "name", "surname"],
        },
    )


def fetch_users(api: ZabbixAPI) -> List[Dict[str, Any]]:
    return api.call(
        "user.get",
        {
            "output": ["userid", "username", "alias", "name", "surname"],
            "selectMedias": "extend",
            "selectUsrgrps": ["usrgrpid", "name"],
        },
    )


def fetch_maintenances(api: ZabbixAPI) -> List[Dict[str, Any]]:
    return api.call(
        "maintenance.get",
        {
            "output": ["maintenanceid", "name", "active_since", "active_till"],
            "selectGroups": ["groupid", "name"],
        },
    )


def _user_display(user: Dict[str, Any]) -> str:
    username = str(user.get("username") or user.get("alias") or "").strip()
    full_name = f"{str(user.get('name') or '').strip()} {str(user.get('surname') or '').strip()}".strip()
    if username and full_name:
        return f"{username} ({full_name})"
    if username:
        return username
    if full_name:
        return full_name
    return f"userid={user.get('userid')}"


def _host_name(host: Dict[str, Any]) -> str:
    return str(host.get("name") or host.get("host") or host.get("hostid") or "")


def _group_bucket_rows(
    bucket: Dict[str, Dict[str, Any]],
    sample_limit: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for group_name, data in sorted(bucket.items(), key=lambda item: item[0].lower()):
        rows.append(
            {
                "group_name": group_name,
                "groupid": data["groupid"],
                "hosts_count": len(data["hostids"]),
                "as_values": join_sorted(data["as_values"]),
                "env_values": join_sorted(data["env_values"]),
                "sample_hosts": sample_host_names(data["host_names"], sample_limit),
            }
        )
    return rows


def build_scope_report(
    api: ZabbixAPI,
    scope_as: Sequence[str],
    scope_envs: Sequence[str],
) -> Dict[str, Any]:
    scope_as_values = normalize_values(scope_as)
    scope_as_lower = normalize_lower_set(scope_as_values)
    scope_env_values = normalize_scope_envs(scope_envs)
    scope_env_lower = normalize_lower_set(scope_env_values)

    if not scope_as_values:
        raise RuntimeError("v2 audit scope is empty. Set v2/config.py SCOPE_AS.")

    hostgroups = fetch_hostgroups(api)
    hosts = fetch_hosts(api)
    actions = fetch_actions(api)
    usergroups = fetch_usergroups(api)
    users = fetch_users(api)
    maintenances = fetch_maintenances(api)

    groupid_to_name: Dict[str, str] = {}
    for group in hostgroups:
        if group.get("groupid") is None or not group.get("name"):
            continue
        groupid_to_name[str(group["groupid"])] = str(group["name"])

    users_by_id: Dict[str, Dict[str, Any]] = {}
    user_media_by_id: Dict[str, List[str]] = {}
    for user in users:
        if user.get("userid") is None:
            continue
        user_id = str(user["userid"])
        users_by_id[user_id] = user
        user_media_by_id[user_id] = extract_active_media_sendto(user.get("medias") or [])

    scope_hosts: List[Dict[str, Any]] = []
    scope_hosts_skipped_env: List[Dict[str, Any]] = []
    old_bucket: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"groupid": "", "hostids": set(), "host_names": set(), "as_values": set(), "env_values": set()}
    )
    new_bucket: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"groupid": "", "hostids": set(), "host_names": set(), "as_values": set(), "env_values": set()}
    )
    scope_groupids: Set[str] = set()
    scope_asn_values: Set[str] = set()

    for host in hosts:
        tags = host.get("tags") or []
        as_value = get_tag_value(tags, config.TAG_AS)
        env_value_raw = get_tag_value(tags, config.TAG_ENV)
        env_value = canonical_env_value(env_value_raw)
        asn_value = get_tag_value(tags, config.TAG_ASN)

        if not as_value or as_value.strip().lower() not in scope_as_lower:
            continue

        host_row = {
            "hostid": str(host.get("hostid") or ""),
            "host": str(host.get("host") or ""),
            "name": str(host.get("name") or ""),
            "status": str(host.get("status") or ""),
            "AS": as_value or "",
            "ASN": asn_value or "",
            "ENV_RAW": env_value_raw or "",
            "ENV_SCOPE": env_value or "",
            "old_groups": "",
            "new_groups": "",
            "other_groups": "",
        }

        if scope_env_lower and (not env_value or env_value.strip().lower() not in scope_env_lower):
            host_row["skip_reason"] = "ENV mismatch"
            scope_hosts_skipped_env.append(host_row)
            continue

        scope_hosts.append(host_row)
        if asn_value:
            scope_asn_values.add(asn_value)

        host_name = _host_name(host)
        old_groups: List[str] = []
        new_groups: List[str] = []
        other_groups: List[str] = []

        for group in host.get("groups") or []:
            group_name = str(group.get("name") or "")
            group_id = str(group.get("groupid") or "")
            if not group_name or not group_id or is_excluded_group(group_name):
                continue

            if is_old_group(group_name):
                old_groups.append(group_name)
                scope_groupids.add(group_id)
                old_bucket[group_name]["groupid"] = group_id
                old_bucket[group_name]["hostids"].add(str(host.get("hostid") or ""))
                old_bucket[group_name]["host_names"].add(host_name)
                old_bucket[group_name]["as_values"].add(as_value)
                if env_value:
                    old_bucket[group_name]["env_values"].add(env_value)
                continue

            if is_new_group_for_as(group_name, as_value):
                new_groups.append(group_name)
                scope_groupids.add(group_id)
                new_bucket[group_name]["groupid"] = group_id
                new_bucket[group_name]["hostids"].add(str(host.get("hostid") or ""))
                new_bucket[group_name]["host_names"].add(host_name)
                new_bucket[group_name]["as_values"].add(as_value)
                if env_value:
                    new_bucket[group_name]["env_values"].add(env_value)
                continue

            other_groups.append(group_name)

        host_row["old_groups"] = join_sorted(old_groups)
        host_row["new_groups"] = join_sorted(new_groups)
        host_row["other_groups"] = join_sorted(other_groups)

    action_rows: List[Dict[str, Any]] = []
    recipient_usergroup_ids: Set[str] = set()
    recipient_user_ids: Set[str] = set()

    for action in actions:
        condition_ids, operation_ids = extract_action_groupids(action)
        matched_condition_ids = condition_ids.intersection(scope_groupids)
        matched_operation_ids = operation_ids.intersection(scope_groupids)
        matched_ids = matched_condition_ids.union(matched_operation_ids)
        if not matched_ids:
            continue

        if matched_condition_ids and matched_operation_ids:
            where_found = "both"
        elif matched_condition_ids:
            where_found = "conditions"
        else:
            where_found = "operations"

        action_usergroup_ids, action_user_ids = extract_action_recipients(action)
        recipient_usergroup_ids.update(action_usergroup_ids)
        recipient_user_ids.update(action_user_ids)

        resolved_user_ids = set(action_user_ids)
        for usergroup_id in action_usergroup_ids:
            for user in users:
                for membership in user.get("usrgrps") or []:
                    if str(membership.get("usrgrpid") or "") == usergroup_id:
                        resolved_user_ids.add(str(user.get("userid") or ""))

        recipients_media: Set[str] = set()
        for user_id in resolved_user_ids:
            recipients_media.update(user_media_by_id.get(user_id, []))

        action_rows.append(
            {
                "actionid": str(action.get("actionid") or ""),
                "name": str(action.get("name") or ""),
                "status": str(action.get("status") or ""),
                "where_found": where_found,
                "matched_groupids": join_sorted(matched_ids),
                "matched_group_names": join_sorted(groupid_to_name.get(group_id, group_id) for group_id in matched_ids),
                "recipient_usergroups": join_sorted(action_usergroup_ids),
                "recipient_users": join_sorted(_user_display(users_by_id[user_id]) for user_id in action_user_ids if user_id in users_by_id),
                "recipients_media": join_sorted(recipients_media),
            }
        )

    usergroup_rows: List[Dict[str, Any]] = []
    scoped_user_ids: Set[str] = set()
    for usergroup in usergroups:
        rights = usergroup.get("hostgroup_rights") or []
        touched_rights: List[str] = []
        for right in rights:
            group_id = str(right.get("groupid") or right.get("id") or right.get("hostgroupid") or "")
            if group_id not in scope_groupids:
                continue
            touched_rights.append(f"{groupid_to_name.get(group_id, group_id)}:{right.get('permission')}")

        tag_filters = usergroup.get("tag_filters") or []
        matching_filters: List[str] = []
        for tag_filter in tag_filters:
            tag_name = resolve_tagfilter_tag(tag_filter)
            tag_value = resolve_tagfilter_value(tag_filter)
            if not tag_name or tag_value is None:
                continue
            if tag_name == config.TAG_AS and tag_value.strip().lower() in scope_as_lower:
                matching_filters.append(f"{tag_name}={tag_value}")
            elif tag_name == config.TAG_ASN and tag_value in scope_asn_values:
                matching_filters.append(f"{tag_name}={tag_value}")
            elif scope_env_lower and tag_name == config.TAG_ENV and canonical_env_value(tag_value).strip().lower() in scope_env_lower:
                matching_filters.append(f"{tag_name}={tag_value}")

        usergroup_id = str(usergroup.get("usrgrpid") or "")
        if not touched_rights and not matching_filters and usergroup_id not in recipient_usergroup_ids:
            continue

        users_chunks: List[str] = []
        users_media: Set[str] = set()
        for user in usergroup.get("users") or []:
            user_id = str(user.get("userid") or "")
            if user_id:
                scoped_user_ids.add(user_id)
            users_chunks.append(_user_display(user))
            users_media.update(user_media_by_id.get(user_id, []))

        usergroup_rows.append(
            {
                "usrgrpid": usergroup_id,
                "name": str(usergroup.get("name") or ""),
                "rights_on_scope_groups": "; ".join(touched_rights),
                "matching_tag_filters": "; ".join(sorted(set(matching_filters))),
                "users": ", ".join(users_chunks),
                "users_media": join_sorted(users_media),
                "is_action_recipient": "yes" if usergroup_id in recipient_usergroup_ids else "",
            }
        )

    scoped_user_ids.update(recipient_user_ids)

    maintenance_rows: List[Dict[str, Any]] = []
    for maintenance in maintenances:
        matched_ids = {
            str(group.get("groupid") or "")
            for group in maintenance.get("groups") or []
            if str(group.get("groupid") or "") in scope_groupids
        }
        if not matched_ids:
            continue
        maintenance_rows.append(
            {
                "maintenanceid": str(maintenance.get("maintenanceid") or ""),
                "name": str(maintenance.get("name") or ""),
                "matched_groupids": join_sorted(matched_ids),
                "matched_group_names": join_sorted(groupid_to_name.get(group_id, group_id) for group_id in matched_ids),
                "active_since": str(maintenance.get("active_since") or ""),
                "active_till": str(maintenance.get("active_till") or ""),
            }
        )

    inventory_hostgroups = [
        {
            "groupid": row["groupid"],
            "name": row["group_name"],
            "kind": "OLD",
        }
        for row in _group_bucket_rows(old_bucket, config.GROUP_SAMPLE_HOSTS)
    ] + [
        {
            "groupid": row["groupid"],
            "name": row["group_name"],
            "kind": "NEW",
        }
        for row in _group_bucket_rows(new_bucket, config.GROUP_SAMPLE_HOSTS)
    ]

    summary = {
        "scope_as": scope_as_values,
        "scope_envs": scope_env_values,
        "env_policy": f"{config.ENV_PROD_LABEL} => {config.ENV_PROD_LABEL}; everything else => {config.ENV_NONPROD_LABEL}",
        "hosts_in_scope": len(scope_hosts),
        "hosts_skipped_env": len(scope_hosts_skipped_env),
        "old_groups": len(old_bucket),
        "new_groups": len(new_bucket),
        "actions": len(action_rows),
        "usergroups": len(usergroup_rows),
        "maintenances": len(maintenance_rows),
    }

    return {
        "summary": summary,
        "hosts": scope_hosts,
        "hosts_skipped_env": scope_hosts_skipped_env,
        "groups_old": _group_bucket_rows(old_bucket, config.GROUP_SAMPLE_HOSTS),
        "groups_new": _group_bucket_rows(new_bucket, config.GROUP_SAMPLE_HOSTS),
        "actions": action_rows,
        "usergroups": usergroup_rows,
        "maintenances": maintenance_rows,
        "grafana": [],
        "inventory": {
            "scope_as": scope_as_values,
            "scope_envs": scope_env_values,
            "hostids": sorted(row["hostid"] for row in scope_hosts if row.get("hostid")),
            "hostgroups": sorted(inventory_hostgroups, key=lambda item: (item["kind"], item["name"].lower())),
            "actionids": sorted(row["actionid"] for row in action_rows if row.get("actionid")),
            "usergroupids": sorted(row["usrgrpid"] for row in usergroup_rows if row.get("usrgrpid")),
            "userids": sorted(scoped_user_ids),
            "maintenanceids": sorted(row["maintenanceid"] for row in maintenance_rows if row.get("maintenanceid")),
        },
    }
