from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, Dict, List, Sequence, Set, Tuple

import config
from api_clients import ZabbixAPI
from common import (
    build_expected_hostgroups,
    canonical_env_value,
    extract_action_groupids,
    extract_action_recipients,
    extract_active_media_sendto,
    get_tag_value,
    is_excluded_group,
    is_old_group,
    join_sorted,
    normalize_lower_set,
    normalize_scope_env,
    normalize_upper_tag_value,
    normalize_values,
    parse_standard_group,
    resolve_host_org,
    resolve_os_family,
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



def _log(log: Callable[[str], None] | None, message: str) -> None:
    if log is not None:
        log(message)



def _is_old_group_for_as(name: str, as_value: str) -> bool:
    text = str(name or "").strip().lower()
    scope = str(as_value or "").strip().lower()
    if not text or not scope or "/" in text:
        return False
    normalized = scope.replace("_", "-")
    if text.startswith(f"bnk-{scope}") or text.startswith(f"dom-{scope}"):
        return True
    if text.startswith(f"bnk-{normalized}") or text.startswith(f"dom-{normalized}"):
        return True
    if "_" in scope:
        tail = scope.split("_", 1)[1]
        if tail and (text.startswith(f"bnk-{tail}") or text.startswith(f"dom-{tail}")):
            return True
    return False



def _group_matches_scope(group_name: str, scope_as_values: Sequence[str]) -> bool:
    parsed = parse_standard_group(group_name)
    for as_value in scope_as_values:
        if _is_old_group_for_as(group_name, as_value):
            return True
        if parsed and parsed.get("root_kind") == "AS":
            if str(parsed.get("as_value") or "").strip().lower() == str(as_value or "").strip().lower():
                return True
    return False



def _host_status_label(status: Any) -> str:
    return "enabled" if str(status or "") == "0" else "disabled"



def _display_value(value: str | None) -> str:
    text = str(value or "").strip()
    return text or "(empty)"



def _is_unknown_value(value: str | None) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return text == str(config.UNKNOWN_TAG_VALUE).strip()



def _unknown_reasons(host: Dict[str, Any]) -> List[str]:
    tags = host.get("tags") or []
    as_value = get_tag_value(tags, config.TAG_AS)
    asn_value = get_tag_value(tags, config.TAG_ASN)

    reasons: List[str] = []
    if _is_unknown_value(as_value):
        reasons.append("AS=UNKNOWN")
    if _is_unknown_value(asn_value):
        reasons.append("ASN=UNKNOWN")

    group_names = [
        str(group.get("name") or "")
        for group in (host.get("groups") or [])
        if str(group.get("name") or "") and not is_excluded_group(str(group.get("name") or ""))
    ]
    if str(config.UNKNOWN_GROUP_NAME or "").strip() in group_names:
        reasons.append("group=UNKNOWN")
    if not as_value:
        reasons.append("missing AS")
    return reasons



def _unknown_in_scope(host: Dict[str, Any], scope_as_values: Sequence[str], scope_as_lower: Set[str]) -> bool:
    tags = host.get("tags") or []
    as_value = get_tag_value(tags, config.TAG_AS)
    if as_value and as_value.strip().lower() in scope_as_lower:
        return True

    for group in host.get("groups") or []:
        group_name = str(group.get("name") or "")
        if not group_name or is_excluded_group(group_name):
            continue
        if _group_matches_scope(group_name, scope_as_values):
            return True
    return False



def _touch_value_summary(
    bucket: Dict[Any, Dict[str, Any]],
    key: Any,
    hostid: str,
    host_name: str,
    replace_candidate: bool,
    disabled: bool,
) -> None:
    row = bucket[key]
    row["hostids"].add(hostid)
    row["host_names"].add(host_name)
    if replace_candidate:
        row["replace_hostids"].add(hostid)
    if disabled:
        row["disabled_hostids"].add(hostid)



def _value_summary_rows(
    bucket: Dict[Any, Dict[str, Any]],
    columns: Sequence[str],
    sample_limit: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for key in sorted(bucket.keys(), key=lambda item: tuple(str(part).lower() for part in (item if isinstance(item, tuple) else (item,)))):
        data = bucket[key]
        if not isinstance(key, tuple):
            key = (key,)
        row = {columns[index]: key[index] if index < len(key) else "" for index in range(len(columns))}
        hosts_count = len(data["hostids"])
        disabled_hosts = len(data["disabled_hostids"])
        row.update(
            {
                "hosts_count": hosts_count,
                "enabled_hosts": hosts_count - disabled_hosts,
                "disabled_hosts": disabled_hosts,
                "replace_candidates": len(data["replace_hostids"]),
                "legacy_hosts": len(data["replace_hostids"]),
                "sample_hosts": sample_host_names(data["host_names"], sample_limit),
            }
        )
        rows.append(row)
    return rows



def _sort_host_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("AS") or "").lower(),
            str(row.get("name") or row.get("host") or "").lower(),
            str(row.get("hostid") or ""),
        ),
    )



def _iter_groupid_paths(node: Any, path: str, hits: List[Tuple[str, str]]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            child_path = f"{path}.{key}" if path else str(key)
            if key == "groupid" and value is not None:
                hits.append((child_path, str(value)))
            else:
                _iter_groupid_paths(value, child_path, hits)
        return
    if isinstance(node, list):
        for index, item in enumerate(node):
            child_path = f"{path}[{index}]" if path else f"[{index}]"
            _iter_groupid_paths(item, child_path, hits)



def _build_hostgroup_lookup(hostgroups: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    lookup: Dict[str, Dict[str, str]] = {}
    for group in hostgroups:
        name = str(group.get("name") or "").strip()
        groupid = str(group.get("groupid") or "").strip()
        if not name or not groupid:
            continue
        lookup[name.lower()] = {"groupid": groupid, "name": name}
    return lookup



def _ensure_old_bucket_row() -> Dict[str, Any]:
    return {
        "groupid": "",
        "hostids": set(),
        "host_names": set(),
        "org_values": set(),
        "as_values": set(),
        "env_raw_values": set(),
        "env_scope_values": set(),
    }



def _ensure_standard_bucket_row() -> Dict[str, Any]:
    return {
        "groupid": "",
        "group_kind": "",
        "org": "",
        "hostids": set(),
        "host_names": set(),
        "as_values": set(),
        "env_raw_values": set(),
        "env_scope_values": set(),
        "gas_values": set(),
        "os_families": set(),
    }



def _ensure_expected_bucket_row() -> Dict[str, Any]:
    return {
        "groupid": "",
        "group_kind": "",
        "org": "",
        "exists_in_zabbix": False,
        "hostids": set(),
        "host_names": set(),
        "present_hostids": set(),
        "missing_hostids": set(),
        "as_values": set(),
        "env_raw_values": set(),
        "env_scope_values": set(),
        "gas_values": set(),
        "os_families": set(),
    }



def _old_bucket_rows(bucket: Dict[str, Dict[str, Any]], sample_limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for group_name, data in sorted(bucket.items(), key=lambda item: item[0].lower()):
        rows.append(
            {
                "group_name": group_name,
                "groupid": data["groupid"],
                "org_values": join_sorted(data["org_values"]),
                "as_values": join_sorted(data["as_values"]),
                "env_raw_values": join_sorted(data["env_raw_values"]),
                "env_scope_values": join_sorted(data["env_scope_values"]),
                "hosts_count": len(data["hostids"]),
                "sample_hosts": sample_host_names(data["host_names"], sample_limit),
            }
        )
    return rows



def _standard_bucket_rows(bucket: Dict[str, Dict[str, Any]], sample_limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for group_name, data in sorted(bucket.items(), key=lambda item: item[0].lower()):
        rows.append(
            {
                "group_name": group_name,
                "groupid": data["groupid"],
                "group_kind": data["group_kind"],
                "org": data["org"],
                "as_values": join_sorted(data["as_values"]),
                "env_raw_values": join_sorted(data["env_raw_values"]),
                "env_scope_values": join_sorted(data["env_scope_values"]),
                "gas_values": join_sorted(data["gas_values"]),
                "os_families": join_sorted(data["os_families"]),
                "hosts_count": len(data["hostids"]),
                "sample_hosts": sample_host_names(data["host_names"], sample_limit),
            }
        )
    return rows



def _expected_bucket_rows(bucket: Dict[str, Dict[str, Any]], sample_limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for group_name, data in sorted(bucket.items(), key=lambda item: item[0].lower()):
        rows.append(
            {
                "group_name": group_name,
                "groupid": data["groupid"],
                "group_kind": data["group_kind"],
                "org": data["org"],
                "exists_in_zabbix": "yes" if data["exists_in_zabbix"] else "",
                "source_hosts_count": len(data["hostids"]),
                "host_present_count": len(data["present_hostids"]),
                "host_missing_count": len(data["missing_hostids"]),
                "as_values": join_sorted(data["as_values"]),
                "env_raw_values": join_sorted(data["env_raw_values"]),
                "env_scope_values": join_sorted(data["env_scope_values"]),
                "gas_values": join_sorted(data["gas_values"]),
                "os_families": join_sorted(data["os_families"]),
                "sample_hosts": sample_host_names(data["host_names"], sample_limit),
            }
        )
    return rows



def _build_mapping_plan_rows(
    old_bucket: Dict[str, Dict[str, Any]],
    standard_bucket: Dict[str, Dict[str, Any]],
    host_mapping_targets: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    per_old: Dict[str, List[Dict[str, Any]]] = {}

    for old_name, old_data in old_bucket.items():
        old_hostids = set(old_data["hostids"])
        old_count = len(old_hostids)
        old_orgs = sorted(str(item).strip() for item in old_data["org_values"] if str(item).strip())
        old_as_values = sorted(str(item).strip() for item in old_data["as_values"] if str(item).strip())
        old_env_raws = sorted(str(item).strip() for item in old_data["env_raw_values"] if str(item).strip())
        old_env_scopes = sorted(str(item).strip() for item in old_data["env_scope_values"] if str(item).strip())

        candidate_state: Dict[str, Dict[str, Any]] = {}
        for hostid in old_hostids:
            for target in host_mapping_targets.get(hostid, []):
                target_name = str(target.get("group_name") or "").strip()
                if not target_name:
                    continue
                state = candidate_state.setdefault(
                    target_name,
                    {
                        "group_name": target_name,
                        "groupid": str(target.get("groupid") or ""),
                        "group_kind": str(target.get("group_kind") or ""),
                        "target_exists": bool(target.get("exists_in_zabbix")),
                        "org": str(target.get("org") or ""),
                        "target_env_raw": str(target.get("env_raw") or ""),
                        "target_scope_hostids": set(),
                    },
                )
                state["target_scope_hostids"].add(hostid)

        rows: List[Dict[str, Any]] = []
        if not candidate_state:
            per_old[old_name] = [
                {
                    "selected": "",
                    "AS": join_sorted(old_as_values),
                    "ORG": join_sorted(old_orgs),
                    "old_group": old_name,
                    "old_groupid": str(old_data.get("groupid") or ""),
                    "new_group": "",
                    "new_groupid": "",
                    "target_kind": "",
                    "target_exists": "",
                    "candidate_rank": "",
                    "candidate_count": 0,
                    "intersection": 0,
                    "old_hosts_count": old_count,
                    "target_scope_hosts": 0,
                    "new_hosts_count": 0,
                    "old_coverage": 0.0,
                    "new_coverage": 0.0,
                    "jaccard": 0.0,
                    "host_action": "",
                    "hosts_need_add_new": old_count,
                    "hosts_already_have_new": 0,
                    "old_orgs": join_sorted(old_orgs),
                    "old_envs": join_sorted(old_env_raws),
                    "old_env_scopes": join_sorted(old_env_scopes),
                    "target_env_raw": "",
                    "auto_reason": "",
                    "top1_new_conflict": "",
                    "manual_required": "yes",
                    "status": "no_candidate",
                    "comment": "",
                }
            ]
            continue

        for state in candidate_state.values():
            target_name = str(state["group_name"])
            target_scope_hostids = set(state["target_scope_hostids"])
            target_group_hostids = set((standard_bucket.get(target_name) or {}).get("hostids") or set())
            intersection = len(old_hostids.intersection(target_group_hostids))
            target_scope_hosts = len(target_scope_hostids)
            new_count = len(target_group_hostids)
            union_count = old_count + new_count - intersection
            old_coverage = (intersection / old_count) if old_count else 0.0
            new_coverage = (intersection / new_count) if new_count else 0.0
            jaccard = (intersection / union_count) if union_count else 0.0
            rows.append(
                {
                    "selected": "",
                    "AS": join_sorted(old_as_values),
                    "ORG": join_sorted(old_orgs),
                    "old_group": old_name,
                    "old_groupid": str(old_data.get("groupid") or ""),
                    "new_group": target_name,
                    "new_groupid": str(state.get("groupid") or ""),
                    "target_kind": str(state.get("group_kind") or ""),
                    "target_exists": "yes" if bool(state.get("target_exists")) else "",
                    "candidate_rank": 0,
                    "candidate_count": 0,
                    "intersection": intersection,
                    "old_hosts_count": old_count,
                    "target_scope_hosts": target_scope_hosts,
                    "new_hosts_count": new_count,
                    "old_coverage": round(old_coverage, 4),
                    "new_coverage": round(new_coverage, 4),
                    "jaccard": round(jaccard, 4),
                    "host_action": "add_new_if_missing",
                    "hosts_need_add_new": max(target_scope_hosts - intersection, 0),
                    "hosts_already_have_new": intersection,
                    "old_orgs": join_sorted(old_orgs),
                    "old_envs": join_sorted(old_env_raws),
                    "old_env_scopes": join_sorted(old_env_scopes),
                    "target_env_raw": str(state.get("target_env_raw") or ""),
                    "auto_reason": "",
                    "top1_new_conflict": "",
                    "manual_required": "yes",
                    "status": "candidate",
                    "comment": "",
                }
            )

        unique_org = len(old_orgs) == 1
        unique_as = len(old_as_values) == 1
        unique_env = len(old_env_raws) == 1
        preferred_kind = "AS_ENV" if unique_env else "AS"

        def sort_key(row: Dict[str, Any]) -> Tuple[int, int, int, int, int, str]:
            exists_score = 1 if str(row.get("target_exists") or "") == "yes" else 0
            preferred_score = 1 if str(row.get("target_kind") or "") == preferred_kind else 0
            return (
                exists_score,
                preferred_score,
                int(row.get("intersection") or 0),
                int(row.get("target_scope_hosts") or 0),
                int(row.get("new_hosts_count") or 0),
                str(row.get("new_group") or "").lower(),
            )

        rows.sort(key=sort_key, reverse=True)
        existing_rows = [row for row in rows if str(row.get("target_exists") or "") == "yes"]
        mixed_host_tags = not (unique_org and unique_as)
        for index, row in enumerate(rows, start=1):
            row["candidate_rank"] = index
            row["candidate_count"] = len(rows)
            row["top1_new_conflict"] = ""
            if not str(row.get("target_exists") or ""):
                row["status"] = "missing_target_group"
                row["manual_required"] = "yes"
                row["selected"] = ""
                continue
            if mixed_host_tags:
                row["status"] = "mixed_host_tags"
                row["manual_required"] = "yes"
                row["selected"] = ""
                continue
            if len(existing_rows) == 1 and row is existing_rows[0]:
                row["status"] = "auto_selected"
                row["manual_required"] = ""
                row["selected"] = "yes"
                row["auto_reason"] = "single_existing_candidate"
                continue
            if index == 1:
                row["status"] = "preferred_env" if preferred_kind == "AS_ENV" else "preferred_base"
            else:
                row["status"] = "candidate"
            row["manual_required"] = "yes"
            row["selected"] = ""
        per_old[old_name] = rows

    out: List[Dict[str, Any]] = []
    for old_name in sorted(per_old.keys(), key=str.lower):
        out.extend(per_old[old_name])
    return out



def _mapping_candidates_by_oldid(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    bucket: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        old_groupid = str(row.get("old_groupid") or "").strip()
        if not old_groupid:
            continue
        bucket[old_groupid].append(row)
    for candidates in bucket.values():
        candidates.sort(key=lambda item: (int(item.get("candidate_rank") or 0), str(item.get("new_group") or "").lower()))
    return bucket



def _build_host_enrichment_rows(
    host_rows: Sequence[Dict[str, Any]],
    host_expected_groups: Sequence[Dict[str, Any]],
    old_name_to_groupid: Dict[str, str],
    mapping_candidates: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    expected_by_hostid: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in host_expected_groups:
        hostid = str(row.get("hostid") or "").strip()
        if hostid:
            expected_by_hostid[hostid].append(row)

    rows: List[Dict[str, Any]] = []
    for host_row in host_rows:
        hostid = str(host_row.get("hostid") or "")
        expected_rows = expected_by_hostid.get(hostid, [])
        old_groups = [item.strip() for item in str(host_row.get("old_groups") or "").split(",") if item.strip()]

        expected_by_kind: Dict[str, Set[str]] = defaultdict(set)
        catalog_existing: Set[str] = set()
        catalog_missing: Set[str] = set()
        host_present: Set[str] = set()
        host_missing: Set[str] = set()
        for expected in expected_rows:
            group_name = str(expected.get("group_name") or "").strip()
            group_kind = str(expected.get("group_kind") or "").strip()
            if not group_name:
                continue
            expected_by_kind[group_kind].add(group_name)
            exists_in_zabbix = str(expected.get("exists_in_zabbix") or "") == "yes"
            on_host = str(expected.get("on_host") or "") == "yes"
            if exists_in_zabbix:
                catalog_existing.add(group_name)
            else:
                catalog_missing.add(group_name)
            if on_host:
                host_present.add(group_name)
            elif exists_in_zabbix:
                host_missing.add(group_name)

        suggested_pairs: List[str] = []
        suggested_new_groups: Set[str] = set()
        mapping_manual = False
        for old_group in old_groups:
            old_groupid = old_name_to_groupid.get(old_group, "")
            candidates = mapping_candidates.get(old_groupid) or []
            if not candidates:
                mapping_manual = True
                suggested_pairs.append(f"{old_group} -> (no candidate)")
                continue
            top = candidates[0]
            new_group = str(top.get("new_group") or "").strip()
            status = str(top.get("status") or "")
            if new_group:
                suggested_new_groups.add(new_group)
                suggested_pairs.append(f"{old_group} -> {new_group} [{status}]")
            else:
                suggested_pairs.append(f"{old_group} -> (empty)")
            if status != "auto_selected":
                mapping_manual = True

        unresolved_reasons = str(host_row.get("org_resolution_reasons") or "").strip()
        rows.append(
            {
                "hostid": host_row.get("hostid", ""),
                "host": host_row.get("host", ""),
                "name": host_row.get("name", ""),
                "status": host_row.get("status", ""),
                "status_label": host_row.get("status_label", ""),
                "ORG": host_row.get("ORG", ""),
                "AS": host_row.get("AS", ""),
                "ASN": host_row.get("ASN", ""),
                "ENV_RAW": host_row.get("ENV_RAW", ""),
                "ENV_SCOPE": host_row.get("ENV_SCOPE", ""),
                "GAS": host_row.get("GAS", ""),
                "GUEST_NAME": host_row.get("GUEST_NAME", ""),
                "OS_FAMILY": host_row.get("OS_FAMILY", ""),
                "old_groups": host_row.get("old_groups", ""),
                "standard_groups": host_row.get("standard_groups", ""),
                "expected_env_groups": join_sorted(expected_by_kind.get("ENV", set())),
                "expected_as_groups": join_sorted(expected_by_kind.get("AS", set()).union(expected_by_kind.get("AS_ENV", set()))),
                "expected_gas_groups": join_sorted(
                    expected_by_kind.get("GAS", set()).union(expected_by_kind.get("GAS_ENV", set())).union(expected_by_kind.get("GAS_AS_ENV", set()))
                ),
                "expected_os_groups": join_sorted(expected_by_kind.get("OS", set()).union(expected_by_kind.get("OS_ENV", set()))),
                "catalog_existing_groups": join_sorted(catalog_existing),
                "catalog_missing_groups": join_sorted(catalog_missing),
                "host_present_expected_groups": join_sorted(host_present),
                "host_missing_expected_groups": join_sorted(host_missing),
                "suggested_pairs": "; ".join(suggested_pairs),
                "suggested_new_groups": join_sorted(suggested_new_groups),
                "host_action": "add_existing_expected_groups" if host_missing else "",
                "unresolved_reasons": unresolved_reasons,
                "manual_required": "yes" if catalog_missing or unresolved_reasons or mapping_manual else "",
            }
        )
    return rows



def _preview_rows_for_object(
    object_type: str,
    object_id: str,
    object_name: str,
    where_found: str,
    field_path: str,
    old_groupid: str,
    mapping_candidates: Dict[str, List[Dict[str, Any]]],
    groupid_to_name: Dict[str, str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    old_group = groupid_to_name.get(old_groupid, old_groupid)
    candidates = mapping_candidates.get(old_groupid) or []
    if not candidates:
        rows.append(
            {
                "object_type": object_type,
                "object_id": object_id,
                "object_name": object_name,
                "where_found": where_found,
                "field_path": field_path,
                "old_group": old_group,
                "old_groupid": old_groupid,
                "candidate_new_group": "",
                "candidate_new_groupid": "",
                "candidate_rank": "",
                "candidate_count": 0,
                "target_kind": "",
                "target_exists": "",
                "mapping_status": "no_candidate",
                "manual_required": "yes",
                "host_action": "",
                "hosts_need_add_new": "",
                "hosts_already_have_new": "",
            }
        )
        return rows

    for candidate in candidates:
        rows.append(
            {
                "object_type": object_type,
                "object_id": object_id,
                "object_name": object_name,
                "where_found": where_found,
                "field_path": field_path,
                "old_group": old_group,
                "old_groupid": old_groupid,
                "candidate_new_group": str(candidate.get("new_group") or ""),
                "candidate_new_groupid": str(candidate.get("new_groupid") or ""),
                "candidate_rank": candidate.get("candidate_rank", ""),
                "candidate_count": candidate.get("candidate_count", 0),
                "target_kind": str(candidate.get("target_kind") or ""),
                "target_exists": str(candidate.get("target_exists") or ""),
                "mapping_status": str(candidate.get("status") or ""),
                "manual_required": str(candidate.get("manual_required") or ""),
                "host_action": str(candidate.get("host_action") or ""),
                "hosts_need_add_new": candidate.get("hosts_need_add_new", ""),
                "hosts_already_have_new": candidate.get("hosts_already_have_new", ""),
            }
        )
    return rows



def build_scope_report(
    api: ZabbixAPI,
    scope_as: Sequence[str],
    scope_env: str,
    log: Callable[[str], None] | None = None,
) -> Dict[str, Any]:
    scope_as_values = normalize_values(scope_as)
    scope_as_lower = normalize_lower_set(scope_as_values)
    scope_env_value = normalize_scope_env(scope_env)
    scope_env_lower = {scope_env_value.lower()} if scope_env_value else set()

    if not scope_as_values:
        raise RuntimeError("Audit scope is empty. Set SCOPE_AS in config.py.")

    _log(log, f"zabbix: scope_as={scope_as_values} scope_env={scope_env_value or '(all)'}")
    _log(log, "zabbix: fetching hostgroups")
    hostgroups = fetch_hostgroups(api)
    _log(log, f"zabbix: fetched hostgroups={len(hostgroups)}")
    _log(log, "zabbix: fetching hosts")
    hosts = fetch_hosts(api)
    _log(log, f"zabbix: fetched hosts={len(hosts)} monitored_only={config.MONITORED_HOSTS_ONLY}")
    _log(log, "zabbix: fetching actions")
    actions = fetch_actions(api)
    _log(log, f"zabbix: fetched actions={len(actions)}")
    _log(log, "zabbix: fetching usergroups")
    usergroups = fetch_usergroups(api)
    _log(log, f"zabbix: fetched usergroups={len(usergroups)}")
    _log(log, "zabbix: fetching users")
    users = fetch_users(api)
    _log(log, f"zabbix: fetched users={len(users)}")
    _log(log, "zabbix: fetching maintenances")
    maintenances = fetch_maintenances(api)
    _log(log, f"zabbix: fetched maintenances={len(maintenances)}")

    hostgroup_lookup = _build_hostgroup_lookup(hostgroups)
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
    scope_hosts_replace: List[Dict[str, Any]] = []
    scope_hosts_clean: List[Dict[str, Any]] = []
    scope_hosts_disabled: List[Dict[str, Any]] = []
    scope_hosts_no_any_new: List[Dict[str, Any]] = []
    scope_hosts_skipped_env: List[Dict[str, Any]] = []
    scope_groupids: Set[str] = set()
    scope_asn_values: Set[str] = set()
    unknown_rows: List[Dict[str, Any]] = []
    old_bucket: Dict[str, Dict[str, Any]] = defaultdict(_ensure_old_bucket_row)
    standard_bucket: Dict[str, Dict[str, Any]] = defaultdict(_ensure_standard_bucket_row)
    expected_bucket: Dict[str, Dict[str, Any]] = defaultdict(_ensure_expected_bucket_row)
    host_expected_groups: List[Dict[str, Any]] = []
    host_mapping_targets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    env_summary_bucket: Dict[Any, Dict[str, Any]] = defaultdict(
        lambda: {"hostids": set(), "host_names": set(), "replace_hostids": set(), "disabled_hostids": set()}
    )
    asn_summary_bucket: Dict[Any, Dict[str, Any]] = defaultdict(
        lambda: {"hostids": set(), "host_names": set(), "replace_hostids": set(), "disabled_hostids": set()}
    )
    gas_summary_bucket: Dict[Any, Dict[str, Any]] = defaultdict(
        lambda: {"hostids": set(), "host_names": set(), "replace_hostids": set(), "disabled_hostids": set()}
    )
    guest_name_summary_bucket: Dict[Any, Dict[str, Any]] = defaultdict(
        lambda: {"hostids": set(), "host_names": set(), "replace_hostids": set(), "disabled_hostids": set()}
    )

    for host in hosts:
        tags = host.get("tags") or []
        as_value = get_tag_value(tags, config.TAG_AS)
        as_upper = normalize_upper_tag_value(as_value)
        env_value_raw = get_tag_value(tags, config.TAG_ENV)
        env_raw_upper = normalize_upper_tag_value(env_value_raw)
        env_value = canonical_env_value(env_value_raw)
        asn_value = get_tag_value(tags, config.TAG_ASN)
        gas_value = get_tag_value(tags, config.TAG_GAS)
        gas_upper = normalize_upper_tag_value(gas_value)
        guest_name = get_tag_value(tags, config.TAG_GUEST_NAME)
        os_family = resolve_os_family(guest_name)
        unknown_reasons = _unknown_reasons(host)
        filtered_groups = [
            group
            for group in (host.get("groups") or [])
            if str(group.get("name") or "") and not is_excluded_group(str(group.get("name") or ""))
        ]
        group_names = [str(group.get("name") or "") for group in filtered_groups]

        if unknown_reasons:
            include_unknown = _unknown_in_scope(host, scope_as_values, scope_as_lower)
            if include_unknown:
                unknown_rows.append(
                    {
                        "hostid": str(host.get("hostid") or ""),
                        "host": str(host.get("host") or ""),
                        "name": str(host.get("name") or ""),
                        "status": str(host.get("status") or ""),
                        "status_label": _host_status_label(host.get("status")),
                        "ORG": resolve_host_org(group_names)[0],
                        "AS": as_value or "",
                        "ASN": asn_value or "",
                        "GAS": gas_value or "",
                        "GUEST_NAME": guest_name or "",
                        "OS_FAMILY": os_family,
                        "ENV_RAW": env_value_raw or "",
                        "ENV_SCOPE": env_value or "",
                        "groups": join_sorted(group_names),
                        "unknown_reasons": ", ".join(unknown_reasons),
                    }
                )
            if config.EXCLUDE_UNKNOWN_FROM_STATS:
                continue

        if not as_value or as_value.strip().lower() not in scope_as_lower:
            continue

        host_row = {
            "hostid": str(host.get("hostid") or ""),
            "host": str(host.get("host") or ""),
            "name": str(host.get("name") or ""),
            "status": str(host.get("status") or ""),
            "status_label": _host_status_label(host.get("status")),
            "ORG": "",
            "AS": as_value or "",
            "ASN": asn_value or "",
            "GAS": gas_value or "",
            "GUEST_NAME": guest_name or "",
            "OS_FAMILY": os_family,
            "ENV_RAW": env_value_raw or "",
            "ENV_SCOPE": env_value or "",
            "old_groups": "",
            "new_groups": "",
            "standard_groups": "",
            "env_groups": "",
            "as_groups": "",
            "gas_groups": "",
            "os_groups": "",
            "other_groups": "",
            "replace_candidate": "",
            "has_old_groups": "",
            "has_standard_groups": "",
            "missing_any_new_group": "",
            "org_resolution_reasons": "",
        }

        if scope_env_lower and (not env_value or env_value.strip().lower() not in scope_env_lower):
            host_row["skip_reason"] = "ENV mismatch"
            scope_hosts_skipped_env.append(host_row)
            continue

        if asn_value:
            scope_asn_values.add(asn_value)

        org_value, org_reasons = resolve_host_org(group_names)
        host_row["ORG"] = org_value
        host_row["org_resolution_reasons"] = "; ".join(org_reasons)

        host_name = _host_name(host)
        old_groups: List[str] = []
        standard_groups: List[str] = []
        env_groups: List[str] = []
        as_groups: List[str] = []
        gas_groups: List[str] = []
        os_groups: List[str] = []
        other_groups: List[str] = []
        assigned_standard_lookup: Set[str] = set()

        for group in filtered_groups:
            group_name = str(group.get("name") or "")
            group_id = str(group.get("groupid") or "")
            if not group_name or not group_id:
                continue

            if is_old_group(group_name):
                old_groups.append(group_name)
                scope_groupids.add(group_id)
                bucket = old_bucket[group_name]
                bucket["groupid"] = group_id
                bucket["hostids"].add(str(host.get("hostid") or ""))
                bucket["host_names"].add(host_name)
                if org_value:
                    bucket["org_values"].add(org_value)
                if as_upper:
                    bucket["as_values"].add(as_upper)
                if env_raw_upper:
                    bucket["env_raw_values"].add(env_raw_upper)
                if env_value:
                    bucket["env_scope_values"].add(env_value)
                continue

            parsed_standard = parse_standard_group(group_name)
            if parsed_standard:
                standard_groups.append(group_name)
                assigned_standard_lookup.add(group_name.lower())
                scope_groupids.add(group_id)
                bucket = standard_bucket[group_name]
                bucket["groupid"] = group_id
                bucket["group_kind"] = str(parsed_standard.get("group_kind") or "")
                bucket["org"] = str(parsed_standard.get("org") or "")
                bucket["hostids"].add(str(host.get("hostid") or ""))
                bucket["host_names"].add(host_name)
                if parsed_standard.get("as_value"):
                    bucket["as_values"].add(str(parsed_standard["as_value"]))
                if parsed_standard.get("env_raw"):
                    bucket["env_raw_values"].add(str(parsed_standard["env_raw"]))
                if env_value:
                    bucket["env_scope_values"].add(env_value)
                if parsed_standard.get("gas_value"):
                    bucket["gas_values"].add(str(parsed_standard["gas_value"]))
                if parsed_standard.get("os_family"):
                    bucket["os_families"].add(str(parsed_standard["os_family"]))
                group_kind = str(parsed_standard.get("group_kind") or "")
                if group_kind.startswith("ENV"):
                    env_groups.append(group_name)
                elif group_kind.startswith("AS"):
                    as_groups.append(group_name)
                elif group_kind.startswith("GAS"):
                    gas_groups.append(group_name)
                elif group_kind.startswith("OS"):
                    os_groups.append(group_name)
                continue

            other_groups.append(group_name)

        expected_groups = build_expected_hostgroups(
            org_value,
            as_value,
            env_value_raw,
            env_value,
            gas_value,
            guest_name,
        )
        host_mapping_targets_seen: Set[str] = set()
        for expected in expected_groups:
            group_name = str(expected.get("group_name") or "").strip()
            if not group_name:
                continue
            lookup_hit = hostgroup_lookup.get(group_name.lower())
            exists_in_zabbix = lookup_hit is not None
            group_id = str((lookup_hit or {}).get("groupid") or "")
            on_host = group_name.lower() in assigned_standard_lookup
            expected_row = {
                "hostid": str(host.get("hostid") or ""),
                "host": str(host.get("host") or ""),
                "name": str(host.get("name") or ""),
                "status": str(host.get("status") or ""),
                "status_label": _host_status_label(host.get("status")),
                "ORG": org_value,
                "AS": as_upper,
                "ASN": asn_value or "",
                "GAS": gas_upper,
                "GUEST_NAME": guest_name or "",
                "OS_FAMILY": os_family,
                "ENV_RAW": env_raw_upper,
                "ENV_SCOPE": env_value,
                "group_name": group_name,
                "groupid": group_id,
                "group_kind": str(expected.get("group_kind") or ""),
                "exists_in_zabbix": "yes" if exists_in_zabbix else "",
                "on_host": "yes" if on_host else "",
            }
            host_expected_groups.append(expected_row)

            bucket = expected_bucket[group_name]
            bucket["groupid"] = group_id
            bucket["group_kind"] = str(expected.get("group_kind") or "")
            bucket["org"] = org_value
            bucket["exists_in_zabbix"] = exists_in_zabbix
            bucket["hostids"].add(str(host.get("hostid") or ""))
            bucket["host_names"].add(host_name)
            if on_host:
                bucket["present_hostids"].add(str(host.get("hostid") or ""))
            else:
                bucket["missing_hostids"].add(str(host.get("hostid") or ""))
            if as_upper:
                bucket["as_values"].add(as_upper)
            if env_raw_upper:
                bucket["env_raw_values"].add(env_raw_upper)
            if env_value:
                bucket["env_scope_values"].add(env_value)
            if gas_upper:
                bucket["gas_values"].add(gas_upper)
            if os_family:
                bucket["os_families"].add(os_family)

            group_kind = str(expected.get("group_kind") or "")
            if group_kind not in {"AS", "AS_ENV"}:
                continue
            key = group_name.lower()
            if key in host_mapping_targets_seen:
                continue
            host_mapping_targets_seen.add(key)
            host_mapping_targets[str(host.get("hostid") or "")].append(
                {
                    "group_name": group_name,
                    "groupid": group_id,
                    "group_kind": group_kind,
                    "exists_in_zabbix": exists_in_zabbix,
                    "org": org_value,
                    "env_raw": str(expected.get("env_raw") or ""),
                }
            )

        host_row["old_groups"] = join_sorted(old_groups)
        host_row["new_groups"] = join_sorted(standard_groups)
        host_row["standard_groups"] = join_sorted(standard_groups)
        host_row["env_groups"] = join_sorted(env_groups)
        host_row["as_groups"] = join_sorted(as_groups)
        host_row["gas_groups"] = join_sorted(gas_groups)
        host_row["os_groups"] = join_sorted(os_groups)
        host_row["other_groups"] = join_sorted(other_groups)
        replace_candidate = bool(old_groups)
        missing_any_new_group = bool(old_groups and not standard_groups)
        has_standard_groups = bool(standard_groups)
        is_disabled = host_row["status_label"] == "disabled"
        host_row["replace_candidate"] = "yes" if replace_candidate else ""
        host_row["has_old_groups"] = "yes" if replace_candidate else ""
        host_row["has_standard_groups"] = "yes" if has_standard_groups else ""
        host_row["missing_any_new_group"] = "yes" if missing_any_new_group else ""

        scope_hosts.append(host_row)
        if replace_candidate:
            scope_hosts_replace.append(dict(host_row))
        else:
            scope_hosts_clean.append(dict(host_row))
        if is_disabled:
            scope_hosts_disabled.append(dict(host_row))
        if missing_any_new_group:
            scope_hosts_no_any_new.append(dict(host_row))

        host_id = str(host.get("hostid") or "")
        _touch_value_summary(
            env_summary_bucket,
            (as_value or "", _display_value(env_value_raw), _display_value(env_value)),
            host_id,
            host_name,
            replace_candidate,
            is_disabled,
        )
        _touch_value_summary(
            asn_summary_bucket,
            (as_value or "", _display_value(asn_value)),
            host_id,
            host_name,
            replace_candidate,
            is_disabled,
        )
        _touch_value_summary(
            gas_summary_bucket,
            (as_value or "", _display_value(gas_value)),
            host_id,
            host_name,
            replace_candidate,
            is_disabled,
        )
        _touch_value_summary(
            guest_name_summary_bucket,
            (as_value or "", _display_value(guest_name)),
            host_id,
            host_name,
            replace_candidate,
            is_disabled,
        )

    _log(
        log,
        "zabbix: host scan completed "
        f"scope_hosts={len(scope_hosts)} old_scope={len(scope_hosts_replace)} "
        f"no_any_new={len(scope_hosts_no_any_new)} disabled={len(scope_hosts_disabled)} "
        f"skipped_env={len(scope_hosts_skipped_env)} unknown={len(unknown_rows)}",
    )

    mapping_plan_rows = _build_mapping_plan_rows(old_bucket, standard_bucket, host_mapping_targets)
    mapping_candidates = _mapping_candidates_by_oldid(mapping_plan_rows)
    old_scope_groupids = {str(data["groupid"]) for data in old_bucket.values() if str(data.get("groupid") or "").strip()}
    old_name_to_groupid = {str(name): str(data.get("groupid") or "") for name, data in old_bucket.items()}
    zabbix_mapping_preview: List[Dict[str, Any]] = []
    _log(
        log,
        f"zabbix: group buckets old={len(old_bucket)} standard={len(standard_bucket)} expected={len(expected_bucket)} mapping_plan_rows={len(mapping_plan_rows)}",
    )

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

        for index, condition in enumerate((action.get("filter") or {}).get("conditions") or []):
            if str(condition.get("conditiontype") or "") != "0":
                continue
            group_id = str(condition.get("value") or "")
            if group_id not in old_scope_groupids:
                continue
            zabbix_mapping_preview.extend(
                _preview_rows_for_object(
                    "action",
                    str(action.get("actionid") or ""),
                    str(action.get("name") or ""),
                    "conditions",
                    f"filter.conditions[{index}].value",
                    group_id,
                    mapping_candidates,
                    groupid_to_name,
                )
            )

        action_groupid_hits: List[Tuple[str, str]] = []
        for key in ("operations", "recovery_operations", "update_operations"):
            _iter_groupid_paths(action.get(key), key, action_groupid_hits)
        for field_path, group_id in action_groupid_hits:
            if group_id not in old_scope_groupids:
                continue
            zabbix_mapping_preview.extend(
                _preview_rows_for_object(
                    "action",
                    str(action.get("actionid") or ""),
                    str(action.get("name") or ""),
                    "operations",
                    field_path,
                    group_id,
                    mapping_candidates,
                    groupid_to_name,
                )
            )

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
    _log(log, f"zabbix: matched actions={len(action_rows)}")

    usergroup_rows: List[Dict[str, Any]] = []
    scoped_user_ids: Set[str] = set()
    for usergroup in usergroups:
        rights = usergroup.get("hostgroup_rights") or []
        touched_rights: List[str] = []
        for index, right in enumerate(rights):
            group_id = str(right.get("groupid") or right.get("id") or right.get("hostgroupid") or "")
            if group_id not in scope_groupids:
                continue
            touched_rights.append(f"{groupid_to_name.get(group_id, group_id)}:{right.get('permission')}")
            if group_id in old_scope_groupids:
                zabbix_mapping_preview.extend(
                    _preview_rows_for_object(
                        "usergroup",
                        str(usergroup.get("usrgrpid") or ""),
                        str(usergroup.get("name") or ""),
                        "hostgroup_rights",
                        f"hostgroup_rights[{index}].groupid",
                        group_id,
                        mapping_candidates,
                        groupid_to_name,
                    )
                )

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
    _log(log, f"zabbix: matched usergroups={len(usergroup_rows)} scoped_users={len(scoped_user_ids.union(recipient_user_ids))}")

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
        for index, group in enumerate(maintenance.get("groups") or []):
            group_id = str(group.get("groupid") or "")
            if group_id not in old_scope_groupids:
                continue
            zabbix_mapping_preview.extend(
                _preview_rows_for_object(
                    "maintenance",
                    str(maintenance.get("maintenanceid") or ""),
                    str(maintenance.get("name") or ""),
                    "groups",
                    f"groups[{index}].groupid",
                    group_id,
                    mapping_candidates,
                    groupid_to_name,
                )
            )
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
    _log(log, f"zabbix: matched maintenances={len(maintenance_rows)}")

    preview_seen: Set[Tuple[str, ...]] = set()
    preview_rows_sorted: List[Dict[str, Any]] = []
    for row in sorted(
        zabbix_mapping_preview,
        key=lambda item: (
            str(item.get("object_type") or ""),
            str(item.get("object_name") or "").lower(),
            str(item.get("field_path") or ""),
            str(item.get("candidate_rank") or ""),
            str(item.get("candidate_new_group") or "").lower(),
        ),
    ):
        signature = tuple(
            str(row.get(key) or "")
            for key in (
                "object_type",
                "object_id",
                "field_path",
                "old_groupid",
                "candidate_new_groupid",
                "candidate_rank",
                "mapping_status",
            )
        )
        if signature in preview_seen:
            continue
        preview_seen.add(signature)
        preview_rows_sorted.append(row)

    host_rows_sorted = _sort_host_rows(scope_hosts)
    host_enrichment_rows = _build_host_enrichment_rows(host_rows_sorted, host_expected_groups, old_name_to_groupid, mapping_candidates)
    hosts_needing_enrichment = [
        row
        for row in host_enrichment_rows
        if str(row.get("host_action") or "").strip() or str(row.get("catalog_missing_groups") or "").strip() or str(row.get("unresolved_reasons") or "").strip()
    ]
    _log(
        log,
        f"zabbix: host_enrichment_rows={len(host_enrichment_rows)} hosts_need_enrichment={len(hosts_needing_enrichment)} zbx_map_preview_rows(before_dedup)={len(zabbix_mapping_preview)}",
    )

    grafana_old_groups_dedup: List[Dict[str, str]] = []
    grafana_old_seen: Set[Tuple[str, str]] = set()
    for group_name, data in sorted(old_bucket.items(), key=lambda item: item[0].lower()):
        group_id = str(data.get("groupid") or "")
        for as_value in sorted({str(item).strip() for item in data.get("as_values") or set() if str(item).strip()}, key=str.lower):
            signature = (as_value, group_id)
            if signature in grafana_old_seen:
                continue
            grafana_old_seen.add(signature)
            grafana_old_groups_dedup.append({"groupid": group_id, "name": group_name, "kind": "OLD", "AS": as_value})
    _log(log, f"zabbix: grafana_old_groups_dedup={len(grafana_old_groups_dedup)}")

    inventory_hostgroups = [
        {"groupid": row["groupid"], "name": row["group_name"], "kind": "OLD"}
        for row in _old_bucket_rows(old_bucket, config.GROUP_SAMPLE_HOSTS)
    ] + [
        {"groupid": row["groupid"], "name": row["group_name"], "kind": row["group_kind"]}
        for row in _standard_bucket_rows(standard_bucket, config.GROUP_SAMPLE_HOSTS)
    ]

    env_summary_rows = _value_summary_rows(env_summary_bucket, ["AS", "ENV_RAW", "ENV_SCOPE"], config.GROUP_SAMPLE_HOSTS)
    asn_summary_rows = _value_summary_rows(asn_summary_bucket, ["AS", "ASN"], config.GROUP_SAMPLE_HOSTS)
    gas_summary_rows = _value_summary_rows(gas_summary_bucket, ["AS", "GAS"], config.GROUP_SAMPLE_HOSTS)
    guest_name_summary_rows = _value_summary_rows(guest_name_summary_bucket, ["AS", "GUEST_NAME"], config.GROUP_SAMPLE_HOSTS)
    expected_group_rows = _expected_bucket_rows(expected_bucket, config.GROUP_SAMPLE_HOSTS)

    summary = {
        "scope_as": scope_as_values,
        "scope_env": scope_env_value,
        "env_policy": f"{config.ENV_PROD_LABEL} => {config.ENV_PROD_LABEL}; everything else => {config.ENV_NONPROD_LABEL}",
        "hosts_in_scope": len(scope_hosts),
        "hosts_old_scope": len(scope_hosts_replace),
        "hosts_no_any_new": len(scope_hosts_no_any_new),
        "hosts_need_enrichment": len(hosts_needing_enrichment),
        "hosts_enrichment": len(host_enrichment_rows),
        "hosts_clean": len(scope_hosts_clean),
        "hosts_disabled": len(scope_hosts_disabled),
        "hosts_skipped_env": len(scope_hosts_skipped_env),
        "unknown_hosts": len(unknown_rows),
        "env_values": len(env_summary_rows),
        "asn_values": len(asn_summary_rows),
        "gas_values": len(gas_summary_rows),
        "guest_name_values": len(guest_name_summary_rows),
        "old_groups": len(old_bucket),
        "standard_groups": len(standard_bucket),
        "expected_groups": len(expected_group_rows),
        "expected_groups_missing_catalog": sum(1 for row in expected_group_rows if not str(row.get("exists_in_zabbix") or "").strip()),
        "mapping_plan_rows": len(mapping_plan_rows),
        "zabbix_mapping_preview_rows": len(preview_rows_sorted),
        "actions": len(action_rows),
        "usergroups": len(usergroup_rows),
        "maintenances": len(maintenance_rows),
    }
    _log(log, f"zabbix: summary={summary}")

    return {
        "summary": summary,
        "unknown_hosts": _sort_host_rows(unknown_rows),
        "hosts": host_rows_sorted,
        "hosts_replace": _sort_host_rows(scope_hosts_replace),
        "hosts_no_any_new": _sort_host_rows(scope_hosts_no_any_new),
        "host_enrichment": host_enrichment_rows,
        "hosts_need_enrichment": _sort_host_rows(hosts_needing_enrichment),
        "hosts_clean": _sort_host_rows(scope_hosts_clean),
        "hosts_disabled": _sort_host_rows(scope_hosts_disabled),
        "hosts_skipped_env": _sort_host_rows(scope_hosts_skipped_env),
        "host_expected_groups": sorted(
            host_expected_groups,
            key=lambda row: (
                str(row.get("AS") or "").lower(),
                str(row.get("name") or "").lower(),
                str(row.get("group_name") or "").lower(),
            ),
        ),
        "env_summary": env_summary_rows,
        "asn_summary": asn_summary_rows,
        "gas_summary": gas_summary_rows,
        "guest_name_summary": guest_name_summary_rows,
        "groups_old": _old_bucket_rows(old_bucket, config.GROUP_SAMPLE_HOSTS),
        "groups_new": _standard_bucket_rows(standard_bucket, config.GROUP_SAMPLE_HOSTS),
        "expected_groups": expected_group_rows,
        "mapping_plan": mapping_plan_rows,
        "zabbix_mapping_preview": preview_rows_sorted,
        "actions": action_rows,
        "usergroups": usergroup_rows,
        "maintenances": maintenance_rows,
        "grafana": [],
        "grafana_summary": [],
        "inventory": {
            "scope_as": scope_as_values,
            "scope_env": scope_env_value,
            "hostids": sorted(row["hostid"] for row in scope_hosts if row.get("hostid")),
            "hostgroups": sorted(inventory_hostgroups, key=lambda item: (item["kind"], item["name"].lower())),
            "grafana_old_groups": grafana_old_groups_dedup,
            "actionids": sorted(row["actionid"] for row in action_rows if row.get("actionid")),
            "usergroupids": sorted(row["usrgrpid"] for row in usergroup_rows if row.get("usrgrpid")),
            "userids": sorted(scoped_user_ids),
            "maintenanceids": sorted(row["maintenanceid"] for row in maintenance_rows if row.get("maintenanceid")),
        },
    }
