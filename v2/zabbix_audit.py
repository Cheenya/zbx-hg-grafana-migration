from __future__ import annotations

import re
from collections import Counter, defaultdict
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
    normalize_scope_env,
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


def _is_old_group_for_as(name: str, as_value: str) -> bool:
    text = str(name or "").strip().lower()
    scope = str(as_value or "").strip().lower()
    if not text or not scope or "/" in text:
        return False
    return text.startswith(f"bnk-{scope}") or text.startswith(f"dom-{scope}")


def _group_matches_scope(group_name: str, scope_as_values: Sequence[str]) -> bool:
    for as_value in scope_as_values:
        if _is_old_group_for_as(group_name, as_value) or is_new_group_for_as(group_name, as_value):
            return True
    return False


def _host_status_label(status: Any) -> str:
    return "enabled" if str(status or "") == "0" else "disabled"


def _display_value(value: str | None) -> str:
    text = str(value or "").strip()
    return text or "(empty)"


def _compile_patterns(values: Sequence[str]) -> List[re.Pattern[str]]:
    return [re.compile(item) for item in values if str(item or "").strip()]


def _match_host_hints(
    patterns: Sequence[re.Pattern[str]],
    host: Dict[str, Any],
    as_value: str | None,
    asn_value: str | None,
    env_value_raw: str | None,
    gas_value: str | None,
    guest_name: str | None,
    group_names: Sequence[str],
) -> str:
    candidates = [
        ("host", str(host.get("host") or "").strip()),
        ("name", str(host.get("name") or "").strip()),
        ("AS", str(as_value or "").strip()),
        ("ASN", str(asn_value or "").strip()),
        ("ENV", str(env_value_raw or "").strip()),
        ("GAS", str(gas_value or "").strip()),
        ("GUEST_NAME", str(guest_name or "").strip()),
    ]
    for group_name in group_names:
        candidates.append(("group", str(group_name or "").strip()))

    hits: List[str] = []
    seen: Set[str] = set()
    for pattern in patterns:
        for field_name, value in candidates:
            if not value or not pattern.search(value):
                continue
            hit = f"{field_name}:{value}"
            if hit in seen:
                continue
            seen.add(hit)
            hits.append(hit)
    return "; ".join(hits)


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


def _env_relation(old_envs: Set[str], new_envs: Set[str]) -> str:
    if not old_envs or not new_envs:
        return "unknown"
    if old_envs.intersection(new_envs):
        if old_envs == new_envs:
            return "match"
        return "overlap"
    return "mismatch"


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


def _iter_groupid_paths(node: Any, path: str, hits: List[tuple[str, str]]) -> None:
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


def _build_mapping_plan_rows(
    old_bucket: Dict[str, Dict[str, Any]],
    new_bucket: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    per_old: Dict[str, List[Dict[str, Any]]] = {}

    for old_name, old_data in old_bucket.items():
        old_hostids = set(old_data["hostids"])
        old_count = len(old_hostids)
        old_as_values = set(old_data["as_values"])
        old_envs = {str(item).strip() for item in old_data["env_values"] if str(item).strip()}
        rows: List[Dict[str, Any]] = []

        for new_name, new_data in new_bucket.items():
            new_as_values = set(new_data["as_values"])
            if old_as_values and new_as_values and not old_as_values.intersection(new_as_values):
                continue

            new_hostids = set(new_data["hostids"])
            new_count = len(new_hostids)
            if old_count == 0 or new_count == 0:
                continue

            intersection = len(old_hostids.intersection(new_hostids))
            if intersection < int(config.MAPPING_MIN_INTERSECTION):
                continue

            old_coverage = intersection / old_count
            new_coverage = intersection / new_count
            if old_coverage < float(config.MAPPING_MIN_OLD_COVERAGE) and new_coverage < float(config.MAPPING_MIN_NEW_COVERAGE):
                continue

            union_count = old_count + new_count - intersection
            jaccard = (intersection / union_count) if union_count else 0.0
            new_envs = {str(item).strip() for item in new_data["env_values"] if str(item).strip()}
            rows.append(
                {
                    "selected": "",
                    "AS": join_sorted(old_as_values or new_as_values),
                    "old_group": old_name,
                    "old_groupid": str(old_data["groupid"] or ""),
                    "new_group": new_name,
                    "new_groupid": str(new_data["groupid"] or ""),
                    "candidate_rank": 0,
                    "candidate_count": 0,
                    "intersection": intersection,
                    "old_hosts_count": old_count,
                    "new_hosts_count": new_count,
                    "old_coverage": round(old_coverage, 4),
                    "new_coverage": round(new_coverage, 4),
                    "jaccard": round(jaccard, 4),
                    "host_action": "add_new_if_missing",
                    "hosts_need_add_new": max(old_count - intersection, 0),
                    "hosts_already_have_new": intersection,
                    "old_envs": join_sorted(old_envs),
                    "new_envs": join_sorted(new_envs),
                    "env_relation": _env_relation(old_envs, new_envs),
                    "top1_new_conflict": "",
                    "manual_required": "yes",
                    "status": "",
                    "comment": "",
                }
            )

        rows.sort(
            key=lambda item: (
                int(item["intersection"]),
                float(item["old_coverage"]),
                float(item["new_coverage"]),
                float(item["jaccard"]),
                str(item["new_group"]).lower(),
            ),
            reverse=True,
        )

        if not rows:
            per_old[old_name] = [
                {
                    "selected": "",
                    "AS": join_sorted(old_as_values),
                    "old_group": old_name,
                    "old_groupid": str(old_data["groupid"] or ""),
                    "new_group": "",
                    "new_groupid": "",
                    "candidate_rank": "",
                    "candidate_count": 0,
                    "intersection": 0,
                    "old_hosts_count": old_count,
                    "new_hosts_count": "",
                    "old_coverage": "",
                    "new_coverage": "",
                    "jaccard": "",
                    "host_action": "",
                    "hosts_need_add_new": old_count,
                    "hosts_already_have_new": 0,
                    "old_envs": join_sorted(old_envs),
                    "new_envs": "",
                    "env_relation": "",
                    "top1_new_conflict": "",
                    "manual_required": "yes",
                    "status": "no_candidate",
                    "comment": "",
                }
            ]
            continue

        for index, row in enumerate(rows, start=1):
            row["candidate_rank"] = index
            row["candidate_count"] = len(rows)
        per_old[old_name] = rows

    top1_new_counter: Counter[str] = Counter()
    for rows in per_old.values():
        first = rows[0] if rows else {}
        new_group = str(first.get("new_group") or "").strip()
        if new_group:
            top1_new_counter[new_group] += 1

    out: List[Dict[str, Any]] = []
    for old_name in sorted(per_old.keys(), key=str.lower):
        rows = per_old[old_name]
        for row in rows:
            new_group = str(row.get("new_group") or "").strip()
            has_many_candidates = int(row.get("candidate_count") or 0) > 1
            top1_conflict = bool(new_group and int(row.get("candidate_rank") or 0) == 1 and top1_new_counter[new_group] > 1)
            env_mismatch = row.get("env_relation") == "mismatch"

            row["top1_new_conflict"] = "yes" if top1_conflict else ""
            if not new_group:
                row["status"] = "no_candidate"
                row["manual_required"] = "yes"
                row["selected"] = ""
            elif env_mismatch and bool(config.MAPPING_FORBID_ENV_MISMATCH):
                row["status"] = "env_mismatch"
                row["manual_required"] = "yes"
                row["selected"] = ""
            elif has_many_candidates:
                row["status"] = "ambiguous_old"
                row["manual_required"] = "yes"
                row["selected"] = ""
            elif top1_conflict:
                row["status"] = "ambiguous_new"
                row["manual_required"] = "yes"
                row["selected"] = ""
            elif int(row.get("candidate_rank") or 0) == 1:
                row["status"] = "auto_selected"
                row["manual_required"] = ""
                row["selected"] = "yes"
            else:
                row["status"] = "candidate"
                row["manual_required"] = "yes"
                row["selected"] = ""
            out.append(row)

    return out


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
) -> Dict[str, Any]:
    scope_as_values = normalize_values(scope_as)
    scope_as_lower = normalize_lower_set(scope_as_values)
    scope_env_value = normalize_scope_env(scope_env)
    scope_env_lower = {scope_env_value.lower()} if scope_env_value else set()

    if not scope_as_values:
        raise RuntimeError("v2 audit scope is empty. Set v2/config.py SCOPE_AS.")

    physical_patterns = _compile_patterns(config.PHYSICAL_HINT_PATTERNS)
    discovery_patterns = _compile_patterns(config.DISCOVERY_HINT_PATTERNS)

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
    scope_hosts_replace: List[Dict[str, Any]] = []
    scope_hosts_clean: List[Dict[str, Any]] = []
    scope_hosts_disabled: List[Dict[str, Any]] = []
    scope_hosts_no_any_new: List[Dict[str, Any]] = []
    scope_hosts_physical: List[Dict[str, Any]] = []
    scope_hosts_discovery: List[Dict[str, Any]] = []
    scope_hosts_skipped_env: List[Dict[str, Any]] = []
    old_bucket: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"groupid": "", "hostids": set(), "host_names": set(), "as_values": set(), "env_values": set()}
    )
    new_bucket: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"groupid": "", "hostids": set(), "host_names": set(), "as_values": set(), "env_values": set()}
    )
    scope_groupids: Set[str] = set()
    scope_asn_values: Set[str] = set()
    unknown_rows: List[Dict[str, Any]] = []
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
        env_value_raw = get_tag_value(tags, config.TAG_ENV)
        env_value = canonical_env_value(env_value_raw)
        asn_value = get_tag_value(tags, config.TAG_ASN)
        gas_value = get_tag_value(tags, config.TAG_GAS)
        guest_name = get_tag_value(tags, config.TAG_GUEST_NAME)
        unknown_reasons = _unknown_reasons(host)
        group_names = [
            str(group.get("name") or "")
            for group in (host.get("groups") or [])
            if str(group.get("name") or "") and not is_excluded_group(str(group.get("name") or ""))
        ]

        physical_hint_reasons = _match_host_hints(
            physical_patterns,
            host,
            as_value,
            asn_value,
            env_value_raw,
            gas_value,
            guest_name,
            group_names,
        )
        discovery_hint_reasons = _match_host_hints(
            discovery_patterns,
            host,
            as_value,
            asn_value,
            env_value_raw,
            gas_value,
            guest_name,
            group_names,
        )

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
                        "AS": as_value or "",
                        "ASN": asn_value or "",
                        "GAS": gas_value or "",
                        "GUEST_NAME": guest_name or "",
                        "ENV_RAW": env_value_raw or "",
                        "ENV_SCOPE": env_value or "",
                        "groups": join_sorted(group_names),
                        "physical_hint": "yes" if physical_hint_reasons else "",
                        "physical_hint_reasons": physical_hint_reasons,
                        "discovery_hint": "yes" if discovery_hint_reasons else "",
                        "discovery_hint_reasons": discovery_hint_reasons,
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
            "AS": as_value or "",
            "ASN": asn_value or "",
            "GAS": gas_value or "",
            "GUEST_NAME": guest_name or "",
            "ENV_RAW": env_value_raw or "",
            "ENV_SCOPE": env_value or "",
            "old_groups": "",
            "new_groups": "",
            "other_groups": "",
            "physical_hint": "yes" if physical_hint_reasons else "",
            "physical_hint_reasons": physical_hint_reasons,
            "discovery_hint": "yes" if discovery_hint_reasons else "",
            "discovery_hint_reasons": discovery_hint_reasons,
            "replace_candidate": "",
            "has_old_groups": "",
            "missing_any_new_group": "",
        }

        if scope_env_lower and (not env_value or env_value.strip().lower() not in scope_env_lower):
            host_row["skip_reason"] = "ENV mismatch"
            scope_hosts_skipped_env.append(host_row)
            continue

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
        replace_candidate = bool(old_groups)
        missing_any_new_group = bool(old_groups and not new_groups)
        is_disabled = host_row["status_label"] == "disabled"
        host_row["replace_candidate"] = "yes" if replace_candidate else ""
        host_row["has_old_groups"] = "yes" if replace_candidate else ""
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
        if physical_hint_reasons:
            scope_hosts_physical.append(dict(host_row))
        if discovery_hint_reasons:
            scope_hosts_discovery.append(dict(host_row))

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

    mapping_plan_rows = _build_mapping_plan_rows(old_bucket, new_bucket)
    mapping_candidates = _mapping_candidates_by_oldid(mapping_plan_rows)
    old_scope_groupids = {str(data["groupid"]) for data in old_bucket.values() if str(data.get("groupid") or "").strip()}
    zabbix_mapping_preview: List[Dict[str, Any]] = []

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

        action_groupid_hits: List[tuple[str, str]] = []
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

    preview_seen: Set[tuple[str, ...]] = set()
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

    env_summary_rows = _value_summary_rows(
        env_summary_bucket,
        ["AS", "ENV_RAW", "ENV_SCOPE"],
        config.GROUP_SAMPLE_HOSTS,
    )
    asn_summary_rows = _value_summary_rows(
        asn_summary_bucket,
        ["AS", "ASN"],
        config.GROUP_SAMPLE_HOSTS,
    )
    gas_summary_rows = _value_summary_rows(
        gas_summary_bucket,
        ["AS", "GAS"],
        config.GROUP_SAMPLE_HOSTS,
    )
    guest_name_summary_rows = _value_summary_rows(
        guest_name_summary_bucket,
        ["AS", "GUEST_NAME"],
        config.GROUP_SAMPLE_HOSTS,
    )

    summary = {
        "scope_as": scope_as_values,
        "scope_env": scope_env_value,
        "env_policy": f"{config.ENV_PROD_LABEL} => {config.ENV_PROD_LABEL}; everything else => {config.ENV_NONPROD_LABEL}",
        "hosts_in_scope": len(scope_hosts),
        "hosts_old_scope": len(scope_hosts_replace),
        "hosts_no_any_new": len(scope_hosts_no_any_new),
        "hosts_clean": len(scope_hosts_clean),
        "hosts_disabled": len(scope_hosts_disabled),
        "hosts_physical_hint": len(scope_hosts_physical),
        "hosts_discovery_hint": len(scope_hosts_discovery),
        "hosts_skipped_env": len(scope_hosts_skipped_env),
        "unknown_hosts": len(unknown_rows),
        "env_values": len(env_summary_rows),
        "asn_values": len(asn_summary_rows),
        "gas_values": len(gas_summary_rows),
        "guest_name_values": len(guest_name_summary_rows),
        "old_groups": len(old_bucket),
        "new_groups": len(new_bucket),
        "mapping_plan_rows": len(mapping_plan_rows),
        "zabbix_mapping_preview_rows": len(preview_rows_sorted),
        "actions": len(action_rows),
        "usergroups": len(usergroup_rows),
        "maintenances": len(maintenance_rows),
    }

    return {
        "summary": summary,
        "unknown_hosts": _sort_host_rows(unknown_rows),
        "hosts": _sort_host_rows(scope_hosts),
        "hosts_replace": _sort_host_rows(scope_hosts_replace),
        "hosts_no_any_new": _sort_host_rows(scope_hosts_no_any_new),
        "hosts_clean": _sort_host_rows(scope_hosts_clean),
        "hosts_disabled": _sort_host_rows(scope_hosts_disabled),
        "hosts_physical": _sort_host_rows(scope_hosts_physical),
        "hosts_discovery": _sort_host_rows(scope_hosts_discovery),
        "hosts_skipped_env": _sort_host_rows(scope_hosts_skipped_env),
        "env_summary": env_summary_rows,
        "asn_summary": asn_summary_rows,
        "gas_summary": gas_summary_rows,
        "guest_name_summary": guest_name_summary_rows,
        "groups_old": _group_bucket_rows(old_bucket, config.GROUP_SAMPLE_HOSTS),
        "groups_new": _group_bucket_rows(new_bucket, config.GROUP_SAMPLE_HOSTS),
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
            "actionids": sorted(row["actionid"] for row in action_rows if row.get("actionid")),
            "usergroupids": sorted(row["usrgrpid"] for row in usergroup_rows if row.get("usrgrpid")),
            "userids": sorted(scoped_user_ids),
            "maintenanceids": sorted(row["maintenanceid"] for row in maintenance_rows if row.get("maintenanceid")),
        },
    }
