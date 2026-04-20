from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Sequence, Set, Tuple

import config
from api_clients import GrafanaAPI
from common import join_sorted, normalize_values


OLD_RX = re.compile(r"\b(?:BNK|DOM)-[A-Za-z0-9_.:-]+(?:-[A-Za-z0-9_.:-]+)*\b")
NEW_RX = re.compile(r"\b(?:BNK|DOM)/[A-Za-z0-9_.:/-]+\b")
REGEX_META_RX = re.compile(r"[\\^$*+?()\[\]{}|]")
QUERY_FIELD_MARKERS = (".query", ".definition", ".expr", ".expression", ".rawsql", ".sql", ".regex")
PATTERN_FIELD_KINDS = {"query", "definition", "expression", "sql", "regex", "current", "options"}
GROUP_FIELD_MARKERS = (
    ".group",
    ".groups",
    ".groupby",
    ".group_by",
    ".groupfilter",
    ".group_filter",
    ".filter",
    ".filters",
    ".query",
    ".definition",
    ".regex",
    ".current.",
    ".options[",
)

HOST_FILTER_MARKERS = (
    ".host.filter",
    ".hosts.filter",
    ".host_filter",
    ".hosts_filter",
)


def _log(log: Callable[[str], None] | None, message: str) -> None:
    if log is not None:
        log(message)


def _iter_strings(node: Any, path: str = "", exclude_keys: Set[str] | None = None) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    excluded = exclude_keys or set()
    if isinstance(node, str):
        out.append((path, node))
        return out
    if isinstance(node, list):
        for index, item in enumerate(node):
            child_path = f"{path}[{index}]" if path else f"[{index}]"
            out.extend(_iter_strings(item, child_path, excluded))
        return out
    if isinstance(node, dict):
        for key, value in node.items():
            if key in excluded:
                continue
            child_path = f"{path}.{key}" if path else str(key)
            out.extend(_iter_strings(value, child_path, excluded))
        return out
    return out


def _walk_panels(panels: Sequence[Dict[str, Any]], path: str = "dashboard.panels") -> Iterable[Tuple[Dict[str, Any], str]]:
    for index, panel in enumerate(panels or []):
        panel_path = f"{path}[{index}]"
        yield panel, panel_path
        nested = panel.get("panels") or []
        if nested:
            yield from _walk_panels(nested, f"{panel_path}.panels")


def _legacy_suffix(group_name: str) -> str:
    text = str(group_name or "").strip()
    if "-" not in text:
        return ""
    return text.split("-", 1)[1].strip()


def _legacy_suffix_tokens(group_name: str) -> List[str]:
    suffix = _legacy_suffix(group_name)
    return [token.strip() for token in suffix.split("-") if token.strip()]


def _common_prefix_tokens(groups: Sequence[str]) -> List[str]:
    token_rows = [_legacy_suffix_tokens(item) for item in groups if _legacy_suffix_tokens(item)]
    if not token_rows:
        return []
    prefix = list(token_rows[0])
    for row in token_rows[1:]:
        size = min(len(prefix), len(row))
        shared: List[str] = []
        for index in range(size):
            if prefix[index].lower() != row[index].lower():
                break
            shared.append(prefix[index])
        prefix = shared
        if not prefix:
            break
    return prefix


def _build_pattern_keys(scope_old_groups: Sequence[Dict[str, str]]) -> Dict[str, List[str]]:
    by_as: Dict[str, Set[str]] = defaultdict(set)
    for row in scope_old_groups:
        as_value = str(row.get("AS") or "").strip().lower()
        group_name = str(row.get("name") or "").strip()
        if as_value and group_name:
            by_as[as_value].add(group_name)

    out: Dict[str, List[str]] = {}
    for as_value, names in by_as.items():
        keys: Set[str] = set()
        common_tokens = _common_prefix_tokens(sorted(names))
        if common_tokens:
            keys.add("-".join(common_tokens))
        for name in names:
            suffix = _legacy_suffix(name)
            if suffix:
                keys.add(suffix)
        out[as_value] = sorted((key for key in keys if key), key=lambda item: (-len(item), item.lower()))
    return out


def _field_kind(json_path: str) -> str:
    lower = str(json_path or "").lower()
    if ".regex" in lower:
        return "regex"
    if ".query" in lower:
        return "query"
    if ".definition" in lower:
        return "definition"
    if ".rawsql" in lower or ".sql" in lower:
        return "sql"
    if ".expr" in lower or ".expression" in lower:
        return "expression"
    if ".current" in lower:
        return "current"
    if ".options" in lower:
        return "options"
    if ".title" in lower:
        return "title"
    if ".description" in lower:
        return "description"
    if ".content" in lower:
        return "content"
    if ".text" in lower:
        return "text"
    return "string"


def _is_pattern_candidate(text: str, location_kind: str, field_kind: str) -> bool:
    if "$" in text or REGEX_META_RX.search(text):
        return True
    if field_kind in PATTERN_FIELD_KINDS:
        return True
    if location_kind == "variable":
        return True
    lower = text.lower()
    return any(marker in lower for marker in QUERY_FIELD_MARKERS)


def _is_group_relevant_context(json_path: str, location_kind: str, field_kind: str, variable_name: str = "") -> bool:
    lower_path = str(json_path or "").lower()
    lower_name = str(variable_name or "").lower()
    if any(marker in lower_path for marker in HOST_FILTER_MARKERS):
        return False
    if location_kind == "variable":
        if field_kind in {"query", "definition", "regex"}:
            return True
        if field_kind in {"current", "options"} and "group" in lower_name:
            return True
        return False
    if location_kind == "panel":
        return ".targets[" in lower_path and ".group.filter" in lower_path
    return False


def _build_dashboard_url(base_url: str, search_row: Dict[str, Any], dashboard_payload: Dict[str, Any], uid: str) -> str:
    meta = dashboard_payload.get("meta") or {}
    raw_path = str(meta.get("url") or search_row.get("url") or "").strip()
    if raw_path:
        if raw_path.startswith("http://") or raw_path.startswith("https://"):
            return raw_path
        return f"{base_url.rstrip('/')}{raw_path}"
    return f"{base_url.rstrip('/')}/d/{uid}"


def _build_panel_url(dashboard_url: str, panel_id: str) -> str:
    if not dashboard_url or not panel_id:
        return ""
    separator = "&" if "?" in dashboard_url else "?"
    return f"{dashboard_url}{separator}viewPanel={panel_id}"


def _dashboard_contexts(dashboard: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = {key: value for key, value in dashboard.items() if key not in {"panels", "templating"}}
    rows: List[Dict[str, Any]] = []
    for json_path, text in _iter_strings(root, path="dashboard"):
        rows.append(
            {
                "location_kind": "dashboard",
                "json_path": json_path,
                "text": text,
                "field_kind": _field_kind(json_path),
                "panel_id": "",
                "panel_title": "",
                "panel_type": "",
                "panel_url": "",
                "variable_name": "",
                "variable_type": "",
            }
        )
    return rows


def _variable_contexts(dashboard: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    templating = (dashboard.get("templating") or {}).get("list") or []
    for index, variable in enumerate(templating):
        base_path = f"dashboard.templating.list[{index}]"
        variable_name = str(variable.get("name") or "")
        variable_type = str(variable.get("type") or "")
        for json_path, text in _iter_strings(variable, path=base_path):
            rows.append(
                {
                    "location_kind": "variable",
                    "json_path": json_path,
                    "text": text,
                    "field_kind": _field_kind(json_path),
                    "panel_id": "",
                    "panel_title": "",
                    "panel_type": "",
                    "panel_url": "",
                    "variable_name": variable_name,
                    "variable_type": variable_type,
                }
            )
    return rows


def _panel_contexts(dashboard: Dict[str, Any], dashboard_url: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for panel, panel_path in _walk_panels(dashboard.get("panels") or []):
        panel_id = str(panel.get("id") or "")
        panel_title = str(panel.get("title") or "")
        panel_type = str(panel.get("type") or "")
        panel_url = _build_panel_url(dashboard_url, panel_id)
        for json_path, text in _iter_strings(panel, path=panel_path, exclude_keys={"panels"}):
            rows.append(
                {
                    "location_kind": "panel",
                    "json_path": json_path,
                    "text": text,
                    "field_kind": _field_kind(json_path),
                    "panel_id": panel_id,
                    "panel_title": panel_title,
                    "panel_type": panel_type,
                    "panel_url": panel_url,
                    "variable_name": "",
                    "variable_type": "",
                }
            )
    return rows


def _add_detail_row(counts: Dict[Tuple[str, ...], int], row: Dict[str, Any]) -> None:
    key = (
        str(row.get("AS") or ""),
        str(row.get("grafana_org_id") or ""),
        str(row.get("dashboard_uid") or ""),
        str(row.get("dashboard_title") or ""),
        str(row.get("folder_title") or ""),
        str(row.get("dashboard_url") or ""),
        str(row.get("panel_url") or ""),
        str(row.get("panel_id") or ""),
        str(row.get("panel_title") or ""),
        str(row.get("panel_type") or ""),
        str(row.get("variable_name") or ""),
        str(row.get("variable_type") or ""),
        str(row.get("location_kind") or ""),
        str(row.get("field_kind") or ""),
        str(row.get("reference_kind") or ""),
        str(row.get("match_type") or ""),
        str(row.get("matched_string") or ""),
        str(row.get("pattern_key") or ""),
        str(row.get("source_text") or ""),
        str(row.get("json_path") or ""),
    )
    counts[key] += 1


def _build_summary_rows(detail_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[str, str, str, str, str, str], Dict[str, Any]] = defaultdict(
        lambda: {
            "hits_total": 0,
            "exact_old": 0,
            "pattern_old": 0,
            "variable_hits": 0,
            "panel_hits": 0,
            "dashboard_hits": 0,
            "panels": set(),
            "variables": set(),
        }
    )

    for row in detail_rows:
        key = (
            str(row.get("AS") or ""),
            str(row.get("grafana_org_id") or ""),
            str(row.get("dashboard_uid") or ""),
            str(row.get("dashboard_title") or ""),
            str(row.get("folder_title") or ""),
            str(row.get("dashboard_url") or ""),
        )
        bucket = buckets[key]
        count = int(row.get("count") or 0)
        bucket["hits_total"] += count
        match_type = str(row.get("match_type") or "")
        if match_type == "OLD":
            bucket["exact_old"] += count
        elif match_type == "OLD_PATTERN":
            bucket["pattern_old"] += count

        location_kind = str(row.get("location_kind") or "")
        if location_kind == "variable":
            bucket["variable_hits"] += count
        elif location_kind == "panel":
            bucket["panel_hits"] += count
        else:
            bucket["dashboard_hits"] += count

        panel_label = str(row.get("panel_title") or row.get("panel_id") or "").strip()
        if panel_label:
            bucket["panels"].add(panel_label)
        variable_name = str(row.get("variable_name") or "").strip()
        if variable_name:
            bucket["variables"].add(variable_name)

    rows: List[Dict[str, Any]] = []
    for key in sorted(buckets.keys(), key=lambda item: (item[0].lower(), int(item[1] or 0), item[3].lower(), item[2])):
        bucket = buckets[key]
        rows.append(
            {
                "AS": key[0],
                "grafana_org_id": key[1],
                "dashboard_uid": key[2],
                "dashboard_title": key[3],
                "folder_title": key[4],
                "dashboard_url": key[5],
                "hits_total": bucket["hits_total"],
                "exact_old": bucket["exact_old"],
                "pattern_old": bucket["pattern_old"],
                "variable_hits": bucket["variable_hits"],
                "panel_hits": bucket["panel_hits"],
                "dashboard_hits": bucket["dashboard_hits"],
                "panels": join_sorted(bucket["panels"]),
                "variables": join_sorted(bucket["variables"]),
            }
        )
    return rows


def _mapping_row_priority(row: Dict[str, Any]) -> Tuple[int, int, int, str]:
    selected = str(row.get("selected") or "").strip().lower() in {"1", "y", "yes", "true", "x"}
    exists = str(row.get("target_exists") or "").strip().lower() == "yes"
    manual = str(row.get("manual_required") or "").strip().lower() == "yes"
    rank = int(row.get("candidate_rank") or 9999)
    return (
        0 if selected else 1,
        0 if exists else 1,
        0 if not manual else 1,
        rank,
    )


def _pick_mapping_by_old_group(mapping_rows: Sequence[Dict[str, Any]] | None) -> Dict[str, Dict[str, Any]]:
    by_old: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in mapping_rows or []:
        old_group = str(row.get("old_group") or "").strip()
        if old_group:
            by_old[old_group].append(dict(row))
    chosen: Dict[str, Dict[str, Any]] = {}
    for old_group, rows in by_old.items():
        rows.sort(key=lambda row: _mapping_row_priority(row))
        chosen[old_group] = rows[0]
    return chosen


def _suggest_value(source_text: str, old_group: str, new_group: str) -> str:
    text = str(source_text or "")
    if not old_group or not new_group or old_group not in text:
        return text
    return text.replace(old_group, new_group)


def _build_scoped_grafana_views(
    detail_rows: Sequence[Dict[str, Any]],
    mapping_rows: Sequence[Dict[str, Any]] | None,
) -> Dict[str, List[Dict[str, Any]]]:
    mapping_by_old = _pick_mapping_by_old_group(mapping_rows)
    enriched_rows: List[Dict[str, Any]] = []
    variable_rows: List[Dict[str, Any]] = []
    panel_rows: List[Dict[str, Any]] = []
    suggestion_rows: List[Dict[str, Any]] = []

    for row in detail_rows:
        out = dict(row)
        source_text = str(row.get("source_text") or "")
        match_type = str(row.get("match_type") or "")
        matched_string = str(row.get("matched_string") or "")

        chosen_mapping: Dict[str, Any] | None = None
        suggestion_status = ""
        manual_required = "yes"
        suggested_old_group = ""
        suggested_new_group = ""
        suggested_value = ""

        if match_type == "OLD":
            suggested_old_group = matched_string
            chosen_mapping = mapping_by_old.get(matched_string)
            if chosen_mapping:
                suggested_new_group = str(chosen_mapping.get("new_group") or "")
                suggested_value = _suggest_value(source_text, suggested_old_group, suggested_new_group)
                manual_required = str(chosen_mapping.get("manual_required") or "")
                suggestion_status = "exact_match" if suggested_new_group else "no_target"
            else:
                suggestion_status = "no_mapping"
        elif match_type == "OLD_PATTERN":
            matched_old_groups = sorted(old_group for old_group in mapping_by_old if old_group in source_text)
            if len(matched_old_groups) == 1:
                suggested_old_group = matched_old_groups[0]
                chosen_mapping = mapping_by_old.get(suggested_old_group)
                if chosen_mapping:
                    suggested_new_group = str(chosen_mapping.get("new_group") or "")
                    suggested_value = _suggest_value(source_text, suggested_old_group, suggested_new_group)
                suggestion_status = "manual_pattern_single"
            elif matched_old_groups:
                suggestion_status = "manual_pattern_ambiguous"
            else:
                suggestion_status = "manual_pattern_unresolved"
            manual_required = "yes"

        out.update(
            {
                "suggested_old_group": suggested_old_group,
                "suggested_new_group": suggested_new_group,
                "suggested_value": suggested_value,
                "suggestion_status": suggestion_status,
                "manual_required": manual_required,
            }
        )
        enriched_rows.append(out)

        location_kind = str(row.get("location_kind") or "")
        if location_kind == "variable":
            variable_rows.append(dict(out))
        if location_kind == "panel":
            panel_rows.append(dict(out))
        if suggestion_status:
            suggestion_rows.append(dict(out))

    return {
        "detail_rows": enriched_rows,
        "variable_rows": variable_rows,
        "panel_rows": panel_rows,
        "suggestion_rows": suggestion_rows,
    }


def _build_org_grafana_suggestions(
    detail_rows: Sequence[Dict[str, Any]],
    mapping_rows: Sequence[Dict[str, Any]] | None,
) -> List[Dict[str, Any]]:
    mapping_by_old = _pick_mapping_by_old_group(mapping_rows)
    suggestions: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, ...]] = set()
    for row in detail_rows:
        source_text = str(row.get("source_text") or "")
        if not source_text:
            continue
        for match in OLD_RX.finditer(source_text):
            old_group = match.group(0).strip()
            chosen_mapping = mapping_by_old.get(old_group)
            if not chosen_mapping:
                continue
            new_group = str(chosen_mapping.get("new_group") or "")
            suggestion_status = "exact_match" if new_group else "no_target"
            signature = (
                str(row.get("grafana_org_id") or ""),
                str(row.get("dashboard_uid") or ""),
                str(row.get("variable_name") or ""),
                str(row.get("panel_id") or ""),
                str(row.get("json_path") or ""),
                old_group,
                new_group,
            )
            if signature in seen:
                continue
            seen.add(signature)
            suggestions.append(
                {
                    "grafana_org_id": str(row.get("grafana_org_id") or ""),
                    "dashboard_uid": str(row.get("dashboard_uid") or ""),
                    "dashboard_title": str(row.get("dashboard_title") or ""),
                    "folder_title": str(row.get("folder_title") or ""),
                    "dashboard_url": str(row.get("dashboard_url") or ""),
                    "panel_url": str(row.get("panel_url") or ""),
                    "panel_id": str(row.get("panel_id") or ""),
                    "panel_title": str(row.get("panel_title") or ""),
                    "panel_type": str(row.get("panel_type") or ""),
                    "variable_name": str(row.get("variable_name") or ""),
                    "variable_type": str(row.get("variable_type") or ""),
                    "location_kind": str(row.get("location_kind") or ""),
                    "field_kind": str(row.get("field_kind") or ""),
                    "reference_kind": str(row.get("reference_kind") or ""),
                    "json_path": str(row.get("json_path") or ""),
                    "old_group": old_group,
                    "new_group": new_group,
                    "source_text": source_text,
                    "planned_value": _suggest_value(source_text, old_group, new_group),
                    "suggestion_status": suggestion_status,
                    "manual_required": str(chosen_mapping.get("manual_required") or ""),
                }
            )
    return suggestions


def collect_grafana_report(
    conn: config.GrafanaConnection,
    scope_pairs: Sequence[Tuple[str, int]],
    scope_old_groups: Sequence[Dict[str, str]],
    mapping_rows: Sequence[Dict[str, Any]] | None = None,
    log: Callable[[str], None] | None = None,
) -> Dict[str, List[Dict[str, Any]]]:
    exact_old: Dict[str, Set[str]] = defaultdict(set)
    for row in scope_old_groups:
        name = str(row.get("name") or "").strip()
        as_value = str(row.get("AS") or "").strip().lower()
        if name and as_value:
            exact_old[name].add(as_value)
    pattern_keys_by_as = _build_pattern_keys(scope_old_groups)

    _log(log, f"grafana: scope_pairs={[(as_value, int(org_id)) for as_value, org_id in scope_pairs]}")
    _log(log, f"grafana: scope_old_groups={len(scope_old_groups)} unique_old_names={len(exact_old)}")

    counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    seen_dashboards: Set[Tuple[str, int, str]] = set()

    for as_value, org_id in scope_pairs:
        as_key = str(as_value or "").strip().lower()
        org_id_int = int(org_id or 0)
        exact_old_count = sum(1 for values in exact_old.values() if as_key in values)
        old_group_sample = sorted(name for name, values in exact_old.items() if as_key in values)[:10]
        pattern_keys = pattern_keys_by_as.get(as_key) or []
        _log(
            log,
            f"grafana: start as={as_value} org_id={org_id_int} old_groups={exact_old_count} "
            f"sample={old_group_sample} pattern_keys={pattern_keys[:10]}",
        )
        api = GrafanaAPI(
            conn.base_url,
            conn.username,
            conn.password,
            org_id=org_id_int,
            timeout_sec=int(config.HTTP_TIMEOUT_SEC),
        )
        dashboards = api.list_dashboards()
        _log(log, f"grafana: as={as_value} org_id={org_id_int} dashboards={len(dashboards)} pattern_keys_count={len(pattern_keys)}")
        org_exact_hits = 0
        org_pattern_hits = 0

        for dashboard_index, dashboard_row in enumerate(dashboards, start=1):
            uid = str(dashboard_row.get("uid") or "").strip()
            if not uid:
                continue
            if (as_value, org_id_int, uid) in seen_dashboards:
                continue
            seen_dashboards.add((as_value, org_id_int, uid))
            if dashboard_index == 1 or dashboard_index % 50 == 0:
                _log(log, f"grafana: progress as={as_value} org_id={org_id_int} dashboard={dashboard_index}/{len(dashboards)} uid={uid}")

            try:
                dashboard_payload = api.get_dashboard_by_uid(uid)
            except RuntimeError as exc:
                _log(log, f"grafana: skip dashboard as={as_value} org_id={org_id_int} uid={uid}: {exc}")
                continue
            dashboard = dashboard_payload.get("dashboard") or dashboard_payload
            dashboard_title = str(dashboard.get("title") or dashboard_row.get("title") or "")
            folder_title = str(dashboard_row.get("folderTitle") or "")
            dashboard_url = _build_dashboard_url(conn.base_url, dashboard_row, dashboard_payload, uid)
            contexts = []
            contexts.extend(_dashboard_contexts(dashboard))
            contexts.extend(_variable_contexts(dashboard))
            contexts.extend(_panel_contexts(dashboard, dashboard_url))
            dashboard_exact_hits = 0
            dashboard_pattern_hits = 0

            for context in contexts:
                text = str(context.get("text") or "")
                if not text.strip():
                    continue
                location_kind = str(context.get("location_kind") or "")
                field_kind = str(context.get("field_kind") or "")
                json_path = str(context.get("json_path") or "")
                if not _is_group_relevant_context(json_path, location_kind, field_kind, str(context.get("variable_name") or "")):
                    continue
                text_lower = text.lower()

                for match in OLD_RX.finditer(text):
                    candidate = match.group(0).strip()
                    if as_key not in exact_old.get(candidate, set()):
                        continue
                    _add_detail_row(
                        counts,
                        {
                            "AS": as_value,
                            "grafana_org_id": str(int(org_id or 0)),
                            "dashboard_uid": uid,
                            "dashboard_title": dashboard_title,
                            "folder_title": folder_title,
                            "dashboard_url": dashboard_url,
                            "reference_kind": "direct_group_name",
                            "match_type": "OLD",
                            "matched_string": candidate,
                            "pattern_key": "",
                            "source_text": text,
                            **context,
                        },
                    )
                    dashboard_exact_hits += 1
                    org_exact_hits += 1

                if not _is_pattern_candidate(text, str(context.get("location_kind") or ""), str(context.get("field_kind") or "")):
                    continue
                if "bnk-" not in text_lower and "dom-" not in text_lower:
                    continue
                matched_pattern_keys = [key for key in pattern_keys if key.lower() in text_lower]
                if not matched_pattern_keys:
                    continue

                for pattern_key in matched_pattern_keys:
                    _add_detail_row(
                        counts,
                        {
                            "AS": as_value,
                            "grafana_org_id": str(int(org_id or 0)),
                            "dashboard_uid": uid,
                            "dashboard_title": dashboard_title,
                            "folder_title": folder_title,
                            "dashboard_url": dashboard_url,
                            "reference_kind": "pattern_or_regex",
                            "match_type": "OLD_PATTERN",
                            "matched_string": text,
                            "pattern_key": pattern_key,
                            "source_text": text,
                            **context,
                        },
                    )
                    dashboard_pattern_hits += 1
                    org_pattern_hits += 1

            if dashboard_exact_hits or dashboard_pattern_hits:
                _log(
                    log,
                    "grafana: hit "
                    f"as={as_value} org_id={org_id_int} uid={uid} title={dashboard_title!r} "
                    f"exact_old={dashboard_exact_hits} pattern_old={dashboard_pattern_hits}",
                )

        _log(
            log,
            f"grafana: completed as={as_value} org_id={org_id_int} exact_old_hits={org_exact_hits} pattern_old_hits={org_pattern_hits}",
        )

    detail_rows: List[Dict[str, Any]] = []
    for key, count in sorted(counts.items(), key=lambda item: item[0]):
        detail_rows.append(
            {
                "AS": key[0],
                "grafana_org_id": key[1],
                "dashboard_uid": key[2],
                "dashboard_title": key[3],
                "folder_title": key[4],
                "dashboard_url": key[5],
                "panel_url": key[6],
                "panel_id": key[7],
                "panel_title": key[8],
                "panel_type": key[9],
                "variable_name": key[10],
                "variable_type": key[11],
                "location_kind": key[12],
                "field_kind": key[13],
                "reference_kind": key[14],
                "match_type": key[15],
                "matched_string": key[16],
                "pattern_key": key[17],
                "source_text": key[18],
                "json_path": key[19],
                "count": count,
            }
        )

    scoped_views = _build_scoped_grafana_views(detail_rows, mapping_rows)
    summary_rows = _build_summary_rows(scoped_views["detail_rows"])
    _log(
        log,
        "grafana: summary "
        f"dashboards={len(summary_rows)} detail_rows={len(scoped_views['detail_rows'])} "
        f"variables={len(scoped_views['variable_rows'])} panels={len(scoped_views['panel_rows'])} "
        f"suggestions={len(scoped_views['suggestion_rows'])}",
    )
    return {
        "summary_rows": summary_rows,
        "detail_rows": scoped_views["detail_rows"],
        "variable_rows": scoped_views["variable_rows"],
        "panel_rows": scoped_views["panel_rows"],
        "suggestion_rows": scoped_views["suggestion_rows"],
    }


def _is_zabbix_datasource_row(row: Dict[str, Any]) -> bool:
    allowed_types = {
        str(item or "").strip().lower()
        for item in getattr(config, "GRAFANA_ZABBIX_DATASOURCE_TYPES", ())
        if str(item or "").strip()
    }
    ds_type = str(row.get("type") or "").strip().lower()
    plugin_id = str(row.get("pluginId") or "").strip().lower()
    if ds_type in allowed_types:
        return True
    if plugin_id in allowed_types:
        return True
    return False


def _datasource_identity(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(row.get("uid") or "").strip(),
        str(row.get("id") or "").strip(),
        str(row.get("name") or "").strip(),
    )


def _datasource_label(row: Dict[str, Any]) -> str:
    name = str(row.get("name") or "").strip()
    uid = str(row.get("uid") or "").strip()
    if name and uid:
        return f"{name} [{uid}]"
    return name or uid or str(row.get("id") or "").strip()


def _build_zabbix_datasource_index(rows: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    zabbix_rows = [row for row in rows if _is_zabbix_datasource_row(row)]
    token_index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in zabbix_rows:
        tokens = {
            str(row.get("id") or "").strip(),
            str(row.get("uid") or "").strip(),
            str(row.get("name") or "").strip(),
            str(row.get("type") or "").strip(),
            str(row.get("typeName") or "").strip(),
            str(row.get("pluginId") or "").strip(),
        }
        for token in tokens:
            if not token:
                continue
            token_index[token.lower()].append(row)
    return zabbix_rows, token_index


def _extract_template_vars(text: str) -> List[str]:
    return [match.group(1) for match in re.finditer(r"\$\{?([A-Za-z0-9_:-]+)\}?", str(text or ""))]


def _unique_datasource_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[Tuple[str, str, str]] = set()
    out: List[Dict[str, Any]] = []
    for row in rows:
        identity = _datasource_identity(row)
        if identity in seen:
            continue
        seen.add(identity)
        out.append(row)
    return out


def _match_datasource_tokens(tokens: Sequence[str], token_index: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    for token in tokens:
        item = str(token or "").strip()
        if not item:
            continue
        matches.extend(token_index.get(item.lower(), []))
    return _unique_datasource_rows(matches)


def _build_datasource_variable_index(
    dashboard: Dict[str, Any],
    token_index: Dict[str, List[Dict[str, Any]]],
    zabbix_rows: Sequence[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    templating = (dashboard.get("templating") or {}).get("list") or []
    for variable in templating:
        if str(variable.get("type") or "") != "datasource":
            continue
        name = str(variable.get("name") or "").strip()
        if not name:
            continue
        query = str(variable.get("query") or "").strip()
        regex = str(variable.get("regex") or "").strip()
        current = variable.get("current") or {}
        tokens = [
            query,
            regex,
            str(current.get("text") or "").strip(),
            str(current.get("value") or "").strip(),
        ]
        matched = _match_datasource_tokens(tokens, token_index)
        text = " ".join(token.lower() for token in tokens if token)
        if not matched and "zabbix" in text:
            matched = list(zabbix_rows)
        if matched:
            out[name] = _unique_datasource_rows(matched)
    return out


def _resolve_datasource_value(
    value: Any,
    token_index: Dict[str, List[Dict[str, Any]]],
    datasource_vars: Dict[str, List[Dict[str, Any]]],
) -> Tuple[List[Dict[str, Any]], str, str]:
    raw_tokens: List[str] = []
    ref_kind = ""

    if isinstance(value, str):
        text = str(value).strip()
        raw_tokens.append(text)
        variable_matches: List[Dict[str, Any]] = []
        for variable_name in _extract_template_vars(text):
            variable_matches.extend(datasource_vars.get(variable_name, []))
        if variable_matches:
            return _unique_datasource_rows(variable_matches), text, "datasource_variable"
        return _match_datasource_tokens(raw_tokens, token_index), text, "datasource"

    if isinstance(value, dict):
        ref_kind = "datasource"
        for key in ("uid", "name", "type", "typeName", "pluginId"):
            token = str(value.get(key) or "").strip()
            if token:
                raw_tokens.append(token)
        variable_matches = []
        for token in raw_tokens:
            for variable_name in _extract_template_vars(token):
                variable_matches.extend(datasource_vars.get(variable_name, []))
        if variable_matches:
            return _unique_datasource_rows(variable_matches), json.dumps(value, ensure_ascii=False), "datasource_variable"
        return _match_datasource_tokens(raw_tokens, token_index), json.dumps(value, ensure_ascii=False), ref_kind

    text = str(value or "").strip()
    if not text:
        return [], "", ""
    return [], text, "datasource"


def _collect_datasource_references(
    node: Any,
    token_index: Dict[str, List[Dict[str, Any]]],
    datasource_vars: Dict[str, List[Dict[str, Any]]],
    path: str = "",
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            child_path = f"{path}.{key}" if path else str(key)
            key_lower = str(key).lower()
            if key_lower == "datasource" or key_lower in {"datasourceuid", "datasource_uid"}:
                matched_rows, raw_value, ref_kind = _resolve_datasource_value(value, token_index, datasource_vars)
                if matched_rows:
                    rows.append(
                        {
                            "json_path": child_path,
                            "raw_value": raw_value,
                            "reference_kind": ref_kind,
                            "datasources": _unique_datasource_rows(matched_rows),
                        }
                    )
            rows.extend(_collect_datasource_references(value, token_index, datasource_vars, child_path))
        return rows
    if isinstance(node, list):
        for index, item in enumerate(node):
            child_path = f"{path}[{index}]" if path else f"[{index}]"
            rows.extend(_collect_datasource_references(item, token_index, datasource_vars, child_path))
    return rows


def _format_datasource_labels(rows: Sequence[Dict[str, Any]]) -> str:
    return join_sorted(_datasource_label(row) for row in rows)


def _truncate(text: str, limit: int = 400) -> str:
    item = str(text or "")
    if len(item) <= limit:
        return item
    return item[: limit - 3] + "..."


def _is_relevant_org_text(text: str, json_path: str, field_kind: str, variable_name: str = "") -> bool:
    lower_text = str(text or "").lower()
    lower_path = str(json_path or "").lower()
    if not lower_text.strip():
        return False
    if any(marker in lower_path for marker in HOST_FILTER_MARKERS):
        return False
    location_kind = "variable" if ".templating.list[" in lower_path else ("panel" if ".panels[" in lower_path else "dashboard")
    if not _is_group_relevant_context(json_path, location_kind, field_kind, variable_name):
        return False
    if OLD_RX.search(text) or NEW_RX.search(text):
        return True
    if "$" in lower_text:
        return True
    if REGEX_META_RX.search(text):
        return True
    if field_kind in PATTERN_FIELD_KINDS:
        return True
    return any(marker in lower_path for marker in (".group", ".groups", ".filter", ".filters"))


def _classify_org_text(text: str, json_path: str, field_kind: str) -> str:
    hints: List[str] = []
    lower_path = str(json_path or "").lower()
    if OLD_RX.search(text):
        hints.append("legacy_group_name")
    if NEW_RX.search(text):
        hints.append("new_group_name")
    if "$" in str(text or ""):
        hints.append("template_variable")
    if REGEX_META_RX.search(text):
        hints.append("regex_or_pattern")
    if field_kind in PATTERN_FIELD_KINDS:
        hints.append(f"{field_kind}_field")
    if ".datasource" in lower_path:
        hints.append("datasource_ref")
    if any(token in lower_path for token in (".group", ".groups")):
        hints.append("group_selector_field")
    if any(token in lower_path for token in (".host", ".hosts")):
        hints.append("host_selector_field")
    if any(token in lower_path for token in (".item", ".application", ".filter", ".search")):
        hints.append("selector_field")
    return join_sorted(hints)


def _build_org_summary_rows(
    org_id: int,
    total_dashboards: int,
    datasources: Sequence[Dict[str, Any]],
    dashboard_rows: Sequence[Dict[str, Any]],
    variable_rows: Sequence[Dict[str, Any]],
    panel_rows: Sequence[Dict[str, Any]],
    detail_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return [
        {
            "grafana_org_id": str(org_id),
            "zabbix_datasources": len(datasources),
            "zabbix_datasource_names": _format_datasource_labels(datasources),
            "dashboards_total": int(total_dashboards),
            "dashboards_with_zabbix": len(dashboard_rows),
            "variables_with_zabbix": len(variable_rows),
            "panels_with_zabbix": len(panel_rows),
            "detail_rows": len(detail_rows),
        }
    ]


def collect_grafana_org_report(
    conn: config.GrafanaConnection,
    org_ids: Sequence[int],
    mapping_rows: Sequence[Dict[str, Any]] | None = None,
    log: Callable[[str], None] | None = None,
) -> Dict[str, Any]:
    org_summary_rows: List[Dict[str, Any]] = []
    datasource_rows: List[Dict[str, Any]] = []
    dashboard_rows: List[Dict[str, Any]] = []
    variable_rows: List[Dict[str, Any]] = []
    panel_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []
    error_rows: List[Dict[str, Any]] = []

    normalized_org_ids = [int(value) for value in org_ids]
    _log(log, f"grafana-org: start org_ids={normalized_org_ids}")

    for org_id in normalized_org_ids:
        api = GrafanaAPI(
            conn.base_url,
            conn.username,
            conn.password,
            org_id=org_id,
            timeout_sec=int(config.HTTP_TIMEOUT_SEC),
        )
        all_datasources = api.list_datasources()
        zabbix_datasources, ds_index = _build_zabbix_datasource_index(all_datasources)
        _log(
            log,
            f"grafana-org: org_id={org_id} datasources_total={len(all_datasources)} zabbix_datasources={len(zabbix_datasources)} sample={[_datasource_label(row) for row in zabbix_datasources[:10]]}",
        )

        for row in zabbix_datasources:
            datasource_rows.append(
                {
                    "grafana_org_id": str(org_id),
                    "datasource_id": str(row.get("id") or ""),
                    "datasource_uid": str(row.get("uid") or ""),
                    "datasource_name": str(row.get("name") or ""),
                    "datasource_type": str(row.get("type") or ""),
                    "access": str(row.get("access") or ""),
                    "url": str(row.get("url") or ""),
                    "is_default": str(row.get("isDefault") or ""),
                    "read_only": str(row.get("readOnly") or ""),
                }
            )

        dashboards = api.list_dashboards()
        _log(log, f"grafana-org: org_id={org_id} dashboards_total={len(dashboards)}")

        org_dashboard_rows: List[Dict[str, Any]] = []
        org_variable_rows: List[Dict[str, Any]] = []
        org_panel_rows: List[Dict[str, Any]] = []
        org_detail_rows: List[Dict[str, Any]] = []
        org_error_rows: List[Dict[str, Any]] = []

        for dashboard_index, dashboard_row in enumerate(dashboards, start=1):
            uid = str(dashboard_row.get("uid") or "").strip()
            if not uid:
                continue
            if dashboard_index == 1 or dashboard_index % 50 == 0:
                _log(log, f"grafana-org: org_id={org_id} dashboard={dashboard_index}/{len(dashboards)} uid={uid}")

            try:
                dashboard_payload = api.get_dashboard_by_uid(uid)
            except RuntimeError as exc:
                error_message = str(exc)
                dashboard_url = _build_dashboard_url(conn.base_url, dashboard_row, {}, uid)
                org_error_rows.append(
                    {
                        "grafana_org_id": str(org_id),
                        "dashboard_uid": uid,
                        "dashboard_title": str(dashboard_row.get("title") or ""),
                        "folder_title": str(dashboard_row.get("folderTitle") or ""),
                        "dashboard_url": dashboard_url,
                        "status": "dashboard_fetch_failed",
                        "message": error_message,
                    }
                )
                _log(log, f"grafana-org: skip dashboard org_id={org_id} uid={uid}: {error_message}")
                continue
            dashboard = dashboard_payload.get("dashboard") or dashboard_payload
            dashboard_title = str(dashboard.get("title") or dashboard_row.get("title") or "")
            folder_title = str(dashboard_row.get("folderTitle") or "")
            dashboard_url = _build_dashboard_url(conn.base_url, dashboard_row, dashboard_payload, uid)
            datasource_vars = _build_datasource_variable_index(dashboard, ds_index, zabbix_datasources)

            dashboard_root = {key: value for key, value in dashboard.items() if key not in {"panels", "templating"}}
            dashboard_refs = _collect_datasource_references(dashboard_root, ds_index, datasource_vars, path="dashboard")
            dashboard_context_rows: List[Dict[str, Any]] = []
            if dashboard_refs:
                context_datasources = _unique_datasource_rows(
                    [row for ref in dashboard_refs for row in ref.get("datasources") or []]
                )
                for context in _dashboard_contexts(dashboard):
                    text = str(context.get("text") or "")
                    if not _is_relevant_org_text(text, str(context.get("json_path") or ""), str(context.get("field_kind") or "")):
                        continue
                    dashboard_context_rows.append(
                        {
                            "grafana_org_id": str(org_id),
                            "dashboard_uid": uid,
                            "dashboard_title": dashboard_title,
                            "folder_title": folder_title,
                            "dashboard_url": dashboard_url,
                            "panel_url": "",
                            "panel_id": "",
                            "panel_title": "",
                            "panel_type": "",
                            "variable_name": "",
                            "variable_type": "",
                            "location_kind": "dashboard",
                            "field_kind": str(context.get("field_kind") or ""),
                            "reference_kind": "dashboard_scope",
                            "hint_kinds": _classify_org_text(text, str(context.get("json_path") or ""), str(context.get("field_kind") or "")),
                            "datasource_names": _format_datasource_labels(context_datasources),
                            "datasource_paths": join_sorted(ref.get("json_path") or "" for ref in dashboard_refs),
                            "source_text": _truncate(text),
                            "json_path": str(context.get("json_path") or ""),
                        }
                    )

            dashboard_variable_rows: List[Dict[str, Any]] = []
            templating = (dashboard.get("templating") or {}).get("list") or []
            for index, variable in enumerate(templating):
                base_path = f"dashboard.templating.list[{index}]"
                variable_name = str(variable.get("name") or "")
                variable_type = str(variable.get("type") or "")
                variable_refs = _collect_datasource_references(variable, ds_index, datasource_vars, path=base_path)
                if variable_type == "datasource" and variable_name in datasource_vars and not variable_refs:
                    variable_refs = [
                        {
                            "json_path": f"{base_path}.query",
                            "raw_value": str(variable.get("query") or ""),
                            "reference_kind": "datasource_variable",
                            "datasources": datasource_vars[variable_name],
                        }
                    ]
                if not variable_refs:
                    continue
                variable_datasources = _unique_datasource_rows([row for ref in variable_refs for row in ref.get("datasources") or []])
                strings = _iter_strings(variable, path=base_path)
                relevant_count = 0
                for json_path, text in strings:
                    field_kind = _field_kind(json_path)
                    if not _is_relevant_org_text(text, json_path, field_kind, variable_name):
                        continue
                    relevant_count += 1
                    dashboard_variable_rows.append(
                        {
                            "grafana_org_id": str(org_id),
                            "dashboard_uid": uid,
                            "dashboard_title": dashboard_title,
                            "folder_title": folder_title,
                            "dashboard_url": dashboard_url,
                            "panel_url": "",
                            "panel_id": "",
                            "panel_title": "",
                            "panel_type": "",
                            "variable_name": variable_name,
                            "variable_type": variable_type,
                            "location_kind": "variable",
                            "field_kind": field_kind,
                            "reference_kind": join_sorted(ref.get("reference_kind") or "" for ref in variable_refs),
                            "hint_kinds": _classify_org_text(text, json_path, field_kind),
                            "datasource_names": _format_datasource_labels(variable_datasources),
                            "datasource_paths": join_sorted(ref.get("json_path") or "" for ref in variable_refs),
                            "source_text": _truncate(text),
                            "json_path": json_path,
                        }
                    )
                org_variable_rows.append(
                    {
                        "grafana_org_id": str(org_id),
                        "dashboard_uid": uid,
                        "dashboard_title": dashboard_title,
                        "folder_title": folder_title,
                        "dashboard_url": dashboard_url,
                        "variable_name": variable_name,
                        "variable_type": variable_type,
                        "datasource_names": _format_datasource_labels(variable_datasources),
                        "datasource_paths": join_sorted(ref.get("json_path") or "" for ref in variable_refs),
                        "query": _truncate(str(variable.get("query") or "")),
                        "regex": _truncate(str(variable.get("regex") or "")),
                        "definition": _truncate(str(variable.get("definition") or "")),
                        "refresh": str(variable.get("refresh") or ""),
                        "hide": str(variable.get("hide") or ""),
                        "detail_rows": relevant_count,
                    }
                )

            dashboard_panel_rows: List[Dict[str, Any]] = []
            for panel, panel_path in _walk_panels(dashboard.get("panels") or []):
                panel_id = str(panel.get("id") or "")
                panel_title = str(panel.get("title") or "")
                panel_type = str(panel.get("type") or "")
                panel_url = _build_panel_url(dashboard_url, panel_id)
                panel_refs = _collect_datasource_references(panel, ds_index, datasource_vars, path=panel_path)
                if not panel_refs:
                    continue
                panel_datasources = _unique_datasource_rows([row for ref in panel_refs for row in ref.get("datasources") or []])
                target_count = len(panel.get("targets") or [])
                zabbix_target_count = 0
                for target_index, target in enumerate(panel.get("targets") or []):
                    target_refs = _collect_datasource_references(target, ds_index, datasource_vars, path=f"{panel_path}.targets[{target_index}]")
                    if target_refs:
                        zabbix_target_count += 1
                relevant_count = 0
                for json_path, text in _iter_strings(panel, path=panel_path, exclude_keys={"panels"}):
                    field_kind = _field_kind(json_path)
                    if not _is_relevant_org_text(text, json_path, field_kind):
                        continue
                    relevant_count += 1
                    org_detail_rows.append(
                        {
                            "grafana_org_id": str(org_id),
                            "dashboard_uid": uid,
                            "dashboard_title": dashboard_title,
                            "folder_title": folder_title,
                            "dashboard_url": dashboard_url,
                            "panel_url": panel_url,
                            "panel_id": panel_id,
                            "panel_title": panel_title,
                            "panel_type": panel_type,
                            "variable_name": "",
                            "variable_type": "",
                            "location_kind": "panel",
                            "field_kind": field_kind,
                            "reference_kind": join_sorted(ref.get("reference_kind") or "" for ref in panel_refs),
                            "hint_kinds": _classify_org_text(text, json_path, field_kind),
                            "datasource_names": _format_datasource_labels(panel_datasources),
                            "datasource_paths": join_sorted(ref.get("json_path") or "" for ref in panel_refs),
                            "source_text": _truncate(text),
                            "json_path": json_path,
                        }
                    )
                dashboard_panel_rows.append(
                    {
                        "grafana_org_id": str(org_id),
                        "dashboard_uid": uid,
                        "dashboard_title": dashboard_title,
                        "folder_title": folder_title,
                        "dashboard_url": dashboard_url,
                        "panel_url": panel_url,
                        "panel_id": panel_id,
                        "panel_title": panel_title,
                        "panel_type": panel_type,
                        "datasource_names": _format_datasource_labels(panel_datasources),
                        "datasource_paths": join_sorted(ref.get("json_path") or "" for ref in panel_refs),
                        "targets_total": target_count,
                        "targets_zabbix": zabbix_target_count,
                        "detail_rows": relevant_count,
                    }
                )

            org_detail_rows.extend(dashboard_context_rows)
            org_detail_rows.extend(dashboard_variable_rows)
            org_panel_rows.extend(dashboard_panel_rows)

            if dashboard_refs or dashboard_variable_rows or dashboard_panel_rows:
                org_dashboard_rows.append(
                    {
                        "grafana_org_id": str(org_id),
                        "dashboard_uid": uid,
                        "dashboard_title": dashboard_title,
                        "folder_title": folder_title,
                        "dashboard_url": dashboard_url,
                        "dashboard_datasources": _format_datasource_labels(
                            _unique_datasource_rows([row for ref in dashboard_refs for row in ref.get("datasources") or []])
                        ),
                        "dashboard_datasource_paths": join_sorted(ref.get("json_path") or "" for ref in dashboard_refs),
                        "zabbix_variable_count": sum(1 for row in org_variable_rows if row["dashboard_uid"] == uid and row["grafana_org_id"] == str(org_id)),
                        "zabbix_panel_count": len(dashboard_panel_rows),
                        "detail_rows": sum(1 for row in org_detail_rows if row["dashboard_uid"] == uid and row["grafana_org_id"] == str(org_id)),
                    }
                )
                _log(
                    log,
                    f"grafana-org: hit org_id={org_id} uid={uid} title={dashboard_title!r} variables={org_dashboard_rows[-1]['zabbix_variable_count']} panels={len(dashboard_panel_rows)} details={org_dashboard_rows[-1]['detail_rows']}",
                )

        org_summary_rows.extend(
            _build_org_summary_rows(
                org_id,
                len(dashboards),
                zabbix_datasources,
                org_dashboard_rows,
                org_variable_rows,
                org_panel_rows,
                org_detail_rows,
            )
        )
        dashboard_rows.extend(org_dashboard_rows)
        variable_rows.extend(org_variable_rows)
        panel_rows.extend(org_panel_rows)
        detail_rows.extend(org_detail_rows)
        error_rows.extend(org_error_rows)
        _log(
            log,
            f"grafana-org: completed org_id={org_id} dashboards_with_zabbix={len(org_dashboard_rows)} variables={len(org_variable_rows)} panels={len(org_panel_rows)} details={len(org_detail_rows)} errors={len(org_error_rows)}",
        )

    summary = {
        "grafana_org_ids": normalized_org_ids,
        "org_count": len(normalized_org_ids),
        "datasource_rows": len(datasource_rows),
        "dashboard_rows": len(dashboard_rows),
        "variable_rows": len(variable_rows),
        "panel_rows": len(panel_rows),
        "detail_rows": len(detail_rows),
        "error_rows": len(error_rows),
    }
    suggestion_rows = _build_org_grafana_suggestions(detail_rows, mapping_rows)
    summary["suggestion_rows"] = len(suggestion_rows)
    _log(log, f"grafana-org: summary={summary}")
    return {
        "summary": summary,
        "org_summary": org_summary_rows,
        "datasources": datasource_rows,
        "dashboards": dashboard_rows,
        "variables": variable_rows,
        "panels": panel_rows,
        "details": detail_rows,
        "suggestions": suggestion_rows,
        "errors": error_rows,
    }
