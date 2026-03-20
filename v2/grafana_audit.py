from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple

import config
from api_clients import GrafanaAPI
from common import join_sorted, normalize_values


OLD_RX = re.compile(r"\b(?:BNK|DOM)-[A-Za-z0-9_.:-]+(?:-[A-Za-z0-9_.:-]+)*\b")
NEW_RX = re.compile(r"\b(?:BNK|DOM)/(?:[A-Za-z0-9_.:-]+(?:/[A-Za-z0-9_.:-]+)*)\b")
REGEX_META_RX = re.compile(r"[\\^$*+?()\[\]{}|]")
QUERY_FIELD_MARKERS = (".query", ".definition", ".expr", ".expression", ".rawsql", ".sql", ".regex")
PATTERN_FIELD_KINDS = {"query", "definition", "expression", "sql", "regex", "current", "options"}


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


def _pattern_prefixes(scope_as: Sequence[str]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for as_value in normalize_values(scope_as):
        lower = as_value.lower()
        out[as_value] = [
            f"bnk-{lower}",
            f"dom-{lower}",
            f"bnk/as/{lower}",
            f"dom/as/{lower}",
        ]
    return out


def _infer_group_scope_as(group_name: str, scope_as: Sequence[str]) -> Set[str]:
    text = str(group_name or "").strip().lower()
    hits: Set[str] = set()
    for as_value in normalize_values(scope_as):
        lower = as_value.lower()
        if (
            text.startswith(f"bnk-{lower}")
            or text.startswith(f"dom-{lower}")
            or text == f"bnk/as/{lower}"
            or text == f"dom/as/{lower}"
            or text.startswith(f"bnk/as/{lower}/")
            or text.startswith(f"dom/as/{lower}/")
        ):
            hits.add(as_value)
    return hits


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
        str(row.get("source_text") or ""),
        str(row.get("json_path") or ""),
    )
    counts[key] += 1


def _build_summary_rows(detail_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = defaultdict(
        lambda: {
            "hits_total": 0,
            "exact_old": 0,
            "exact_new": 0,
            "pattern_old": 0,
            "pattern_new": 0,
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
        elif match_type == "NEW":
            bucket["exact_new"] += count
        elif match_type == "OLD_PATTERN":
            bucket["pattern_old"] += count
        elif match_type == "NEW_PATTERN":
            bucket["pattern_new"] += count

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
    for key in sorted(buckets.keys(), key=lambda item: (item[0].lower(), item[2].lower(), item[1])):
        bucket = buckets[key]
        rows.append(
            {
                "AS": key[0],
                "dashboard_uid": key[1],
                "dashboard_title": key[2],
                "folder_title": key[3],
                "dashboard_url": key[4],
                "hits_total": bucket["hits_total"],
                "exact_old": bucket["exact_old"],
                "exact_new": bucket["exact_new"],
                "pattern_old": bucket["pattern_old"],
                "pattern_new": bucket["pattern_new"],
                "variable_hits": bucket["variable_hits"],
                "panel_hits": bucket["panel_hits"],
                "dashboard_hits": bucket["dashboard_hits"],
                "panels": join_sorted(bucket["panels"]),
                "variables": join_sorted(bucket["variables"]),
            }
        )
    return rows


def collect_grafana_report(
    conn: config.GrafanaConnection,
    scope_as: Sequence[str],
    inventory_hostgroups: Sequence[Dict[str, str]],
) -> Dict[str, List[Dict[str, Any]]]:
    exact_old: Dict[str, Set[str]] = defaultdict(set)
    exact_new: Dict[str, Set[str]] = defaultdict(set)
    for row in inventory_hostgroups:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        matched_as = _infer_group_scope_as(name, scope_as)
        for as_value in matched_as:
            kind = str(row.get("kind") or "").upper()
            if kind == "OLD":
                exact_old[name].add(as_value)
            elif kind == "NEW":
                exact_new[name].add(as_value)

    api = GrafanaAPI(
        conn.base_url,
        conn.username,
        conn.password,
        timeout_sec=int(config.HTTP_TIMEOUT_SEC),
    )
    dashboards = api.list_dashboards()
    prefixes = _pattern_prefixes(scope_as)
    counts: Dict[Tuple[str, ...], int] = defaultdict(int)

    for dashboard_row in dashboards:
        uid = str(dashboard_row.get("uid") or "").strip()
        if not uid:
            continue

        dashboard_payload = api.get_dashboard_by_uid(uid)
        dashboard = dashboard_payload.get("dashboard") or dashboard_payload
        dashboard_title = str(dashboard.get("title") or dashboard_row.get("title") or "")
        folder_title = str(dashboard_row.get("folderTitle") or "")
        dashboard_url = _build_dashboard_url(conn.base_url, dashboard_row, dashboard_payload, uid)
        contexts = []
        contexts.extend(_dashboard_contexts(dashboard))
        contexts.extend(_variable_contexts(dashboard))
        contexts.extend(_panel_contexts(dashboard, dashboard_url))

        for context in contexts:
            text = str(context.get("text") or "")
            if not text.strip():
                continue
            text_lower = text.lower()
            direct_hit = False

            for match in OLD_RX.finditer(text):
                candidate = match.group(0).strip()
                matched_as = exact_old.get(candidate, set())
                if not matched_as:
                    continue
                direct_hit = True
                for as_value in matched_as:
                    _add_detail_row(
                        counts,
                        {
                            "AS": as_value,
                            "dashboard_uid": uid,
                            "dashboard_title": dashboard_title,
                            "folder_title": folder_title,
                            "dashboard_url": dashboard_url,
                            "reference_kind": "direct_group_name",
                            "match_type": "OLD",
                            "matched_string": candidate,
                            "source_text": text,
                            **context,
                        },
                    )

            for match in NEW_RX.finditer(text):
                candidate = match.group(0).strip()
                matched_as = exact_new.get(candidate, set())
                if not matched_as:
                    continue
                direct_hit = True
                for as_value in matched_as:
                    _add_detail_row(
                        counts,
                        {
                            "AS": as_value,
                            "dashboard_uid": uid,
                            "dashboard_title": dashboard_title,
                            "folder_title": folder_title,
                            "dashboard_url": dashboard_url,
                            "reference_kind": "direct_group_name",
                            "match_type": "NEW",
                            "matched_string": candidate,
                            "source_text": text,
                            **context,
                        },
                    )

            if not _is_pattern_candidate(text, str(context.get("location_kind") or ""), str(context.get("field_kind") or "")):
                continue

            for as_value, as_prefixes in prefixes.items():
                if not any(prefix in text_lower for prefix in as_prefixes):
                    continue

                if "bnk-" in text_lower or "dom-" in text_lower:
                    _add_detail_row(
                        counts,
                        {
                            "AS": as_value,
                            "dashboard_uid": uid,
                            "dashboard_title": dashboard_title,
                            "folder_title": folder_title,
                            "dashboard_url": dashboard_url,
                            "reference_kind": "pattern_or_regex" if direct_hit else "scoped_string",
                            "match_type": "OLD_PATTERN",
                            "matched_string": text,
                            "source_text": text,
                            **context,
                        },
                    )

                if "bnk/" in text_lower or "dom/" in text_lower:
                    _add_detail_row(
                        counts,
                        {
                            "AS": as_value,
                            "dashboard_uid": uid,
                            "dashboard_title": dashboard_title,
                            "folder_title": folder_title,
                            "dashboard_url": dashboard_url,
                            "reference_kind": "pattern_or_regex" if direct_hit else "scoped_string",
                            "match_type": "NEW_PATTERN",
                            "matched_string": text,
                            "source_text": text,
                            **context,
                        },
                    )

    detail_rows: List[Dict[str, Any]] = []
    for key, count in sorted(counts.items(), key=lambda item: item[0]):
        detail_rows.append(
            {
                "AS": key[0],
                "dashboard_uid": key[1],
                "dashboard_title": key[2],
                "folder_title": key[3],
                "dashboard_url": key[4],
                "panel_url": key[5],
                "panel_id": key[6],
                "panel_title": key[7],
                "panel_type": key[8],
                "variable_name": key[9],
                "variable_type": key[10],
                "location_kind": key[11],
                "field_kind": key[12],
                "reference_kind": key[13],
                "match_type": key[14],
                "matched_string": key[15],
                "source_text": key[16],
                "json_path": key[17],
                "count": count,
            }
        )

    return {
        "summary_rows": _build_summary_rows(detail_rows),
        "detail_rows": detail_rows,
    }
