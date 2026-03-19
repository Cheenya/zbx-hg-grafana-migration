from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple

from api_clients import GrafanaAPI
from config import CONFIG, GrafanaConnection

from .common import normalize_values


OLD_RX = re.compile(r"(BNK|DOM)-[^\"\\]+")
NEW_RX = re.compile(r"(BNK|DOM)/[^\"\\]+")
REGEX_META_RX = re.compile(r"[\\^$.*+?()\\[\\]{}|]")


def _iter_strings(node: Any, path: str = "") -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if isinstance(node, str):
        out.append((path, node))
        return out
    if isinstance(node, list):
        for index, item in enumerate(node):
            child_path = f"{path}[{index}]"
            out.extend(_iter_strings(item, child_path))
        return out
    if isinstance(node, dict):
        for key, value in node.items():
            child_path = f"{path}.{key}" if path else str(key)
            out.extend(_iter_strings(value, child_path))
        return out
    return out


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


def collect_grafana_rows(
    conn: GrafanaConnection,
    scope_as: Sequence[str],
    inventory_hostgroups: Sequence[Dict[str, str]],
) -> List[Dict[str, Any]]:
    exact_old: Dict[str, Set[str]] = defaultdict(set)
    exact_new: Dict[str, Set[str]] = defaultdict(set)
    for row in inventory_hostgroups:
        name = str(row.get("name") or "")
        if not name:
            continue
        for as_value in normalize_values(scope_as):
            if row.get("kind") == "OLD":
                exact_old[name].add(as_value)
            elif row.get("kind") == "NEW":
                exact_new[name].add(as_value)

    api = GrafanaAPI(
        conn.base_url,
        conn.username,
        conn.password,
        conn.token,
        timeout_sec=int(CONFIG.runtime.http_timeout_sec),
    )
    dashboards = api.list_dashboards()
    prefixes = _pattern_prefixes(scope_as)
    counts: Dict[Tuple[str, str, str, str, str, str], int] = defaultdict(int)

    for dashboard in dashboards:
        uid = str(dashboard.get("uid") or "")
        if not uid:
            continue
        title = str(dashboard.get("title") or "")
        dashboard_json = api.get_dashboard_by_uid(uid)
        for json_path, text in _iter_strings(dashboard_json):
            for match in OLD_RX.finditer(text):
                candidate = match.group(0).strip()
                for as_value in exact_old.get(candidate, set()):
                    counts[(as_value, uid, title, "OLD", candidate, json_path)] += 1

            for match in NEW_RX.finditer(text):
                candidate = match.group(0).strip()
                for as_value in exact_new.get(candidate, set()):
                    counts[(as_value, uid, title, "NEW", candidate, json_path)] += 1

            text_lower = text.lower()
            if REGEX_META_RX.search(text_lower) is None and "$" not in text_lower:
                continue
            for as_value, as_prefixes in prefixes.items():
                if not any(prefix in text_lower for prefix in as_prefixes):
                    continue
                if "bnk-" in text_lower or "dom-" in text_lower:
                    counts[(as_value, uid, title, "OLD_PATTERN", text, json_path)] += 1
                if "bnk/" in text_lower or "dom/" in text_lower:
                    counts[(as_value, uid, title, "NEW_PATTERN", text, json_path)] += 1

    rows: List[Dict[str, Any]] = []
    for (as_value, uid, title, match_type, matched_string, json_path), count in sorted(counts.items()):
        rows.append(
            {
                "AS": as_value,
                "dashboard_uid": uid,
                "dashboard_title": title,
                "match_type": match_type,
                "matched_string": matched_string,
                "json_path": json_path,
                "count": count,
            }
        )
    return rows
