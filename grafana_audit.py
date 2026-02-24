#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""grafana_audit.py — поиск упоминаний OLD/NEW host-groups в Grafana dashboards."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import urllib3  # type: ignore

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests  # type: ignore

from config import CONFIG, GrafanaConnection

# Грубые паттерны, чтобы вытягивать кандидатов из JSON
OLD_RX = re.compile(r"(BNK|DOM)-[^\"\\\\]+")
NEW_RX = re.compile(r"(BNK|DOM)/[^\"\\\\]+")
TRAILING_CLEAN_RX = re.compile(r"[\\s,;:\\)\\]\\}\\'\\\"]+$")
LEADING_CLEAN_RX = re.compile(r"^[\\s\\(\\[\\{\\'\\\"]+")
REGEX_META_RX = re.compile(r"[\\^$.*+?()\\[\\]{}|]")


class GrafanaAPI:
    def __init__(self, base_url: str, username: str, password: str, token: str, timeout_sec: Optional[int] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = int(timeout_sec or CONFIG.runtime.http_timeout_sec)
        self.s = requests.Session()
        if token:
            self.s.headers.update({"Authorization": f"Bearer {token}"})
        if username or password:
            self.s.auth = (username, password)
        self.s.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        url = f"{self.base_url}{path}"
        try:
            r = self.s.get(url, params=params, timeout=self.timeout, verify=False)
            r.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"Grafana API error (GET {path}): {e}") from e
        try:
            return r.json()
        except Exception as e:
            raise RuntimeError(f"Grafana API invalid JSON (GET {path}): {e}") from e

    def list_dashboards(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        page, limit = 1, 500
        while True:
            chunk = self._get("/api/search", params={"type": "dash-db", "limit": limit, "page": page})
            if not chunk:
                break
            out.extend(chunk)
            if len(chunk) < limit:
                break
            page += 1
        return out

    def get_dashboard_by_uid(self, uid: str) -> Dict[str, Any]:
        return self._get(f"/api/dashboards/uid/{uid}")


def _normalize_candidate(raw: str) -> str:
    s = str(raw).strip()
    s = LEADING_CLEAN_RX.sub("", s)
    s = TRAILING_CLEAN_RX.sub("", s)
    for sep in ("|", "&", ";"):
        if sep in s:
            s = s.split(sep, 1)[0]
    return s.strip()


def _iter_strings(node: Any) -> List[str]:
    out: List[str] = []
    if isinstance(node, str):
        out.append(node)
    elif isinstance(node, list):
        for v in node:
            out.extend(_iter_strings(v))
    elif isinstance(node, dict):
        for v in node.values():
            out.extend(_iter_strings(v))
    return out


def _is_pattern_like(s: str) -> bool:
    if not s:
        return False
    if ("BNK-" not in s and "DOM-" not in s and "BNK/" not in s and "DOM/" not in s):
        return False
    if "$" in s:
        return True
    return REGEX_META_RX.search(s) is not None


def _build_as_prefixes(scope_as: Iterable[str]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for as_val in scope_as:
        av = str(as_val).strip()
        if not av:
            continue
        low = av.lower()
        out[as_val] = [
            f"bnk-{low}",
            f"dom-{low}",
            f"bnk/as/{low}",
            f"dom/as/{low}",
        ]
    return out


def _build_group_meta(report_as: Dict[str, Any], scope_as: Optional[Iterable[str]]) -> Dict[str, Dict[str, Set[str]]]:
    scope_lower = {str(x).strip().lower() for x in scope_as} if scope_as else None
    meta: Dict[str, Dict[str, Set[str]]] = {}
    for as_val, data in report_as.items():
        if scope_lower and str(as_val).strip().lower() not in scope_lower:
            continue
        for og in data.get("groups_old") or []:
            if not og:
                continue
            meta.setdefault(str(og), {"old_as": set(), "new_as": set()})["old_as"].add(as_val)
        for ng in data.get("groups_new") or []:
            if not ng:
                continue
            meta.setdefault(str(ng), {"old_as": set(), "new_as": set()})["new_as"].add(as_val)
    return meta


def collect_grafana_matches(
    conn: GrafanaConnection,
    report_as: Dict[str, Any],
    scope_as: Optional[Iterable[str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Возвращает dict AS -> list(rows) для отчёта Grafana."""
    meta = _build_group_meta(report_as, scope_as)
    if not meta:
        return {}

    api = GrafanaAPI(conn.base_url, conn.username, conn.password, conn.token)
    dashboards = api.list_dashboards()

    counts: Dict[Tuple[str, str, str, str, str], int] = defaultdict(int)
    scope_list = list(report_as.keys())
    as_prefixes = _build_as_prefixes(scope_list)

    for d in dashboards:
        uid = d.get("uid")
        title = d.get("title")
        if not uid:
            continue
        dash_json = api.get_dashboard_by_uid(str(uid))
        strings = _iter_strings(dash_json)

        for s in strings:
            # exact group mentions
            for rx, mtype in ((OLD_RX, "OLD"), (NEW_RX, "NEW")):
                for m in rx.finditer(s):
                    candidate = _normalize_candidate(m.group(0))
                    if not candidate:
                        continue
                    meta_entry = meta.get(candidate)
                    if not meta_entry:
                        continue
                    if mtype == "OLD":
                        as_list = meta_entry.get("old_as") or set()
                    else:
                        as_list = meta_entry.get("new_as") or set()
                    for as_val in as_list:
                        key = (as_val, str(uid), str(title or ""), candidate, mtype)
                        counts[key] += 1

            # pattern/regex/variable mentions
            if _is_pattern_like(s):
                s_low = s.lower()
                matched_as: List[str] = []
                for as_val, prefixes in as_prefixes.items():
                    if any(p in s_low for p in prefixes):
                        matched_as.append(as_val)
                if matched_as:
                    if "BNK-" in s or "DOM-" in s:
                        for as_val in matched_as:
                            key = (as_val, str(uid), str(title or ""), s, "OLD_PATTERN")
                            counts[key] += 1
                    if "BNK/" in s or "DOM/" in s:
                        for as_val in matched_as:
                            key = (as_val, str(uid), str(title or ""), s, "NEW_PATTERN")
                            counts[key] += 1

    rows_by_as: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for (as_val, uid, title, matched, mtype), count in counts.items():
        rows_by_as[as_val].append(
            {
                "dashboard_uid": uid,
                "dashboard_title": title,
                "matched_string": matched,
                "match_type": mtype,
                "count": count,
            }
        )

    for as_val, rows in rows_by_as.items():
        rows.sort(key=lambda r: (r.get("dashboard_title") or "", r.get("matched_string") or ""))
        rows_by_as[as_val] = rows

    return rows_by_as
