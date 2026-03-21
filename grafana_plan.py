from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Sequence, Tuple

from openpyxl import Workbook, load_workbook  # type: ignore

import config
from api_clients import GrafanaAPI
from common import autosize_columns, join_sorted, normalize_values
from grafana_audit import _field_kind, _iter_strings
from mapping_plan import is_selected


GRAFANA_PLAN_HEADERS: List[str] = [
    "apply",
    "grafana_org_id",
    "dashboard_uid",
    "dashboard_title",
    "folder_title",
    "dashboard_url",
    "variable_name",
    "variable_type",
    "datasource_names",
    "field_path",
    "field_kind",
    "old_group",
    "new_group",
    "replace_count",
    "source_value",
    "planned_value",
    "change_kind",
    "manual_required",
    "status",
    "comment",
]

GRAFANA_APPLY_RESULT_HEADERS: List[str] = [
    "grafana_org_id",
    "dashboard_uid",
    "dashboard_title",
    "dashboard_url",
    "variable_name",
    "field_path",
    "field_kind",
    "old_group",
    "new_group",
    "before_value",
    "after_value",
    "status",
    "applied",
    "message",
]


def _log(log: Callable[[str], None] | None, message: str) -> None:
    if log is not None:
        log(message)


def load_grafana_org_report(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save_grafana_plan_json(data: Dict[str, Any], path: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)


def _append_rows(ws, rows: Sequence[Dict[str, Any]], headers: Sequence[str]) -> None:
    ws.append(list(headers))
    for row in rows:
        ws.append([row.get(header, "") for header in headers])
    autosize_columns(ws)


def write_grafana_plan_xlsx(data: Dict[str, Any], path: str) -> None:
    wb = Workbook()
    wb.remove(wb.active)

    summary_ws = wb.create_sheet("SUMMARY")
    summary_ws.append(["key", "value"])
    for key, value in data.get("summary", {}).items():
        if isinstance(value, list):
            rendered = ", ".join(str(item) for item in value)
        else:
            rendered = value
        summary_ws.append([key, rendered])
    autosize_columns(summary_ws)

    plan_ws = wb.create_sheet("PLAN")
    _append_rows(plan_ws, data.get("plan_rows") or [], GRAFANA_PLAN_HEADERS)

    missing_ws = wb.create_sheet("MISSING_VARIABLES")
    _append_rows(
        missing_ws,
        data.get("missing_variables") or [],
        [
            "grafana_org_id",
            "dashboard_uid",
            "dashboard_title",
            "dashboard_url",
            "variable_name",
            "variable_type",
            "status",
            "message",
        ],
    )

    wb.save(path)


def write_grafana_apply_xlsx(data: Dict[str, Any], path: str) -> None:
    wb = Workbook()
    wb.remove(wb.active)

    summary_ws = wb.create_sheet("SUMMARY")
    summary_ws.append(["key", "value"])
    for key, value in data.get("summary", {}).items():
        if isinstance(value, list):
            rendered = ", ".join(str(item) for item in value)
        else:
            rendered = value
        summary_ws.append([key, rendered])
    autosize_columns(summary_ws)

    results_ws = wb.create_sheet("RESULTS")
    _append_rows(results_ws, data.get("results") or [], GRAFANA_APPLY_RESULT_HEADERS)

    dashboards_ws = wb.create_sheet("DASHBOARDS")
    _append_rows(
        dashboards_ws,
        data.get("dashboards") or [],
        [
            "grafana_org_id",
            "dashboard_uid",
            "dashboard_title",
            "dashboard_url",
            "changes_requested",
            "changes_applied",
            "status",
            "message",
        ],
    )

    wb.save(path)


def load_grafana_plan_rows(path: str) -> List[Dict[str, Any]]:
    wb = load_workbook(path, read_only=True, data_only=False)
    ws = wb["PLAN"] if "PLAN" in wb.sheetnames else wb[wb.sheetnames[0]]
    raw_rows = list(ws.iter_rows(values_only=True))
    if not raw_rows:
        return []

    headers = [str(item or "").strip() for item in raw_rows[0]]
    rows: List[Dict[str, Any]] = []
    for raw in raw_rows[1:]:
        if not raw:
            continue
        row = {headers[index]: raw[index] for index in range(min(len(headers), len(raw))) if headers[index]}
        if any(str(value or "").strip() for value in row.values()):
            rows.append(row)
    return rows


def get_selected_grafana_changes(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, str]]:
    selected: List[Dict[str, str]] = []
    for row in rows:
        if not is_selected(row.get("apply")):
            continue
        grafana_org_id = str(row.get("grafana_org_id") or "").strip()
        dashboard_uid = str(row.get("dashboard_uid") or "").strip()
        variable_name = str(row.get("variable_name") or "").strip()
        field_path = str(row.get("field_path") or "").strip()
        old_group = str(row.get("old_group") or "").strip()
        new_group = str(row.get("new_group") or "").strip()
        if not all([grafana_org_id, dashboard_uid, variable_name, field_path, old_group, new_group]):
            raise RuntimeError("Selected Grafana plan row must contain org_id, dashboard_uid, variable_name, field_path, old_group, new_group.")
        selected.append(
            {
                "grafana_org_id": grafana_org_id,
                "dashboard_uid": dashboard_uid,
                "dashboard_title": str(row.get("dashboard_title") or "").strip(),
                "dashboard_url": str(row.get("dashboard_url") or "").strip(),
                "variable_name": variable_name,
                "field_path": field_path,
                "field_kind": str(row.get("field_kind") or "").strip(),
                "old_group": old_group,
                "new_group": new_group,
            }
        )
    if not selected:
        raise RuntimeError("No rows with apply=yes found in Grafana plan.")
    return selected


def _find_variable(dashboard: Dict[str, Any], variable_name: str) -> Tuple[int, Dict[str, Any]] | Tuple[None, None]:
    templating = (dashboard.get("templating") or {}).get("list") or []
    for index, variable in enumerate(templating):
        if str(variable.get("name") or "").strip() == variable_name:
            return index, variable
    return None, None


def _shorten(text: str, limit: int = 500) -> str:
    value = str(text or "")
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


_PATH_TOKEN_RX = re.compile(r"([^.\[]+)|\[(\d+)\]")


def _parse_field_path(path: str) -> List[Any]:
    tokens: List[Any] = []
    for part in str(path or "").split("."):
        if not part:
            continue
        for match in _PATH_TOKEN_RX.finditer(part):
            key, index = match.groups()
            if key is not None:
                tokens.append(key)
            elif index is not None:
                tokens.append(int(index))
    return tokens


def _get_path_value(node: Any, path: str) -> Any:
    current = node
    for token in _parse_field_path(path):
        if isinstance(token, int):
            current = current[token]
        else:
            current = current[token]
    return current


def _set_path_value(node: Any, path: str, value: Any) -> None:
    tokens = _parse_field_path(path)
    if not tokens:
        raise RuntimeError(f"Invalid field path: {path}")
    current = node
    for token in tokens[:-1]:
        if isinstance(token, int):
            current = current[token]
        else:
            current = current[token]
    tail = tokens[-1]
    if isinstance(tail, int):
        current[tail] = value
    else:
        current[tail] = value


def _group_mappings_by_old(selected_mappings: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in selected_mappings:
        old_group = str(row.get("old_group") or "").strip()
        new_group = str(row.get("new_group") or "").strip()
        if not old_group or not new_group:
            continue
        key = (old_group, new_group)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "old_group": old_group,
                "new_group": new_group,
            }
        )
    rows.sort(key=lambda item: (-len(item["old_group"]), item["old_group"].lower()))
    return rows


def _collect_variable_targets(org_audit_report: Dict[str, Any]) -> List[Dict[str, str]]:
    targets: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for row in org_audit_report.get("variables") or []:
        variable_name = str(row.get("variable_name") or "").strip()
        variable_type = str(row.get("variable_type") or "").strip()
        if not variable_name or variable_type == "datasource":
            continue
        key = (
            str(row.get("grafana_org_id") or "").strip(),
            str(row.get("dashboard_uid") or "").strip(),
            variable_name,
        )
        if key in targets:
            continue
        targets[key] = {
            "grafana_org_id": key[0],
            "dashboard_uid": key[1],
            "dashboard_title": str(row.get("dashboard_title") or "").strip(),
            "folder_title": str(row.get("folder_title") or "").strip(),
            "dashboard_url": str(row.get("dashboard_url") or "").strip(),
            "variable_name": variable_name,
            "variable_type": variable_type,
            "datasource_names": str(row.get("datasource_names") or "").strip(),
        }
    return list(targets.values())


def _is_supported_variable_field(path: str) -> bool:
    text = str(path or "").strip()
    if text in {"query", "regex", "definition"}:
        return True
    if text.startswith("current."):
        return True
    if text.startswith("options["):
        return True
    return False


def build_grafana_plan(
    conn: config.GrafanaConnection,
    org_audit_report: Dict[str, Any],
    selected_mappings: Sequence[Dict[str, str]],
    log: Callable[[str], None] | None = None,
) -> Dict[str, Any]:
    mapping_rows = _group_mappings_by_old(selected_mappings)
    if not mapping_rows:
        raise RuntimeError("Selected mappings are empty. Prepare mapping_plan.xlsx first.")

    variable_targets = _collect_variable_targets(org_audit_report)
    api_cache: Dict[int, GrafanaAPI] = {}
    dashboard_cache: Dict[Tuple[int, str], Dict[str, Any]] = {}
    plan_rows: List[Dict[str, Any]] = []
    missing_variables: List[Dict[str, Any]] = []
    seen_rows: set[tuple[str, ...]] = set()

    _log(log, f"grafana-plan: start variable_targets={len(variable_targets)} selected_mappings={len(mapping_rows)}")

    for target_index, target in enumerate(variable_targets, start=1):
        org_id = int(target["grafana_org_id"] or 0)
        dashboard_uid = target["dashboard_uid"]
        variable_name = target["variable_name"]
        if target_index == 1 or target_index % 25 == 0:
            _log(log, f"grafana-plan: progress target={target_index}/{len(variable_targets)} org_id={org_id} uid={dashboard_uid} variable={variable_name}")

        api = api_cache.get(org_id)
        if api is None:
            api = GrafanaAPI(conn.base_url, conn.username, conn.password, org_id=org_id, timeout_sec=int(config.HTTP_TIMEOUT_SEC))
            api_cache[org_id] = api

        cache_key = (org_id, dashboard_uid)
        dashboard_payload = dashboard_cache.get(cache_key)
        if dashboard_payload is None:
            dashboard_payload = api.get_dashboard_by_uid(dashboard_uid)
            dashboard_cache[cache_key] = dashboard_payload
        dashboard = dashboard_payload.get("dashboard") or dashboard_payload

        variable_index, variable = _find_variable(dashboard, variable_name)
        if variable is None or variable_index is None:
            missing_variables.append(
                {
                    "grafana_org_id": str(org_id),
                    "dashboard_uid": dashboard_uid,
                    "dashboard_title": target["dashboard_title"],
                    "dashboard_url": target["dashboard_url"],
                    "variable_name": variable_name,
                    "variable_type": target["variable_type"],
                    "status": "variable_not_found",
                    "message": "Variable missing in current dashboard JSON.",
                }
            )
            continue

        base_path = f"dashboard.templating.list[{variable_index}]"
        string_rows = _iter_strings(variable, path=base_path)
        field_hits = 0
        for json_path, text in string_rows:
            source_value = str(text or "")
            if not source_value:
                continue
            relative_path = json_path[len(base_path) + 1 :] if json_path.startswith(base_path + ".") else json_path[len(base_path) :]
            relative_path = relative_path.lstrip(".")
            if not relative_path:
                continue
            if not _is_supported_variable_field(relative_path):
                continue
            field_kind = _field_kind(json_path)
            matched_rows = [row for row in mapping_rows if row["old_group"] in source_value]
            if not matched_rows:
                continue
            field_hits += 1
            manual_required = field_kind in {"regex", "query", "definition", "expression", "sql"} or len(matched_rows) > 1
            for mapping in matched_rows:
                old_group = mapping["old_group"]
                new_group = mapping["new_group"]
                replace_count = source_value.count(old_group)
                planned_value = source_value.replace(old_group, new_group)
                row = {
                    "apply": "",
                    "grafana_org_id": str(org_id),
                    "dashboard_uid": dashboard_uid,
                    "dashboard_title": target["dashboard_title"],
                    "folder_title": target["folder_title"],
                    "dashboard_url": target["dashboard_url"],
                    "variable_name": variable_name,
                    "variable_type": target["variable_type"],
                    "datasource_names": target["datasource_names"],
                    "field_path": relative_path,
                    "field_kind": field_kind,
                    "old_group": old_group,
                    "new_group": new_group,
                    "replace_count": replace_count,
                    "source_value": _shorten(source_value),
                    "planned_value": _shorten(planned_value),
                    "change_kind": "substring_replace",
                    "manual_required": "yes" if manual_required else "",
                    "status": "review_regex" if manual_required else "candidate",
                    "comment": "",
                }
                row_key = (
                    row["grafana_org_id"],
                    row["dashboard_uid"],
                    row["variable_name"],
                    row["field_path"],
                    row["old_group"],
                    row["new_group"],
                )
                if row_key in seen_rows:
                    continue
                seen_rows.add(row_key)
                plan_rows.append(row)
        if field_hits:
            _log(log, f"grafana-plan: hit org_id={org_id} uid={dashboard_uid} variable={variable_name} fields={field_hits}")

    org_ids = normalize_values(org_audit_report.get("summary", {}).get("grafana_org_ids") or [])
    summary = {
        "grafana_org_ids": org_ids,
        "variable_targets": len(variable_targets),
        "selected_mappings": len(mapping_rows),
        "plan_rows": len(plan_rows),
        "manual_rows": sum(1 for row in plan_rows if str(row.get("manual_required") or "").strip()),
        "missing_variables": len(missing_variables),
    }
    _log(log, f"grafana-plan: summary={summary}")
    return {
        "summary": summary,
        "plan_rows": plan_rows,
        "missing_variables": missing_variables,
    }


def apply_grafana_plan(
    conn: config.GrafanaConnection,
    selected_rows: Sequence[Dict[str, str]],
    dry_run: bool = True,
    log: Callable[[str], None] | None = None,
) -> Dict[str, Any]:
    grouped: Dict[Tuple[int, str], List[Dict[str, str]]] = defaultdict(list)
    for row in selected_rows:
        grouped[(int(row["grafana_org_id"]), row["dashboard_uid"])].append(row)

    results: List[Dict[str, Any]] = []
    dashboards: List[Dict[str, Any]] = []
    api_cache: Dict[int, GrafanaAPI] = {}
    _log(log, f"grafana-apply: start dashboards={len(grouped)} mode={'dry-run' if dry_run else 'apply'} rows={len(selected_rows)}")

    for (org_id, dashboard_uid), rows in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        api = api_cache.get(org_id)
        if api is None:
            api = GrafanaAPI(conn.base_url, conn.username, conn.password, org_id=org_id, timeout_sec=int(config.HTTP_TIMEOUT_SEC))
            api_cache[org_id] = api

        payload = api.get_dashboard_by_uid(dashboard_uid)
        dashboard = payload.get("dashboard") or payload
        meta = payload.get("meta") or {}
        dashboard_title = str(dashboard.get("title") or rows[0].get("dashboard_title") or "")
        dashboard_url = str(meta.get("url") or rows[0].get("dashboard_url") or "")
        if dashboard_url and not dashboard_url.startswith("http://") and not dashboard_url.startswith("https://"):
            dashboard_url = f"{conn.base_url.rstrip('/')}{dashboard_url}"

        changed = 0
        requested = 0
        for row in rows:
            requested += 1
            variable_name = row["variable_name"]
            field_path = row["field_path"]
            field_kind = str(row.get("field_kind") or "")
            old_group = row["old_group"]
            new_group = row["new_group"]
            _, variable = _find_variable(dashboard, variable_name)
            if variable is None:
                results.append(
                    {
                        "grafana_org_id": str(org_id),
                        "dashboard_uid": dashboard_uid,
                        "dashboard_title": dashboard_title,
                        "dashboard_url": dashboard_url,
                        "variable_name": variable_name,
                        "field_path": field_path,
                        "field_kind": field_kind,
                        "old_group": old_group,
                        "new_group": new_group,
                        "before_value": "",
                        "after_value": "",
                        "status": "variable_not_found",
                        "applied": "",
                        "message": "Variable missing in current dashboard JSON.",
                    }
                )
                continue
            try:
                current_value = _get_path_value(variable, field_path)
            except Exception as exc:
                results.append(
                    {
                        "grafana_org_id": str(org_id),
                        "dashboard_uid": dashboard_uid,
                        "dashboard_title": dashboard_title,
                        "dashboard_url": dashboard_url,
                        "variable_name": variable_name,
                        "field_path": field_path,
                        "field_kind": field_kind,
                        "old_group": old_group,
                        "new_group": new_group,
                        "before_value": "",
                        "after_value": "",
                        "status": "field_not_found",
                        "applied": "",
                        "message": str(exc),
                    }
                )
                continue

            before_value = str(current_value or "")
            if old_group not in before_value:
                results.append(
                    {
                        "grafana_org_id": str(org_id),
                        "dashboard_uid": dashboard_uid,
                        "dashboard_title": dashboard_title,
                        "dashboard_url": dashboard_url,
                        "variable_name": variable_name,
                        "field_path": field_path,
                        "field_kind": field_kind,
                        "old_group": old_group,
                        "new_group": new_group,
                        "before_value": _shorten(before_value),
                        "after_value": _shorten(before_value),
                        "status": "old_group_missing",
                        "applied": "",
                        "message": "Old group string not found in current field value.",
                    }
                )
                continue

            after_value = before_value.replace(old_group, new_group)
            _set_path_value(variable, field_path, after_value)
            changed += 1
            results.append(
                {
                    "grafana_org_id": str(org_id),
                    "dashboard_uid": dashboard_uid,
                    "dashboard_title": dashboard_title,
                    "dashboard_url": dashboard_url,
                    "variable_name": variable_name,
                    "field_path": field_path,
                    "field_kind": field_kind,
                    "old_group": old_group,
                    "new_group": new_group,
                    "before_value": _shorten(before_value),
                    "after_value": _shorten(after_value),
                    "status": "dry_run_changed" if dry_run else "changed",
                    "applied": "no" if dry_run else "yes",
                    "message": "",
                }
            )

        dashboard_status = "unchanged"
        dashboard_message = ""
        if changed and not dry_run:
            folder_id = int(meta.get("folderId") or 0)
            message = f"Host-group variable migration ({datetime.now().isoformat(timespec='seconds')})"
            api.update_dashboard(dashboard, folder_id, message)
            dashboard_status = "updated"
            dashboard_message = message
        elif changed:
            dashboard_status = "dry_run"
        dashboards.append(
            {
                "grafana_org_id": str(org_id),
                "dashboard_uid": dashboard_uid,
                "dashboard_title": dashboard_title,
                "dashboard_url": dashboard_url,
                "changes_requested": requested,
                "changes_applied": changed,
                "status": dashboard_status,
                "message": dashboard_message,
            }
        )
        _log(log, f"grafana-apply: dashboard org_id={org_id} uid={dashboard_uid} requested={requested} changed={changed} mode={'dry-run' if dry_run else 'apply'}")

    summary = {
        "mode": "dry-run" if dry_run else "apply",
        "selected_rows": len(selected_rows),
        "dashboards": len(dashboards),
        "changed_rows": sum(1 for row in results if row.get("status") in {"dry_run_changed", "changed"}),
        "updated_dashboards": sum(1 for row in dashboards if row.get("status") in {"dry_run", "updated"}),
        "errors": sum(1 for row in results if str(row.get("status") or "").endswith("not_found") or row.get("status") == "old_group_missing"),
    }
    _log(log, f"grafana-apply: summary={summary}")
    return {
        "summary": summary,
        "dashboards": dashboards,
        "results": results,
    }
