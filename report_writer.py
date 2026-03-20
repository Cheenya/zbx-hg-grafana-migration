from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Mapping, Sequence

from openpyxl import Workbook  # type: ignore

from common import autosize_columns


HOST_HEADERS = [
    "hostid",
    "host",
    "name",
    "status",
    "status_label",
    "AS",
    "ASN",
    "GAS",
    "GUEST_NAME",
    "ENV_RAW",
    "ENV_SCOPE",
    "has_old_groups",
    "missing_any_new_group",
    "old_groups",
    "new_groups",
    "other_groups",
]

UNKNOWN_HOST_HEADERS = [
    "hostid",
    "host",
    "name",
    "status",
    "status_label",
    "AS",
    "ASN",
    "GAS",
    "GUEST_NAME",
    "ENV_RAW",
    "ENV_SCOPE",
    "groups",
    "unknown_reasons",
]

SKIPPED_HOST_HEADERS = [
    "hostid",
    "host",
    "name",
    "status",
    "status_label",
    "AS",
    "ASN",
    "GAS",
    "GUEST_NAME",
    "ENV_RAW",
    "ENV_SCOPE",
    "skip_reason",
]

GRAFANA_SUMMARY_HEADERS = [
    "AS",
    "grafana_org_id",
    "dashboard_uid",
    "dashboard_title",
    "folder_title",
    "dashboard_url",
    "hits_total",
    "exact_old",
    "pattern_old",
    "variable_hits",
    "panel_hits",
    "dashboard_hits",
    "panels",
    "variables",
]

GRAFANA_DETAIL_HEADERS = [
    "AS",
    "grafana_org_id",
    "dashboard_uid",
    "dashboard_title",
    "folder_title",
    "dashboard_url",
    "panel_url",
    "panel_id",
    "panel_title",
    "panel_type",
    "variable_name",
    "variable_type",
    "location_kind",
    "field_kind",
    "reference_kind",
    "match_type",
    "matched_string",
    "source_text",
    "json_path",
    "count",
]


def _append_rows(ws, rows: Sequence[Dict[str, Any]], headers: Sequence[str]) -> None:
    ws.append(list(headers))
    for row in rows:
        ws.append([row.get(header, "") for header in headers])
    autosize_columns(ws)


def _apply_hyperlinks(ws, rows: Sequence[Dict[str, Any]], headers: Sequence[str], link_map: Mapping[str, str]) -> None:
    index_by_header = {header: position + 1 for position, header in enumerate(headers)}
    for row_index, row in enumerate(rows, start=2):
        for header, url_field in link_map.items():
            column_index = index_by_header.get(header)
            if not column_index:
                continue
            url = str(row.get(url_field) or "").strip()
            if not url:
                continue
            cell = ws.cell(row=row_index, column=column_index)
            cell.hyperlink = url
            cell.style = "Hyperlink"


def save_inventory_json(report: Dict[str, Any], path: str) -> None:
    payload = {
        "meta": {
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "version": "2.0",
        },
        "summary": report["summary"],
        "inventory": report["inventory"],
        "unknown_hosts": report["unknown_hosts"],
        "hosts": report["hosts"],
        "hosts_replace": report["hosts_replace"],
        "hosts_no_any_new": report["hosts_no_any_new"],
        "host_enrichment": report["host_enrichment"],
        "hosts_clean": report["hosts_clean"],
        "hosts_disabled": report["hosts_disabled"],
        "hosts_skipped_env": report["hosts_skipped_env"],
        "env_summary": report["env_summary"],
        "asn_summary": report["asn_summary"],
        "gas_summary": report["gas_summary"],
        "guest_name_summary": report["guest_name_summary"],
        "groups_old": report["groups_old"],
        "groups_new": report["groups_new"],
        "mapping_plan": report["mapping_plan"],
        "zabbix_mapping_preview": report["zabbix_mapping_preview"],
        "actions": report["actions"],
        "usergroups": report["usergroups"],
        "maintenances": report["maintenances"],
        "grafana": report["grafana"],
        "grafana_summary": report["grafana_summary"],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def write_workbook(report: Dict[str, Any], out_path: str) -> None:
    wb = Workbook()
    wb.remove(wb.active)

    summary_ws = wb.create_sheet("SUMMARY")
    summary_ws.append(["key", "value"])
    for key, value in report["summary"].items():
        if isinstance(value, list):
            rendered = ", ".join(str(item) for item in value)
        else:
            rendered = value
        summary_ws.append([key, rendered])
    autosize_columns(summary_ws)

    env_ws = wb.create_sheet("ENV_SUMMARY")
    _append_rows(
        env_ws,
        report["env_summary"],
        ["AS", "ENV_RAW", "ENV_SCOPE", "hosts_count", "enabled_hosts", "disabled_hosts", "legacy_hosts", "sample_hosts"],
    )

    asn_ws = wb.create_sheet("ASN_SUMMARY")
    _append_rows(
        asn_ws,
        report["asn_summary"],
        ["AS", "ASN", "hosts_count", "enabled_hosts", "disabled_hosts", "legacy_hosts", "sample_hosts"],
    )

    gas_ws = wb.create_sheet("GAS_SUMMARY")
    _append_rows(
        gas_ws,
        report["gas_summary"],
        ["AS", "GAS", "hosts_count", "enabled_hosts", "disabled_hosts", "legacy_hosts", "sample_hosts"],
    )

    guest_ws = wb.create_sheet("GUEST_NAME_SUMMARY")
    _append_rows(
        guest_ws,
        report["guest_name_summary"],
        ["AS", "GUEST_NAME", "hosts_count", "enabled_hosts", "disabled_hosts", "legacy_hosts", "sample_hosts"],
    )

    hosts_ws = wb.create_sheet("HOSTS")
    _append_rows(hosts_ws, report["hosts"], HOST_HEADERS)

    replace_ws = wb.create_sheet("HOSTS_OLD_SCOPE")
    _append_rows(replace_ws, report["hosts_replace"], HOST_HEADERS)

    replace_missing_ws = wb.create_sheet("HOSTS_NO_ANY_NEW")
    _append_rows(replace_missing_ws, report["hosts_no_any_new"], HOST_HEADERS)

    enrichment_ws = wb.create_sheet("HOST_ENRICHMENT")
    _append_rows(
        enrichment_ws,
        report["host_enrichment"],
        [
            "hostid",
            "host",
            "name",
            "status",
            "status_label",
            "AS",
            "ASN",
            "ENV_RAW",
            "ENV_SCOPE",
            "TARGET_ENV_SCOPE",
            "GAS",
            "TARGET_GAS",
            "GUEST_NAME",
            "old_groups",
            "new_groups",
            "suggested_pairs",
            "suggested_new_groups",
            "missing_new_groups",
            "host_action",
            "manual_required",
        ],
    )

    disabled_ws = wb.create_sheet("HOSTS_DISABLED")
    _append_rows(disabled_ws, report["hosts_disabled"], HOST_HEADERS)

    clean_ws = wb.create_sheet("HOSTS_CLEAN")
    _append_rows(clean_ws, report["hosts_clean"], HOST_HEADERS)

    unknown_ws = wb.create_sheet("UNKNOWN_HOSTS")
    _append_rows(unknown_ws, report["unknown_hosts"], UNKNOWN_HOST_HEADERS)

    skipped_ws = wb.create_sheet("HOSTS_SKIPPED_ENV")
    _append_rows(skipped_ws, report["hosts_skipped_env"], SKIPPED_HOST_HEADERS)

    old_ws = wb.create_sheet("GROUPS_OLD")
    _append_rows(old_ws, report["groups_old"], ["group_name", "groupid", "hosts_count", "as_values", "env_values", "sample_hosts"])

    new_ws = wb.create_sheet("GROUPS_NEW")
    _append_rows(new_ws, report["groups_new"], ["group_name", "groupid", "hosts_count", "as_values", "env_values", "sample_hosts"])

    mapping_ws = wb.create_sheet("MAPPING_PLAN")
    _append_rows(
        mapping_ws,
        report["mapping_plan"],
        [
            "selected",
            "AS",
            "old_group",
            "old_groupid",
            "new_group",
            "new_groupid",
            "candidate_rank",
            "candidate_count",
            "intersection",
            "old_hosts_count",
            "new_hosts_count",
            "old_coverage",
            "new_coverage",
            "jaccard",
            "host_action",
            "hosts_need_add_new",
            "hosts_already_have_new",
            "old_envs",
            "new_envs",
            "env_relation",
            "top1_new_conflict",
            "manual_required",
            "status",
            "comment",
        ],
    )

    preview_ws = wb.create_sheet("ZBX_MAP_PREVIEW")
    _append_rows(
        preview_ws,
        report["zabbix_mapping_preview"],
        [
            "object_type",
            "object_id",
            "object_name",
            "where_found",
            "field_path",
            "old_group",
            "old_groupid",
            "candidate_new_group",
            "candidate_new_groupid",
            "candidate_rank",
            "candidate_count",
            "mapping_status",
            "manual_required",
            "host_action",
            "hosts_need_add_new",
            "hosts_already_have_new",
        ],
    )

    actions_ws = wb.create_sheet("ACTIONS")
    _append_rows(
        actions_ws,
        report["actions"],
        [
            "actionid",
            "name",
            "status",
            "where_found",
            "matched_groupids",
            "matched_group_names",
            "recipient_usergroups",
            "recipient_users",
            "recipients_media",
        ],
    )

    usergroups_ws = wb.create_sheet("USERGROUPS")
    _append_rows(
        usergroups_ws,
        report["usergroups"],
        [
            "usrgrpid",
            "name",
            "rights_on_scope_groups",
            "matching_tag_filters",
            "users",
            "users_media",
            "is_action_recipient",
        ],
    )

    maintenances_ws = wb.create_sheet("MAINTENANCES")
    _append_rows(
        maintenances_ws,
        report["maintenances"],
        ["maintenanceid", "name", "matched_groupids", "matched_group_names", "active_since", "active_till"],
    )

    grafana_summary_ws = wb.create_sheet("GRAFANA_SUMMARY")
    _append_rows(grafana_summary_ws, report["grafana_summary"], GRAFANA_SUMMARY_HEADERS)
    _apply_hyperlinks(grafana_summary_ws, report["grafana_summary"], GRAFANA_SUMMARY_HEADERS, {"dashboard_url": "dashboard_url"})

    inventory_ws = wb.create_sheet("INVENTORY")
    inventory_ws.append(["section", "value"])
    for key, value in report["inventory"].items():
        inventory_ws.append([key, json.dumps(value, ensure_ascii=False)])
    autosize_columns(inventory_ws)

    wb.save(out_path)


def write_grafana_workbook(report: Dict[str, Any], out_path: str) -> None:
    wb = Workbook()
    wb.remove(wb.active)

    summary_rows = [
        {"key": "scope_as", "value": ", ".join(str(item) for item in report["summary"].get("scope_as") or [])},
        {"key": "scope_env", "value": str(report["summary"].get("scope_env") or "")},
        {"key": "grafana_dashboards", "value": report["summary"].get("grafana_dashboards", len(report["grafana_summary"]))},
        {"key": "grafana_rows", "value": report["summary"].get("grafana_rows", len(report["grafana"]))},
        {"key": "grafana_error", "value": report["summary"].get("grafana_error", "")},
    ]
    summary_ws = wb.create_sheet("SUMMARY")
    _append_rows(summary_ws, summary_rows, ["key", "value"])

    dashboards_ws = wb.create_sheet("DASHBOARDS")
    _append_rows(dashboards_ws, report["grafana_summary"], GRAFANA_SUMMARY_HEADERS)
    _apply_hyperlinks(dashboards_ws, report["grafana_summary"], GRAFANA_SUMMARY_HEADERS, {"dashboard_url": "dashboard_url"})

    details_ws = wb.create_sheet("DETAILS")
    _append_rows(details_ws, report["grafana"], GRAFANA_DETAIL_HEADERS)
    _apply_hyperlinks(
        details_ws,
        report["grafana"],
        GRAFANA_DETAIL_HEADERS,
        {
            "dashboard_url": "dashboard_url",
            "panel_url": "panel_url",
        },
    )

    wb.save(out_path)
