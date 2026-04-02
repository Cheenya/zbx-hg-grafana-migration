#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Единая точка применения изменений: безопасный Zabbix apply и/или Grafana apply."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Dict, List, Sequence

import config
from api_clients import ZabbixAPI
from apply_zabbix_plan import (
    _assert_host_massadd_available,
    _validate_backup_scope,
    _auto_object_rows,
    _assert_method_available,
    apply_zabbix_changes,
    load_impact_plan,
    write_apply_xlsx as write_zabbix_apply_xlsx,
)
from common import build_artifact_path, build_org_artifact_path, normalize_values, resolve_input_artifact
from grafana_plan import (
    apply_grafana_plan,
    get_selected_grafana_changes,
    load_grafana_plan_rows,
    write_grafana_apply_xlsx,
)


def _confirm(prompt: str) -> bool:
    answer = input(f"{prompt} (yes/no): ").strip().lower()
    return answer in {"yes", "да"}


def _zabbix_preview(impact_plan: Dict[str, Any]) -> Dict[str, Any]:
    host_rows = impact_plan.get("host_enrich_plan") or []
    host_ids = {
        str(row.get("object_id") or "").strip()
        for row in host_rows
        if str(row.get("object_id") or "").strip()
    }
    auto_object_rows = _auto_object_rows(impact_plan)
    manual_object_rows = [
        row
        for row in impact_plan.get("object_mapping_plan") or []
        if str(row.get("manual_required") or "").strip().lower() == "yes"
        or str(row.get("change_kind") or "").strip() not in {"replace_groupid", "add_group_permission"}
    ]
    summary = impact_plan.get("summary") or {}
    return {
        "scope_as": summary.get("scope_as") or [],
        "scope_env": str(summary.get("scope_env") or "").strip(),
        "scope_gas": summary.get("scope_gas") or [],
        "host_rows": len(host_rows),
        "hosts": len(host_ids),
        "auto_object_rows": len(auto_object_rows),
        "manual_object_rows": len(manual_object_rows),
    }


def _grafana_preview(selected_rows: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    dashboards = {
        (str(row.get("grafana_org_id") or "").strip(), str(row.get("dashboard_uid") or "").strip())
        for row in selected_rows
        if str(row.get("grafana_org_id") or "").strip() and str(row.get("dashboard_uid") or "").strip()
    }
    manual_rows = sum(1 for row in selected_rows if str(row.get("change_mode") or "").strip() == "manual_regex")
    org_ids = sorted({int(str(row.get("grafana_org_id") or "0")) for row in selected_rows if str(row.get("grafana_org_id") or "").strip()})
    return {
        "grafana_org_ids": org_ids,
        "selected_rows": len(selected_rows),
        "dashboards": len(dashboards),
        "manual_rows": manual_rows,
    }


def _print_preview(targets: Sequence[str], zbx_preview: Dict[str, Any] | None, graf_preview: Dict[str, Any] | None, mode: str) -> None:
    print(f"Mode: {mode}")
    if "zabbix" in targets and zbx_preview is not None:
        print("Zabbix:")
        print(f"  scope_as: {', '.join(zbx_preview['scope_as']) or '-'}")
        print(f"  scope_env: {zbx_preview['scope_env'] or '-'}")
        print(f"  scope_gas: {', '.join(zbx_preview['scope_gas']) or '-'}")
        print(f"  host rows: {zbx_preview['host_rows']}")
        print(f"  hosts to enrich: {zbx_preview['hosts']}")
        print(f"  auto object rows: {zbx_preview['auto_object_rows']}")
        print(f"  manual object rows: {zbx_preview['manual_object_rows']}")
    if "grafana" in targets and graf_preview is not None:
        print("Grafana:")
        print(f"  org_ids: {', '.join(str(item) for item in graf_preview['grafana_org_ids']) or '-'}")
        print(f"  selected rows: {graf_preview['selected_rows']}")
        print(f"  dashboards: {graf_preview['dashboards']}")
        print(f"  manual regex rows: {graf_preview['manual_rows']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Unified apply runner for Zabbix and Grafana")
    parser.add_argument("--target", choices=("zabbix", "grafana", "both"), default="both", help="What to run")
    parser.add_argument("--apply", action="store_true", help="Apply changes instead of dry-run")
    parser.add_argument("--yes", action="store_true", help="Skip interactive confirmation")
    args = parser.parse_args()

    dry_run = not bool(args.apply)
    targets = ["zabbix", "grafana"] if args.target == "both" else [args.target]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    impact_plan_path = ""
    impact_plan: Dict[str, Any] | None = None
    zbx_preview: Dict[str, Any] | None = None
    backup_path = ""
    zbx_scope_as: List[str] = []
    zbx_scope_env = ""
    zbx_scope_gas: List[str] = []

    plan_path = ""
    selected_grafana_rows: List[Dict[str, str]] = []
    selected_mappings: List[Dict[str, str]] = []
    graf_preview: Dict[str, Any] | None = None

    if "zabbix" in targets:
        try:
            impact_plan_path = resolve_input_artifact(
                config.SOURCE_IMPACT_PLAN_JSON,
                config.IMPACT_PLAN_PREFIX,
                ".json",
                scope_as=config.SCOPE_AS,
                scope_env=config.SCOPE_ENV,
                scope_gas=config.SCOPE_GAS,
                label="impact plan JSON",
            )
        except RuntimeError as exc:
            raise RuntimeError(f"{exc}. Run `python build_impact_plan.py` first.") from exc
        impact_plan = load_impact_plan(impact_plan_path)
        zbx_preview = _zabbix_preview(impact_plan)
        zbx_scope_as = normalize_values((impact_plan.get("summary") or {}).get("scope_as") or [])
        zbx_scope_env = str((impact_plan.get("summary") or {}).get("scope_env") or "").strip()
        zbx_scope_gas = normalize_values((impact_plan.get("summary") or {}).get("scope_gas") or [])
        if not dry_run:
            try:
                backup_path = resolve_input_artifact(
                    config.SOURCE_BACKUP_FILE,
                    config.BACKUP_PREFIX,
                    ".json.gz",
                    scope_as=zbx_scope_as,
                    scope_env=zbx_scope_env,
                    scope_gas=zbx_scope_gas,
                    label="backup file",
                )
            except RuntimeError as exc:
                raise RuntimeError(f"{exc}. Run `python make_backup.py` and `python verify_backup.py` first.") from exc

    if "grafana" in targets:
        if impact_plan is None:
            try:
                impact_plan_path = resolve_input_artifact(
                    config.SOURCE_IMPACT_PLAN_JSON,
                    config.IMPACT_PLAN_PREFIX,
                    ".json",
                    scope_as=config.SCOPE_AS,
                    scope_env=config.SCOPE_ENV,
                    scope_gas=config.SCOPE_GAS,
                    label="impact plan JSON",
                )
            except RuntimeError as exc:
                raise RuntimeError(f"{exc}. Run `python build_impact_plan.py` first.") from exc
            impact_plan = load_impact_plan(impact_plan_path)
        try:
            plan_path = resolve_input_artifact(
                config.SOURCE_GRAFANA_PLAN_XLSX,
                config.GRAFANA_PLAN_PREFIX,
                ".xlsx",
                org_ids=config.GRAFANA_AUDIT_ORGIDS,
                label="Grafana plan XLSX",
            )
        except RuntimeError as exc:
            raise RuntimeError(f"{exc}. Run `python build_grafana_plan.py` after `python build_impact_plan.py`.") from exc
        selected_grafana_rows = get_selected_grafana_changes(load_grafana_plan_rows(plan_path))
        selected_mappings = list((impact_plan or {}).get("selected_mappings") or [])
        graf_preview = _grafana_preview(selected_grafana_rows)

    _print_preview(targets, zbx_preview, graf_preview, "APPLY" if not dry_run else "DRY-RUN")

    if not dry_run and not args.yes:
        if not _confirm("Продолжить применение изменений"):
            print("Cancelled.")
            return 1

    if "zabbix" in targets and impact_plan is not None:
        connection = config.load_zabbix_connection()
        api = ZabbixAPI(connection.api_url, timeout_sec=int(config.HTTP_TIMEOUT_SEC))
        api.authenticate(connection.username, connection.password, connection.api_token)
        if not dry_run:
            auto_rows = _auto_object_rows(impact_plan)
            methods_to_check = set()
            if any(str(row.get("object_id") or "").strip() for row in impact_plan.get("host_enrich_plan") or []):
                methods_to_check.add("host.massadd")
            if any(str(row.get("object_type") or "") == "action" for row in auto_rows):
                methods_to_check.add("action.update")
            if any(str(row.get("object_type") or "") == "usergroup" for row in auto_rows):
                methods_to_check.add("usergroup.update")
            if any(str(row.get("object_type") or "") == "maintenance" for row in auto_rows):
                methods_to_check.add("maintenance.update")
            for method in sorted(methods_to_check):
                print(f"Checking Zabbix API permissions for {method}")
                params = {"hosts": [], "groups": []} if method == "host.massadd" else {}
                if method == "host.massadd":
                    _assert_host_massadd_available(api)
                else:
                    _assert_method_available(api, method, params)
            print(f"Validating backup for Zabbix apply: {backup_path}")
            _validate_backup_scope(backup_path, impact_plan, api)

        print(f"Running Zabbix {'apply' if not dry_run else 'dry-run'} from: {impact_plan_path}")
        zbx_result = apply_zabbix_changes(api, impact_plan, dry_run=dry_run)
        zbx_result.setdefault("meta", {})["impact_plan_path"] = impact_plan_path
        zbx_out_xlsx = build_artifact_path(config.ZABBIX_APPLY_PREFIX, zbx_scope_as, zbx_scope_env, zbx_scope_gas, ".xlsx", timestamp=timestamp)
        zbx_out_json = build_artifact_path(config.ZABBIX_APPLY_PREFIX, zbx_scope_as, zbx_scope_env, zbx_scope_gas, ".json", timestamp=timestamp)
        print(f"Writing Zabbix apply XLSX: {zbx_out_xlsx}")
        write_zabbix_apply_xlsx(zbx_result, zbx_out_xlsx)
        print(f"Writing Zabbix apply JSON: {zbx_out_json}")
        with open(zbx_out_json, "w", encoding="utf-8") as handle:
            json.dump(zbx_result, handle, ensure_ascii=False, indent=2)

    if "grafana" in targets:
        connection = config.load_grafana_connection()
        print(f"Running Grafana {'apply' if not dry_run else 'dry-run'} from: {plan_path}")
        if impact_plan_path:
            print(f"Validating Grafana plan against impact plan: {impact_plan_path}")
        graf_result = apply_grafana_plan(connection, selected_grafana_rows, selected_mappings, dry_run=dry_run, log=print)
        org_ids = [int(value) for value in normalize_values(sorted({row['grafana_org_id'] for row in selected_grafana_rows}))]
        graf_out_xlsx = build_org_artifact_path(config.GRAFANA_APPLY_PREFIX, org_ids, ".xlsx", timestamp=timestamp)
        graf_out_json = build_org_artifact_path(config.GRAFANA_APPLY_PREFIX, org_ids, ".json", timestamp=timestamp)
        print(f"Writing Grafana apply XLSX: {graf_out_xlsx}")
        write_grafana_apply_xlsx(graf_result, graf_out_xlsx)
        print(f"Writing Grafana apply JSON: {graf_out_json}")
        with open(graf_out_json, "w", encoding="utf-8") as handle:
            json.dump(graf_result, handle, ensure_ascii=False, indent=2)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
