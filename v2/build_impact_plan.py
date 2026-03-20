from __future__ import annotations

import argparse
from datetime import datetime

import config
from api_clients import ZabbixAPI
from common import build_artifact_path, normalize_values
from impact_plan import build_impact_plan, load_audit_report, save_impact_plan_json, write_impact_plan_xlsx
from mapping_plan import get_selected_mappings, load_mapping_plan_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Build impact plan from audit JSON and edited mapping plan")
    parser.add_argument("--out-xlsx", dest="out_xlsx", help="Path to impact plan XLSX")
    parser.add_argument("--out-json", dest="out_json", help="Path to impact plan JSON")
    args = parser.parse_args()

    audit_json_path = str(config.SOURCE_AUDIT_JSON or "").strip()
    mapping_plan_path = str(config.SOURCE_MAPPING_PLAN_XLSX or "").strip()
    if not audit_json_path:
        raise RuntimeError("Set v2/config.py SOURCE_AUDIT_JSON before building impact plan.")
    if not mapping_plan_path:
        raise RuntimeError("Set v2/config.py SOURCE_MAPPING_PLAN_XLSX before building impact plan.")

    audit_report = load_audit_report(audit_json_path)
    inventory = audit_report.get("inventory") or {}
    scope_as = normalize_values(inventory.get("scope_as") or [])
    scope_env = str(inventory.get("scope_env") or "").strip()
    if not scope_env:
        legacy_scope_envs = inventory.get("scope_envs") or []
        if legacy_scope_envs:
            scope_env = str(legacy_scope_envs[0] or "").strip()
    mapping_rows = load_mapping_plan_rows(mapping_plan_path)
    selected_mappings = get_selected_mappings(mapping_rows)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_xlsx = build_artifact_path(config.IMPACT_PLAN_PREFIX, scope_as, scope_env, ".xlsx", timestamp=timestamp)
    default_json = build_artifact_path(config.IMPACT_PLAN_PREFIX, scope_as, scope_env, ".json", timestamp=timestamp)
    out_xlsx = args.out_xlsx or default_xlsx
    out_json = args.out_json or default_json

    connection = config.load_zabbix_connection()
    api = ZabbixAPI(connection.api_url, timeout_sec=int(config.HTTP_TIMEOUT_SEC))
    api.login(connection.username, connection.password)

    print(f"Building impact plan from: {mapping_plan_path}")
    impact_data = build_impact_plan(api, audit_report, selected_mappings, audit_json_path, mapping_plan_path)

    print(f"Writing impact XLSX: {out_xlsx}")
    write_impact_plan_xlsx(impact_data, out_xlsx)
    print(f"Writing impact JSON: {out_json}")
    save_impact_plan_json(impact_data, out_json)
    print("Impact plan completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
