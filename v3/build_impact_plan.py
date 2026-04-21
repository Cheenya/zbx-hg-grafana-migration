from __future__ import annotations

import argparse
from datetime import datetime

import config
from clients.api_clients import ZabbixAPI
from core.common import build_artifact_path, normalize_values, resolve_input_artifact
from planning.impact_plan import build_impact_plan, load_audit_report, save_impact_plan_json, write_impact_plan_xlsx
from planning.mapping_plan import get_selected_mappings, load_mapping_plan_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Build impact plan from audit JSON and edited mapping plan")
    parser.add_argument("--out-xlsx", dest="out_xlsx", help="Path to impact plan XLSX")
    parser.add_argument("--out-json", dest="out_json", help="Path to impact plan JSON")
    args = parser.parse_args()

    audit_json_path = resolve_input_artifact(
        config.SOURCE_AUDIT_JSON,
        config.OUTPUT_PREFIX,
        ".json",
        scope_as=config.SCOPE_AS,
        scope_env=config.SCOPE_ENV,
        scope_gas=config.SCOPE_GAS,
        label="audit JSON",
        strict_scope_match=True,
    )
    mapping_plan_path = resolve_input_artifact(
        config.SOURCE_MAPPING_PLAN_XLSX,
        config.MAPPING_PLAN_PREFIX,
        ".xlsx",
        scope_as=config.SCOPE_AS,
        scope_env=config.SCOPE_ENV,
        scope_gas=config.SCOPE_GAS,
        label="mapping plan XLSX",
        strict_scope_match=True,
    )

    audit_report = load_audit_report(audit_json_path)
    inventory = audit_report.get("inventory") or {}
    scope_as = normalize_values(inventory.get("scope_as") or [])
    scope_env = str(inventory.get("scope_env") or "").strip()
    scope_gas = normalize_values(inventory.get("scope_gas") or [])
    if not scope_env:
        legacy_scope_envs = inventory.get("scope_envs") or []
        if legacy_scope_envs:
            scope_env = str(legacy_scope_envs[0] or "").strip()
    mapping_rows = load_mapping_plan_rows(mapping_plan_path)
    selected_mappings = get_selected_mappings(mapping_rows)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_xlsx = build_artifact_path(config.IMPACT_PLAN_PREFIX, scope_as, scope_env, scope_gas, ".xlsx", timestamp=timestamp)
    default_json = build_artifact_path(config.IMPACT_PLAN_PREFIX, scope_as, scope_env, scope_gas, ".json", timestamp=timestamp)
    out_xlsx = args.out_xlsx or default_xlsx
    out_json = args.out_json or default_json

    connection = config.load_zabbix_connection()
    api = ZabbixAPI(connection.api_url, timeout_sec=int(config.HTTP_TIMEOUT_SEC))
    api.authenticate(connection.username, connection.password, connection.api_token)

    if not str(config.SOURCE_AUDIT_JSON or "").strip():
        print(f"Using latest audit JSON: {audit_json_path}")
    if not str(config.SOURCE_MAPPING_PLAN_XLSX or "").strip():
        print(f"Using latest mapping plan XLSX: {mapping_plan_path}")
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
