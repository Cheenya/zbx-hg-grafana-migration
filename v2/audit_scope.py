from __future__ import annotations

import argparse
from datetime import datetime

import config
from api_clients import ZabbixAPI
from common import build_artifact_path, normalize_scope_env, normalize_values, resolve_scope_org_pairs
from grafana_audit import collect_grafana_report
from mapping_plan import write_mapping_plan_xlsx
from report_writer import save_inventory_json, write_grafana_workbook, write_workbook
from zabbix_audit import build_scope_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only scoped audit v2 (Zabbix + Grafana)")
    parser.add_argument("--out-xlsx", dest="out_xlsx", help="Path to XLSX report")
    parser.add_argument("--out-json", dest="out_json", help="Path to JSON inventory")
    parser.add_argument("--out-mapping", dest="out_mapping", help="Path to standalone mapping plan XLSX")
    parser.add_argument("--out-grafana", dest="out_grafana", help="Path to separate Grafana XLSX report")
    args = parser.parse_args()

    scope_as = normalize_values(config.SCOPE_AS)
    scope_env = normalize_scope_env(config.SCOPE_ENV)
    if not scope_as:
        raise RuntimeError("v2 scope is empty. Set v2/config.py SCOPE_AS.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_xlsx = build_artifact_path(config.OUTPUT_PREFIX, scope_as, scope_env, ".xlsx", timestamp=timestamp)
    default_json = build_artifact_path(config.OUTPUT_PREFIX, scope_as, scope_env, ".json", timestamp=timestamp)
    default_mapping = build_artifact_path(config.MAPPING_PLAN_PREFIX, scope_as, scope_env, ".xlsx", timestamp=timestamp)
    default_grafana = build_artifact_path(config.GRAFANA_REPORT_PREFIX, scope_as, scope_env, ".xlsx", timestamp=timestamp)
    out_xlsx = args.out_xlsx or default_xlsx
    out_json = args.out_json or default_json
    out_mapping = args.out_mapping or default_mapping
    out_grafana = args.out_grafana or default_grafana

    connection = config.load_zabbix_connection()
    zabbix = ZabbixAPI(connection.api_url, timeout_sec=int(config.HTTP_TIMEOUT_SEC))
    zabbix.login(connection.username, connection.password)

    print("Running Zabbix inventory (v2)...")
    report = build_scope_report(zabbix, scope_as, scope_env)

    if config.ENABLE_GRAFANA:
        try:
            print("Running Grafana inventory (v2)...")
            grafana = config.load_grafana_connection()
            scope_pairs = resolve_scope_org_pairs(scope_as, config.GRAFANA_ORGIDS)
            grafana_report = collect_grafana_report(grafana, scope_pairs, report["inventory"]["grafana_old_groups"])
            report["grafana"] = grafana_report["detail_rows"]
            report["grafana_summary"] = grafana_report["summary_rows"]
            report["summary"]["grafana_rows"] = len(report["grafana"])
            report["summary"]["grafana_dashboards"] = len(report["grafana_summary"])
        except Exception as exc:
            report["summary"]["grafana_error"] = str(exc)

    print(f"Writing XLSX: {out_xlsx}")
    write_workbook(report, out_xlsx)

    print(f"Writing mapping plan: {out_mapping}")
    write_mapping_plan_xlsx(report["mapping_plan"], out_mapping)

    if config.ENABLE_GRAFANA and ("grafana_error" not in report["summary"]):
        print(f"Writing Grafana XLSX: {out_grafana}")
        write_grafana_workbook(report, out_grafana)

    if config.SAVE_JSON_INVENTORY:
        print(f"Writing JSON: {out_json}")
        save_inventory_json(report, out_json)

    print("v2 audit completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
