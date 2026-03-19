from __future__ import annotations

import argparse

from api_clients import ZabbixAPI
from config import load_connection_from_env_or_prompt, load_grafana_from_module

from . import settings
from .common import build_output_paths, normalize_values
from .grafana_audit import collect_grafana_rows
from .report_writer import save_inventory_json, write_workbook
from .zabbix_audit import build_scope_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only scoped audit v2 (Zabbix + Grafana)")
    parser.add_argument("--out-xlsx", dest="out_xlsx", help="Path to XLSX report")
    parser.add_argument("--out-json", dest="out_json", help="Path to JSON inventory")
    args = parser.parse_args()

    scope_as = normalize_values(settings.SCOPE_AS)
    scope_envs = normalize_values(settings.SCOPE_ENVS)
    if not scope_as:
        raise RuntimeError("v2 scope is empty. Set v2/settings.py SCOPE_AS or CONFIG.runtime.audit_scope_as.")

    default_xlsx, default_json = build_output_paths(scope_as, scope_envs)
    out_xlsx = args.out_xlsx or default_xlsx
    out_json = args.out_json or default_json

    connection = load_connection_from_env_or_prompt(interactive=False)
    zabbix = ZabbixAPI(connection.api_url, timeout_sec=90)
    zabbix.login(connection.username, connection.password)

    print("Running Zabbix inventory (v2)...")
    report = build_scope_report(zabbix, scope_as, scope_envs)

    if settings.ENABLE_GRAFANA:
        try:
            print("Running Grafana inventory (v2)...")
            grafana = load_grafana_from_module()
            report["grafana"] = collect_grafana_rows(grafana, scope_as, report["inventory"]["hostgroups"])
            report["summary"]["grafana_rows"] = len(report["grafana"])
        except Exception as exc:
            report["summary"]["grafana_error"] = str(exc)

    print(f"Writing XLSX: {out_xlsx}")
    write_workbook(report, out_xlsx)

    if settings.SAVE_JSON_INVENTORY:
        print(f"Writing JSON: {out_json}")
        save_inventory_json(report, out_json)

    print("v2 audit completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
