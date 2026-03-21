from __future__ import annotations

import argparse
from datetime import datetime

import config
from common import build_org_artifact_path, normalize_values
from grafana_plan import build_grafana_plan, load_grafana_org_report, save_grafana_plan_json, write_grafana_plan_xlsx
from mapping_plan import get_selected_mappings, load_mapping_plan_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Grafana variable migration plan from org audit JSON and selected mapping plan")
    parser.add_argument("--out-xlsx", dest="out_xlsx", help="Path to Grafana plan XLSX")
    parser.add_argument("--out-json", dest="out_json", help="Path to Grafana plan JSON")
    args = parser.parse_args()

    org_audit_json_path = str(config.SOURCE_GRAFANA_ORG_JSON or "").strip()
    mapping_plan_path = str(config.SOURCE_MAPPING_PLAN_XLSX or "").strip()
    if not org_audit_json_path:
        raise RuntimeError("Set SOURCE_GRAFANA_ORG_JSON in config.py before building Grafana plan.")
    if not mapping_plan_path:
        raise RuntimeError("Set SOURCE_MAPPING_PLAN_XLSX in config.py before building Grafana plan.")

    org_report = load_grafana_org_report(org_audit_json_path)
    mapping_rows = load_mapping_plan_rows(mapping_plan_path)
    selected_mappings = get_selected_mappings(mapping_rows)

    org_ids = [int(value) for value in normalize_values((org_report.get("summary") or {}).get("grafana_org_ids") or [])]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = args.out_xlsx or build_org_artifact_path(config.GRAFANA_PLAN_PREFIX, org_ids, ".xlsx", timestamp=timestamp)
    out_json = args.out_json or build_org_artifact_path(config.GRAFANA_PLAN_PREFIX, org_ids, ".json", timestamp=timestamp)

    connection = config.load_grafana_connection()
    print(f"Building Grafana plan from: {org_audit_json_path}")
    print(f"Using selected mappings from: {mapping_plan_path}")
    data = build_grafana_plan(connection, org_report, selected_mappings, log=print)

    print(f"Writing Grafana plan XLSX: {out_xlsx}")
    write_grafana_plan_xlsx(data, out_xlsx)
    print(f"Writing Grafana plan JSON: {out_json}")
    save_grafana_plan_json(data, out_json)
    print("Grafana plan completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
