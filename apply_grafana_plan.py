from __future__ import annotations

import argparse
import json
from datetime import datetime

import config
from common import build_org_artifact_path, normalize_values
from grafana_plan import apply_grafana_plan, get_selected_grafana_changes, load_grafana_plan_rows, write_grafana_apply_xlsx


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply Grafana variable migration plan")
    parser.add_argument("--out-xlsx", dest="out_xlsx", help="Path to Grafana apply XLSX")
    parser.add_argument("--out-json", dest="out_json", help="Path to Grafana apply JSON")
    args = parser.parse_args()

    plan_path = str(config.SOURCE_GRAFANA_PLAN_XLSX or "").strip()
    if not plan_path:
        raise RuntimeError("Set SOURCE_GRAFANA_PLAN_XLSX in config.py before applying Grafana plan.")

    plan_rows = load_grafana_plan_rows(plan_path)
    selected_rows = get_selected_grafana_changes(plan_rows)
    org_ids = [int(value) for value in normalize_values(sorted({row["grafana_org_id"] for row in selected_rows}))]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = args.out_xlsx or build_org_artifact_path(config.GRAFANA_APPLY_PREFIX, org_ids, ".xlsx", timestamp=timestamp)
    out_json = args.out_json or build_org_artifact_path(config.GRAFANA_APPLY_PREFIX, org_ids, ".json", timestamp=timestamp)

    dry_run = not bool(config.GRAFANA_APPLY_CHANGES)
    connection = config.load_grafana_connection()
    print(f"Applying Grafana plan from: {plan_path}")
    print(f"Mode: {'DRY-RUN' if dry_run else 'APPLY'}")
    data = apply_grafana_plan(connection, selected_rows, dry_run=dry_run, log=print)

    print(f"Writing Grafana apply XLSX: {out_xlsx}")
    write_grafana_apply_xlsx(data, out_xlsx)
    print(f"Writing Grafana apply JSON: {out_json}")
    with open(out_json, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    print("Grafana apply completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
