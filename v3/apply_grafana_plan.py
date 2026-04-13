from __future__ import annotations

import argparse
import json
from datetime import datetime

import config
from common import build_org_artifact_path, normalize_values, resolve_input_artifact
from grafana_plan import (
    apply_grafana_plan,
    get_selected_grafana_changes,
    load_grafana_plan_rows,
    load_impact_plan,
    write_grafana_apply_xlsx,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply Grafana migration plan")
    parser.add_argument("--out-xlsx", dest="out_xlsx", help="Path to Grafana apply XLSX")
    parser.add_argument("--out-json", dest="out_json", help="Path to Grafana apply JSON")
    args = parser.parse_args()

    plan_path = resolve_input_artifact(
        config.SOURCE_GRAFANA_PLAN_XLSX,
        config.GRAFANA_PLAN_PREFIX,
        ".xlsx",
        label="Grafana plan XLSX",
        strict_scope_match=False,
    )
    impact_plan_path = resolve_input_artifact(
        config.SOURCE_IMPACT_PLAN_JSON,
        config.IMPACT_PLAN_PREFIX,
        ".json",
        scope_as=config.SCOPE_AS,
        scope_env=config.SCOPE_ENV,
        scope_gas=config.SCOPE_GAS,
        label="impact plan JSON",
        strict_scope_match=True,
    )

    plan_rows = load_grafana_plan_rows(plan_path)
    selected_rows = get_selected_grafana_changes(plan_rows)
    impact_plan = load_impact_plan(impact_plan_path)
    grafana_org_ids = sorted(
        {
            int(str(row.get("grafana_org_id") or "").strip())
            for row in (impact_plan.get("grafana_changes") or [])
            if str(row.get("grafana_org_id") or "").strip()
        }
    )
    if not str(config.SOURCE_GRAFANA_PLAN_XLSX or "").strip():
        plan_path = resolve_input_artifact(
            config.SOURCE_GRAFANA_PLAN_XLSX,
            config.GRAFANA_PLAN_PREFIX,
            ".xlsx",
            org_ids=grafana_org_ids,
            label="Grafana plan XLSX",
            strict_scope_match=True,
        )
        plan_rows = load_grafana_plan_rows(plan_path)
        selected_rows = get_selected_grafana_changes(plan_rows)
    org_ids = [int(value) for value in normalize_values(sorted({row["grafana_org_id"] for row in selected_rows}))]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = args.out_xlsx or build_org_artifact_path(config.GRAFANA_APPLY_PREFIX, org_ids, ".xlsx", timestamp=timestamp)
    out_json = args.out_json or build_org_artifact_path(config.GRAFANA_APPLY_PREFIX, org_ids, ".json", timestamp=timestamp)

    dry_run = not bool(config.GRAFANA_APPLY_CHANGES)
    connection = config.load_grafana_connection()
    if not str(config.SOURCE_GRAFANA_PLAN_XLSX or "").strip():
        print(f"Using latest Grafana plan XLSX: {plan_path}")
    if not str(config.SOURCE_IMPACT_PLAN_JSON or "").strip():
        print(f"Using latest impact plan JSON: {impact_plan_path}")
    print(f"Applying Grafana plan from: {plan_path}")
    print(f"Validating against impact plan: {impact_plan_path}")
    print(f"Mode: {'DRY-RUN' if dry_run else 'APPLY'}")
    data = apply_grafana_plan(connection, selected_rows, impact_plan, dry_run=dry_run, log=print)

    print(f"Writing Grafana apply XLSX: {out_xlsx}")
    write_grafana_apply_xlsx(data, out_xlsx)
    print(f"Writing Grafana apply JSON: {out_json}")
    with open(out_json, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    print("Grafana apply completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
