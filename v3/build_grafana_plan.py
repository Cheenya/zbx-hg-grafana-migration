from __future__ import annotations

import argparse
from datetime import datetime

import config
from core.common import build_org_artifact_path, normalize_values, resolve_input_artifact
from planning.grafana_plan import build_grafana_plan_from_impact, load_impact_plan, save_grafana_plan_json, write_grafana_plan_xlsx


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Grafana migration plan from impact plan")
    parser.add_argument("--out-xlsx", dest="out_xlsx", help="Path to Grafana plan XLSX")
    parser.add_argument("--out-json", dest="out_json", help="Path to Grafana plan JSON")
    args = parser.parse_args()

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

    impact_plan = load_impact_plan(impact_plan_path)
    org_ids = [
        int(value)
        for value in normalize_values(
            sorted(
                {
                    row.get("grafana_org_id")
                    for row in list(impact_plan.get("grafana_changes") or []) + list(impact_plan.get("grafana_manual_review") or [])
                    if str(row.get("grafana_org_id") or "").strip()
                }
            )
        )
    ]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = args.out_xlsx or build_org_artifact_path(config.GRAFANA_PLAN_PREFIX, org_ids, ".xlsx", timestamp=timestamp)
    out_json = args.out_json or build_org_artifact_path(config.GRAFANA_PLAN_PREFIX, org_ids, ".json", timestamp=timestamp)

    if not str(config.SOURCE_IMPACT_PLAN_JSON or "").strip():
        print(f"Using latest impact plan JSON: {impact_plan_path}")
    print(f"Building Grafana plan from impact plan: {impact_plan_path}")
    data = build_grafana_plan_from_impact(impact_plan, log=print)

    print(f"Writing Grafana plan XLSX: {out_xlsx}")
    write_grafana_plan_xlsx(data, out_xlsx)
    print(f"Writing Grafana plan JSON: {out_json}")
    save_grafana_plan_json(data, out_json)
    print("Grafana plan completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
