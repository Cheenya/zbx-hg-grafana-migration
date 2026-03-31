from __future__ import annotations

import argparse
from datetime import datetime
from typing import TextIO

import config
from api_clients import ZabbixAPI
from common import build_artifact_path, normalize_scope_env, normalize_values, resolve_scope_org_pairs
from grafana_audit import collect_grafana_report
from mapping_plan import write_mapping_plan_xlsx
from report_writer import save_inventory_json, write_grafana_workbook, write_workbook
from zabbix_audit import build_scope_report


class AuditLogger:
    def __init__(self, path: str) -> None:
        self.path = path
        self._handle: TextIO = open(path, "w", encoding="utf-8")

    def log(self, message: str) -> None:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{stamp}] {message}"
        print(line, flush=True)
        self._handle.write(line + "\n")
        self._handle.flush()

    def close(self) -> None:
        self._handle.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only scoped audit (Zabbix + Grafana)")
    parser.add_argument("--out-xlsx", dest="out_xlsx", help="Path to XLSX report")
    parser.add_argument("--out-json", dest="out_json", help="Path to JSON inventory")
    parser.add_argument("--out-mapping", dest="out_mapping", help="Path to standalone mapping plan XLSX")
    parser.add_argument("--out-grafana", dest="out_grafana", help="Path to separate Grafana XLSX report")
    parser.add_argument("--out-log", dest="out_log", help="Path to audit log file")
    args = parser.parse_args()

    scope_as = normalize_values(config.SCOPE_AS)
    scope_env = normalize_scope_env(config.SCOPE_ENV)
    scope_gas = normalize_values(config.SCOPE_GAS)
    if not scope_as:
        raise RuntimeError("Scope is empty. Set SCOPE_AS in config.py.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_xlsx = build_artifact_path(config.OUTPUT_PREFIX, scope_as, scope_env, scope_gas, ".xlsx", timestamp=timestamp)
    default_json = build_artifact_path(config.OUTPUT_PREFIX, scope_as, scope_env, scope_gas, ".json", timestamp=timestamp)
    default_mapping = build_artifact_path(config.MAPPING_PLAN_PREFIX, scope_as, scope_env, scope_gas, ".xlsx", timestamp=timestamp)
    default_grafana = build_artifact_path(config.GRAFANA_REPORT_PREFIX, scope_as, scope_env, scope_gas, ".xlsx", timestamp=timestamp)
    default_log = build_artifact_path(config.AUDIT_LOG_PREFIX, scope_as, scope_env, scope_gas, ".log", timestamp=timestamp)
    out_xlsx = args.out_xlsx or default_xlsx
    out_json = args.out_json or default_json
    out_mapping = args.out_mapping or default_mapping
    out_grafana = args.out_grafana or default_grafana
    out_log = args.out_log or default_log

    logger = AuditLogger(out_log)
    logger.log(f"audit: started scope_as={scope_as} scope_env={scope_env or '(all)'} scope_gas={scope_gas or ['(all)']}")
    logger.log(f"audit: outputs xlsx={out_xlsx} json={out_json} mapping={out_mapping} grafana={out_grafana}")

    try:
        connection = config.load_zabbix_connection()
        logger.log(f"audit: zabbix_url={connection.api_url}")
        zabbix = ZabbixAPI(connection.api_url, timeout_sec=int(config.HTTP_TIMEOUT_SEC))
        logger.log("audit: logging in to zabbix")
        zabbix.login(connection.username, connection.password)
        logger.log("audit: zabbix login ok")

        logger.log("audit: running zabbix inventory")
        report = build_scope_report(zabbix, scope_as, scope_env, scope_gas, log=logger.log)
        report["summary"]["audit_log_path"] = out_log

        if config.ENABLE_GRAFANA:
            try:
                logger.log("audit: running grafana inventory")
                grafana = config.load_grafana_connection()
                scope_pairs = resolve_scope_org_pairs(scope_as, config.GRAFANA_ORGIDS)
                logger.log(f"audit: grafana_scope_pairs={scope_pairs}")
                grafana_report = collect_grafana_report(grafana, scope_pairs, report["inventory"]["grafana_old_groups"], log=logger.log)
                report["grafana"] = grafana_report["detail_rows"]
                report["grafana_summary"] = grafana_report["summary_rows"]
                report["summary"]["grafana_rows"] = len(report["grafana"])
                report["summary"]["grafana_dashboards"] = len(report["grafana_summary"])
            except Exception as exc:
                report["summary"]["grafana_error"] = str(exc)
                logger.log(f"audit: grafana error: {exc}")

        logger.log(f"audit: writing xlsx {out_xlsx}")
        write_workbook(report, out_xlsx)

        logger.log(f"audit: writing mapping plan {out_mapping}")
        write_mapping_plan_xlsx(report["mapping_plan"], out_mapping)

        if config.ENABLE_GRAFANA and ("grafana_error" not in report["summary"]):
            logger.log(f"audit: writing grafana xlsx {out_grafana}")
            write_grafana_workbook(report, out_grafana)

        if config.SAVE_JSON_INVENTORY:
            logger.log(f"audit: writing json {out_json}")
            save_inventory_json(report, out_json)

        logger.log(f"audit: completed summary={report['summary']}")
        return 0
    except Exception as exc:
        logger.log(f"audit: failed: {exc}")
        raise
    finally:
        logger.close()


if __name__ == "__main__":
    raise SystemExit(main())
