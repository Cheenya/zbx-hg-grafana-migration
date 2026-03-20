from __future__ import annotations

import argparse
from datetime import datetime
from typing import TextIO

import config
from common import build_org_artifact_path
from grafana_audit import collect_grafana_org_report
from report_writer import save_grafana_org_json, write_grafana_org_workbook


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
    parser = argparse.ArgumentParser(description="Grafana org audit for Zabbix datasources")
    parser.add_argument("--out-xlsx", dest="out_xlsx", help="Path to XLSX report")
    parser.add_argument("--out-json", dest="out_json", help="Path to JSON report")
    parser.add_argument("--out-log", dest="out_log", help="Path to log file")
    args = parser.parse_args()

    org_ids = [int(value) for value in config.GRAFANA_AUDIT_ORGIDS]
    if not org_ids:
        raise RuntimeError("Grafana org scope is empty. Set GRAFANA_AUDIT_ORGIDS in config.py.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = args.out_xlsx or build_org_artifact_path(config.GRAFANA_ORG_AUDIT_PREFIX, org_ids, ".xlsx", timestamp=timestamp)
    out_json = args.out_json or build_org_artifact_path(config.GRAFANA_ORG_AUDIT_PREFIX, org_ids, ".json", timestamp=timestamp)
    out_log = args.out_log or build_org_artifact_path(config.GRAFANA_ORG_AUDIT_LOG_PREFIX, org_ids, ".log", timestamp=timestamp)

    logger = AuditLogger(out_log)
    logger.log(f"grafana-org-audit: started org_ids={org_ids}")
    logger.log(f"grafana-org-audit: outputs xlsx={out_xlsx} json={out_json} log={out_log}")

    try:
        connection = config.load_grafana_connection()
        logger.log(f"grafana-org-audit: grafana_url={connection.base_url}")
        report = collect_grafana_org_report(connection, org_ids, log=logger.log)
        report["summary"]["audit_log_path"] = out_log

        logger.log(f"grafana-org-audit: writing xlsx {out_xlsx}")
        write_grafana_org_workbook(report, out_xlsx)

        logger.log(f"grafana-org-audit: writing json {out_json}")
        save_grafana_org_json(report, out_json)

        logger.log(f"grafana-org-audit: completed summary={report['summary']}")
        return 0
    except Exception as exc:
        logger.log(f"grafana-org-audit: failed: {exc}")
        raise
    finally:
        logger.close()


if __name__ == "__main__":
    raise SystemExit(main())
