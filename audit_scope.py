#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""audit_scope.py — аудит по заданному списку AS (Zabbix + Grafana)."""

from __future__ import annotations

import argparse

from config import CONFIG
from scope_utils import build_scope_xlsx_path, normalize_scope
from zbx_hg_mapping_audit import run_audit


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit Zabbix/Grafana by AS list (from config)")
    parser.add_argument("--out", dest="output", help="XLSX output path")
    args = parser.parse_args()

    scope_as = normalize_scope(CONFIG.runtime.audit_scope_as)

    if not scope_as:
        raise RuntimeError("AS scope is empty. Set CONFIG.runtime.audit_scope_as in config.py.")

    if args.output:
        output_path = args.output
    else:
        output_path = build_scope_xlsx_path(CONFIG.excel.output_xlsx, scope_as)
    run_audit(as_filter=scope_as, output_xlsx=output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
