#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""audit_scope.py — аудит по заданному списку AS (Zabbix + Grafana)."""

from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from typing import Iterable, List

from config import CONFIG
from zbx_hg_mapping_audit import run_audit


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit Zabbix/Grafana by AS list (from config)")
    parser.add_argument("--out", dest="output", help="XLSX output path")
    args = parser.parse_args()

    scope_as = list(CONFIG.runtime.audit_scope_as) if CONFIG.runtime.audit_scope_as else []

    if not scope_as:
        raise RuntimeError("AS scope is empty. Set CONFIG.runtime.audit_scope_as in config.py.")

    if args.output:
        output_path = args.output
    else:
        base_dir = os.path.dirname(CONFIG.excel.output_xlsx) or "."
        base_name = os.path.splitext(os.path.basename(CONFIG.excel.output_xlsx))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        scope_list = [str(x).strip() for x in scope_as if str(x).strip()]
        if not scope_list:
            scope_part = "ALL"
        elif len(scope_list) <= 3:
            safe = [re.sub(r"[^A-Za-z0-9_-]", "_", x) for x in scope_list]
            scope_part = "-".join(safe)
        else:
            scope_part = f"MULTI{len(scope_list)}"
        filename = f"{base_name}_scope_{scope_part}_{timestamp}.xlsx"
        output_path = os.path.join(base_dir, filename)
    run_audit(as_filter=scope_as, output_xlsx=output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
