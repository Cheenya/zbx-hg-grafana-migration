#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""artifact_paths.py — генерация имён служебных файлов отчёта."""

from __future__ import annotations


def _strip_xlsx_suffix(path: str) -> str:
    if path.lower().endswith(".xlsx"):
        return path[:-5]
    return path


def build_seed_path(output_xlsx: str) -> str:
    base = _strip_xlsx_suffix(output_xlsx)
    return f"{base}_zbx_seed.json"


def build_migration_plan_path(output_xlsx: str) -> str:
    base = _strip_xlsx_suffix(output_xlsx)
    return f"{base}_migration_plan.json"
