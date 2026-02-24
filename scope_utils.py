#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""scope_utils.py — утилиты для scope-имён и путей файлов."""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Iterable, List, Optional


def normalize_scope(scope_as: Optional[Iterable[str]]) -> List[str]:
    """Возвращает очищенный список AS без пустых значений."""
    out: List[str] = []
    for x in scope_as or []:
        s = str(x).strip()
        if s:
            out.append(s)
    return out


def build_scope_part(scope_as: Optional[Iterable[str]]) -> str:
    """Формирует компактный суффикс scope для имени файла."""
    scope_list = normalize_scope(scope_as)
    if not scope_list:
        return "ALL"
    if len(scope_list) <= 3:
        safe = [re.sub(r"[^A-Za-z0-9_-]", "_", x) for x in scope_list]
        return "-".join(safe)
    return f"MULTI{len(scope_list)}"


def build_scope_xlsx_path(base_xlsx: str, scope_as: Optional[Iterable[str]]) -> str:
    """Строит путь вида <base>_scope_<scope>_<timestamp>.xlsx."""
    base_dir = os.path.dirname(base_xlsx) or "."
    base_name = os.path.splitext(os.path.basename(base_xlsx))[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    scope_part = build_scope_part(scope_as)
    filename = f"{base_name}_scope_{scope_part}_{timestamp}.xlsx"
    return os.path.join(base_dir, filename)


def build_scope_backup_path(base_path: str, scope_as: Optional[Iterable[str]]) -> str:
    """Строит путь бэкапа вида zbx_backup_<scope>_<timestamp>.json.gz."""
    base_dir = os.path.dirname(base_path) or "."
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    scope_part = build_scope_part(scope_as)
    filename = f"zbx_backup_{scope_part}_{timestamp}.json.gz"
    return os.path.join(base_dir, filename)
