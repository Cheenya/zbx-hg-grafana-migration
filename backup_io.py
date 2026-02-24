#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""backup_io.py — сохранение/загрузка бэкапа (JSON или .json.gz)."""

from __future__ import annotations

import gzip
import json
from dataclasses import asdict
from typing import Any, Dict

from backup_model import (
    ActionBackup,
    BackupData,
    BackupMeta,
    HostBackup,
    UserBackup,
    UserGroupBackup,
)


def save_backup(data: BackupData, path: str) -> None:
    payload = asdict(data)
    if path.lower().endswith(".gz"):
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)


def load_backup(path: str) -> BackupData:
    if path.lower().endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            raw = json.load(f)
    else:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

    meta = BackupMeta(**(raw.get("meta") or {}))
    hosts = [HostBackup(**x) for x in (raw.get("hosts") or [])]
    actions = [ActionBackup(**x) for x in (raw.get("actions") or [])]
    usergroups = [UserGroupBackup(**x) for x in (raw.get("usergroups") or [])]
    users = [UserBackup(**x) for x in (raw.get("users") or [])]

    return BackupData(meta=meta, hosts=hosts, actions=actions, usergroups=usergroups, users=users)
