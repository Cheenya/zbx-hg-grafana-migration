from __future__ import annotations

import gzip
import json
from dataclasses import asdict

from backup_model import (
    ActionBackup,
    BackupData,
    BackupMeta,
    HostBackup,
    HostGroupBackup,
    MaintenanceBackup,
    UserBackup,
    UserGroupBackup,
)


def save_backup(data: BackupData, path: str) -> None:
    payload = asdict(data)
    if path.lower().endswith(".gz"):
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def load_backup(path: str) -> BackupData:
    if path.lower().endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            raw = json.load(handle)
    else:
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)

    meta_raw = raw.get("meta") or {}
    if "scope_env" not in meta_raw:
        scope_envs = meta_raw.get("scope_envs") or []
        if isinstance(scope_envs, list) and scope_envs:
            meta_raw["scope_env"] = str(scope_envs[0] or "")
        else:
            meta_raw["scope_env"] = ""

    return BackupData(
        meta=BackupMeta(**meta_raw),
        impact_plan=raw.get("impact_plan") or raw.get("inventory") or {},
        hostgroups=[HostGroupBackup(**item) for item in (raw.get("hostgroups") or [])],
        hosts=[HostBackup(**item) for item in (raw.get("hosts") or [])],
        actions=[ActionBackup(**item) for item in (raw.get("actions") or [])],
        usergroups=[UserGroupBackup(**item) for item in (raw.get("usergroups") or [])],
        users=[UserBackup(**item) for item in (raw.get("users") or [])],
        maintenances=[MaintenanceBackup(**item) for item in (raw.get("maintenances") or [])],
    )
