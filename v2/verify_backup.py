from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Sequence, Tuple

try:
    from . import config
    from .backup_io import load_backup
    from .common import normalize_values
except ImportError:
    import config  # type: ignore
    from backup_io import load_backup  # type: ignore
    from common import normalize_values  # type: ignore


def load_inventory(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _sorted_ids(values: Iterable[str]) -> List[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _compare(label: str, expected: Sequence[str], actual: Sequence[str]) -> Tuple[List[str], List[str]]:
    expected_ids = set(_sorted_ids(expected))
    actual_ids = set(_sorted_ids(actual))
    missing = sorted(expected_ids.difference(actual_ids))
    extra = sorted(actual_ids.difference(expected_ids))
    print(f"{label}: expected={len(expected_ids)} actual={len(actual_ids)} missing={len(missing)} extra={len(extra)}")
    if missing:
        print(f"  missing {label}: {', '.join(missing)}")
    if extra:
        print(f"  extra {label}: {', '.join(extra)}")
    return missing, extra


def main() -> int:
    inventory_path = str(config.SOURCE_INVENTORY_JSON or "").strip()
    backup_path = str(config.SOURCE_BACKUP_FILE or "").strip()
    if not inventory_path:
        raise RuntimeError("Set v2/config.py SOURCE_INVENTORY_JSON before verify.")
    if not backup_path:
        raise RuntimeError("Set v2/config.py SOURCE_BACKUP_FILE before verify.")

    raw_inventory = load_inventory(inventory_path)
    inventory = raw_inventory.get("inventory") or {}
    backup = load_backup(backup_path)

    failures: List[str] = []

    if _sorted_ids(backup.meta.scope_as) != _sorted_ids(inventory.get("scope_as") or []):
        failures.append("scope_as mismatch")
    if _sorted_ids(backup.meta.scope_envs) != _sorted_ids(inventory.get("scope_envs") or []):
        failures.append("scope_envs mismatch")

    checks = [
        ("hostgroups", [str(item.get("groupid") or "") for item in (inventory.get("hostgroups") or [])], [item.groupid for item in backup.hostgroups]),
        ("hosts", inventory.get("hostids") or [], [item.hostid for item in backup.hosts]),
        ("actions", inventory.get("actionids") or [], [item.actionid for item in backup.actions]),
        ("usergroups", inventory.get("usergroupids") or [], [item.usrgrpid for item in backup.usergroups]),
        ("users", inventory.get("userids") or [], [item.userid for item in backup.users]),
        ("maintenances", inventory.get("maintenanceids") or [], [item.maintenanceid for item in backup.maintenances]),
    ]

    for label, expected, actual in checks:
        missing, extra = _compare(label, expected, actual)
        if missing or extra:
            failures.append(label)

    if failures:
        raise RuntimeError(f"Backup verification failed: {', '.join(failures)}")

    print("Backup verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
