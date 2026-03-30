from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import config
from backup_io import load_backup
from common import resolve_input_artifact


def load_impact_plan(path: str) -> Dict[str, Any]:
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
    impact_plan_path = resolve_input_artifact(
        config.SOURCE_IMPACT_PLAN_JSON,
        config.IMPACT_PLAN_PREFIX,
        ".json",
        scope_as=config.SCOPE_AS,
        scope_env=config.SCOPE_ENV,
        label="impact plan JSON",
    )
    backup_path = resolve_input_artifact(
        config.SOURCE_BACKUP_FILE,
        config.BACKUP_PREFIX,
        ".json.gz",
        scope_as=config.SCOPE_AS,
        scope_env=config.SCOPE_ENV,
        label="backup file",
    )

    if not str(config.SOURCE_IMPACT_PLAN_JSON or "").strip():
        print(f"Using latest impact plan JSON: {impact_plan_path}")
    if not str(config.SOURCE_BACKUP_FILE or "").strip():
        print(f"Using latest backup file: {backup_path}")

    raw_impact_plan = load_impact_plan(impact_plan_path)
    summary = raw_impact_plan.get("summary") or {}
    backup_scope = raw_impact_plan.get("backup_scope") or {}
    backup = load_backup(backup_path)

    failures: List[str] = []
    summary_scope_env = str(summary.get("scope_env") or "").strip()
    if not summary_scope_env:
        legacy_scope_envs = summary.get("scope_envs") or []
        if legacy_scope_envs:
            summary_scope_env = str(legacy_scope_envs[0] or "").strip()

    if _sorted_ids(backup.meta.scope_as) != _sorted_ids(summary.get("scope_as") or []):
        failures.append("scope_as mismatch")
    if str(backup.meta.scope_env or "").strip() != summary_scope_env:
        failures.append("scope_env mismatch")

    checks = [
        ("hostgroups", [str(item.get("groupid") or "") for item in (backup_scope.get("hostgroups") or [])], [item.groupid for item in backup.hostgroups]),
        ("hosts", backup_scope.get("hostids") or [], [item.hostid for item in backup.hosts]),
        ("actions", backup_scope.get("actionids") or [], [item.actionid for item in backup.actions]),
        ("usergroups", backup_scope.get("usergroupids") or [], [item.usrgrpid for item in backup.usergroups]),
        ("users", backup_scope.get("userids") or [], [item.userid for item in backup.users]),
        ("maintenances", backup_scope.get("maintenanceids") or [], [item.maintenanceid for item in backup.maintenances]),
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
