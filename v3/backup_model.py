from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class BackupMeta:
    created_at: str
    version: str = "2.0"
    impact_plan_path: str = ""
    zabbix_url: str = ""
    scope_as: List[str] = field(default_factory=list)
    scope_env: str = ""
    scope_gas: List[str] = field(default_factory=list)


@dataclass
class HostGroupBackup:
    groupid: str
    name: str = ""
    kind: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HostBackup:
    hostid: str
    raw: Dict[str, Any] = field(default_factory=dict)
    groups: List[Dict[str, Any]] = field(default_factory=list)
    tags: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ActionBackup:
    actionid: str
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UserGroupBackup:
    usrgrpid: str
    name: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UserBackup:
    userid: str
    username: str = ""
    name: str = ""
    surname: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MaintenanceBackup:
    maintenanceid: str
    name: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BackupData:
    meta: BackupMeta
    impact_plan: Dict[str, Any] = field(default_factory=dict)
    hostgroups: List[HostGroupBackup] = field(default_factory=list)
    hosts: List[HostBackup] = field(default_factory=list)
    actions: List[ActionBackup] = field(default_factory=list)
    usergroups: List[UserGroupBackup] = field(default_factory=list)
    users: List[UserBackup] = field(default_factory=list)
    maintenances: List[MaintenanceBackup] = field(default_factory=list)
