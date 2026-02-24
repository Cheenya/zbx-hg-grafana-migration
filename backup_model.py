#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""backup_model.py — модели бэкапа Zabbix для быстрого отката."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class BackupMeta:
    created_at: str
    scope_as: List[str] = field(default_factory=list)
    zabbix_url: str = ""
    version: str = "1.0"


@dataclass
class HostBackup:
    hostid: str
    groups: List[Dict[str, Any]] = field(default_factory=list)
    tags: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ActionBackup:
    actionid: str
    raw: Dict[str, Any] = field(default_factory=dict)
    filter: Dict[str, Any] = field(default_factory=dict)
    operations: List[Dict[str, Any]] = field(default_factory=list)
    recovery_operations: List[Dict[str, Any]] = field(default_factory=list)
    update_operations: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class UserGroupBackup:
    usrgrpid: str
    name: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)
    users: List[Dict[str, Any]] = field(default_factory=list)
    hostgroup_rights: List[Dict[str, Any]] = field(default_factory=list)
    tag_filters: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class UserBackup:
    userid: str
    username: str = ""
    name: str = ""
    surname: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)
    medias: List[Dict[str, Any]] = field(default_factory=list)
    usrgrps: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class BackupData:
    meta: BackupMeta
    hosts: List[HostBackup] = field(default_factory=list)
    actions: List[ActionBackup] = field(default_factory=list)
    usergroups: List[UserGroupBackup] = field(default_factory=list)
    users: List[UserBackup] = field(default_factory=list)
