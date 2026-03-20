#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""api_clients.py — HTTP-клиенты только для v2."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import urllib3  # type: ignore

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests  # type: ignore


class ZabbixAPI:
    """Минимальный JSON-RPC клиент для Zabbix 7.0+."""

    def __init__(self, api_url: str, timeout_sec: int = 60) -> None:
        self.api_url = api_url
        self.timeout = int(timeout_sec)
        self.auth: Optional[str] = None
        self._id = 1

    def call(self, method: str, params: Dict[str, Any]) -> Any:
        payload: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._id,
        }
        self._id += 1
        if self.auth is not None:
            payload["auth"] = self.auth

        try:
            response = requests.post(self.api_url, json=payload, timeout=self.timeout, verify=False)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            raise RuntimeError(f"Zabbix API error ({method}): {exc}") from exc

        if "error" in data:
            raise RuntimeError(f"Zabbix API error ({method}): {data['error']}")
        return data["result"]

    def login(self, username: str, password: str) -> None:
        self.auth = self.call("user.login", {"username": username, "password": password})


class GrafanaAPI:
    """Минимальный HTTP клиент для Grafana API."""

    def __init__(
        self,
        base_url: str,
        username: str = "",
        password: str = "",
        token: str = "",
        timeout_sec: int = 60,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = int(timeout_sec)
        self.session = requests.Session()
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        if username or password:
            self.session.auth = (username, password)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=payload,
                timeout=self.timeout,
                verify=False,
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            raise RuntimeError(f"Grafana API error ({method} {path}): {exc}") from exc

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params)

    def post(self, path: str, payload: Dict[str, Any]) -> Any:
        return self._request("POST", path, payload=payload)

    def list_dashboards(self) -> List[Dict[str, Any]]:
        dashboards: List[Dict[str, Any]] = []
        page = 1
        limit = 500
        while True:
            chunk = self.get("/api/search", params={"type": "dash-db", "limit": limit, "page": page})
            if not chunk:
                break
            dashboards.extend(chunk)
            if len(chunk) < limit:
                break
            page += 1
        return dashboards

    def get_dashboard_by_uid(self, uid: str) -> Dict[str, Any]:
        return self.get(f"/api/dashboards/uid/{uid}")

    def update_dashboard(self, dash_json: Dict[str, Any], folder_id: int, message: str) -> Any:
        payload = {"dashboard": dash_json, "folderId": folder_id, "message": message, "overwrite": True}
        return self.post("/api/dashboards/db", payload)
