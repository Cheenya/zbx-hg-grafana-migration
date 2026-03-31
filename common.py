from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from openpyxl.utils import get_column_letter  # type: ignore

import config


EXCLUDED_GROUP_PATTERNS = [re.compile(pattern) for pattern in config.EXCLUDED_GROUP_PATTERNS]
OLD_GROUP_RX = re.compile(r"^(BNK|DOM)-", re.IGNORECASE)
STANDARD_GROUP_RX = re.compile(r"^(BNK|DOM)/(ENV|AS|GAS|OS)(?:/(.+))?$", re.IGNORECASE)


def normalize_values(values: Optional[Iterable[str]]) -> List[str]:
    out: List[str] = []
    for value in values or []:
        item = str(value).strip()
        if item:
            out.append(item)
    return out


def normalize_lower_set(values: Optional[Iterable[str]]) -> Set[str]:
    return {item.lower() for item in normalize_values(values)}


def canonical_env_value(value: Optional[str]) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    upper = text.upper()
    if upper in {item.upper() for item in config.PROD_ENV_VALUES}:
        return config.ENV_PROD_LABEL
    return config.ENV_NONPROD_LABEL


def normalize_scope_env(value: Optional[str]) -> str:
    return canonical_env_value(value)


def normalize_upper_tag_value(value: Optional[str]) -> str:
    return str(value or "").strip().upper()


def get_tag_value(tags: Sequence[Dict[str, Any]], tag_name: str) -> Optional[str]:
    for tag in tags or []:
        if str(tag.get("tag") or "") != tag_name:
            continue
        value = tag.get("value")
        if value is None:
            return None
        text = str(value).strip()
        return text or None
    return None


def is_excluded_group(name: str) -> bool:
    if not name:
        return False
    return any(pattern.search(name) for pattern in EXCLUDED_GROUP_PATTERNS)


def is_old_group(name: str) -> bool:
    text = str(name or "")
    return "/" not in text and OLD_GROUP_RX.match(text) is not None


def old_group_org(name: str) -> str:
    match = OLD_GROUP_RX.match(str(name or "").strip())
    return str(match.group(1) or "").upper() if match else ""


def parse_standard_group(name: str) -> Optional[Dict[str, str]]:
    match = STANDARD_GROUP_RX.match(str(name or "").strip())
    if not match:
        return None

    org = str(match.group(1) or "").upper()
    root_kind = str(match.group(2) or "").upper()
    raw_tail = str(match.group(3) or "").strip()
    parts = [part.strip() for part in raw_tail.split("/") if part.strip()]

    out: Dict[str, str] = {
        "org": org,
        "root_kind": root_kind,
        "group_kind": root_kind,
        "name": str(name or "").strip(),
        "as_value": "",
        "env_raw": "",
        "gas_value": "",
        "os_family": "",
    }

    if root_kind == "ENV":
        if parts:
            out["env_raw"] = parts[0]
        return out

    if root_kind == "AS":
        if parts:
            out["as_value"] = parts[0]
        if len(parts) >= 2:
            out["env_raw"] = parts[1]
            out["group_kind"] = "AS_ENV"
        return out

    if root_kind == "GAS":
        if parts:
            out["gas_value"] = parts[0]
        if len(parts) >= 2:
            out["env_raw"] = parts[1]
            out["group_kind"] = "GAS_ENV"
        if len(parts) >= 3:
            out["as_value"] = parts[1]
            out["env_raw"] = parts[2]
            out["group_kind"] = "GAS_AS_ENV"
        return out

    if root_kind == "OS":
        if parts:
            out["os_family"] = parts[0]
        if len(parts) >= 2:
            out["env_raw"] = parts[1]
            out["group_kind"] = "OS_ENV"
        return out

    return out


def is_standard_group(name: str) -> bool:
    return parse_standard_group(name) is not None


def is_new_group_for_as(name: str, as_value: str) -> bool:
    parsed = parse_standard_group(name)
    if not parsed:
        return False
    if parsed.get("root_kind") != "AS":
        return False
    return str(parsed.get("as_value") or "").strip().lower() == str(as_value or "").strip().lower()


def standard_group_lookup_key(name: str) -> str:
    return str(name or "").strip().lower()


def standard_group_org(name: str) -> str:
    parsed = parse_standard_group(name)
    return str(parsed.get("org") or "") if parsed else ""


def resolve_org_by_domain_values(host_values: Sequence[str]) -> Tuple[str, List[str]]:
    domain_matches: Set[str] = set()
    for raw_value in host_values:
        text = str(raw_value or "").strip().lower()
        if not text:
            continue
        for org_code, suffixes in getattr(config, "ORG_DOMAIN_SUFFIXES", {}).items():
            normalized_org = normalize_upper_tag_value(org_code)
            for suffix in suffixes or ():
                normalized_suffix = str(suffix or "").strip().lower()
                if not normalized_org or not normalized_suffix:
                    continue
                if text == normalized_suffix or text.endswith(f".{normalized_suffix}"):
                    domain_matches.add(normalized_org)

    if len(domain_matches) == 1:
        return next(iter(domain_matches)), []
    if len(domain_matches) > 1:
        return "", [f"ORG unresolved: multiple domain matches ({join_sorted(domain_matches)})"]
    return "", []


def resolve_host_org(host_values: Sequence[str], group_names: Sequence[str]) -> Tuple[str, List[str]]:
    domain_org, domain_reasons = resolve_org_by_domain_values(host_values)
    if domain_org:
        return domain_org, domain_reasons
    if domain_reasons:
        return "", domain_reasons

    candidates: Set[str] = set()
    for group_name in group_names:
        name = str(group_name or "").strip()
        if not name:
            continue
        if is_old_group(name):
            org = old_group_org(name)
            if org:
                candidates.add(org)
            continue
        parsed = parse_standard_group(name)
        if parsed and parsed.get("org"):
            candidates.add(str(parsed["org"]))

    if len(candidates) == 1:
        return next(iter(candidates)), []
    if not candidates:
        return "", ["ORG unresolved: no BNK/DOM groups"]
    return "", [f"ORG unresolved: multiple values ({join_sorted(candidates)})"]


def resolve_os_family(guest_name: Optional[str]) -> str:
    text = str(guest_name or "").strip().lower()
    if not text:
        return ""
    if "windows" in text:
        return "WINDOWS"
    if "linux" in text:
        return "LINUX"
    return ""


def extract_legacy_env_token(group_name: str) -> str:
    allowed = {normalize_upper_tag_value(item) for item in getattr(config, "LEGACY_ENV_TOKENS", ()) if normalize_upper_tag_value(item)}
    if not allowed:
        return ""
    tokens = [normalize_upper_tag_value(part) for part in str(group_name or "").split("-")]
    for token in reversed(tokens):
        if token in allowed:
            return token
    return ""


def build_expected_hostgroups(
    org: str,
    as_value: Optional[str],
    env_raw: Optional[str],
    env_scope: Optional[str],
    gas_value: Optional[str],
    guest_name: Optional[str],
) -> List[Dict[str, str]]:
    org_value = str(org or "").strip().upper()
    as_upper = normalize_upper_tag_value(as_value)
    env_upper = normalize_upper_tag_value(env_raw)
    gas_upper = normalize_upper_tag_value(gas_value)
    os_family = resolve_os_family(guest_name)
    scope_value = str(env_scope or "").strip()

    if not org_value:
        return []

    rows: List[Dict[str, str]] = []
    seen: Set[str] = set()

    def add_group(group_kind: str, group_name: str) -> None:
        name = str(group_name or "").strip()
        if not name:
            return
        key = standard_group_lookup_key(name)
        if key in seen:
            return
        seen.add(key)
        rows.append(
            {
                "group_name": name,
                "group_kind": group_kind,
                "org": org_value,
                "as_value": as_upper,
                "env_raw": env_upper,
                "env_scope": scope_value,
                "gas_value": gas_upper,
                "os_family": os_family,
            }
        )

    if env_upper:
        add_group("ENV", f"{org_value}/ENV/{env_upper}")

    if as_upper:
        add_group("AS", f"{org_value}/AS/{as_upper}")
        if env_upper:
            add_group("AS_ENV", f"{org_value}/AS/{as_upper}/{env_upper}")

    if gas_upper:
        add_group("GAS", f"{org_value}/GAS/{gas_upper}")
        if env_upper:
            add_group("GAS_ENV", f"{org_value}/GAS/{gas_upper}/{env_upper}")
        if as_upper and env_upper:
            add_group("GAS_AS_ENV", f"{org_value}/GAS/{gas_upper}/{as_upper}/{env_upper}")

    if os_family:
        add_group("OS", f"{org_value}/OS/{os_family}")
        if env_upper:
            add_group("OS_ENV", f"{org_value}/OS/{os_family}/{env_upper}")

    return rows


def safe_sheet_title(raw: str, max_len: int = 31) -> str:
    text = re.sub(r"[\[\]\*:/\\\?]", "_", str(raw or "")).strip()
    if not text:
        return "SHEET"
    return text[:max_len]


def autosize_columns(ws, min_width: int = 10, max_width: int = 80) -> None:
    for column in ws.columns:
        max_len = 0
        letter = get_column_letter(column[0].column)
        for cell in column:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[letter].width = max(min_width, min(max_width, max_len + 2))


def build_scope_part(scope_as: Sequence[str], scope_env: str, scope_gas: Sequence[str] | None = None) -> str:
    scope_chunks: List[str] = []
    as_values = normalize_values(scope_as)
    env_value = str(scope_env or "").strip()
    gas_values = normalize_values(scope_gas or [])

    if as_values:
        if len(as_values) <= 3:
            scope_chunks.append("-".join(re.sub(r"[^A-Za-z0-9_-]", "_", item) for item in as_values))
        else:
            scope_chunks.append(f"AS{len(as_values)}")
    else:
        scope_chunks.append("NOAS")

    if env_value:
        scope_chunks.append(re.sub(r"[^A-Za-z0-9_-]", "_", env_value))

    if gas_values:
        if len(gas_values) <= 3:
            scope_chunks.append("gas-" + "-".join(re.sub(r"[^A-Za-z0-9_-]", "_", item) for item in gas_values))
        else:
            scope_chunks.append(f"GAS{len(gas_values)}")

    return "_".join(scope_chunks)


def build_artifact_path(
    prefix: str,
    scope_as: Sequence[str],
    scope_env: str,
    scope_gas: Sequence[str] | None,
    extension: str,
    timestamp: Optional[str] = None,
) -> str:
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    scope_part = build_scope_part(scope_as, scope_env, scope_gas)
    ext = str(extension or "").strip()
    if ext and not ext.startswith("."):
        ext = f".{ext}"
    return os.path.join(config.OUTPUT_DIR, f"{prefix}_{scope_part}_{stamp}{ext}")


def build_org_scope_part(org_ids: Sequence[int]) -> str:
    normalized = [str(int(item)) for item in org_ids]
    if not normalized:
        return "NOORG"
    if len(normalized) <= 3:
        return "-".join(f"org{item}" for item in normalized)
    return f"ORG{len(normalized)}"


def build_org_artifact_path(
    prefix: str,
    org_ids: Sequence[int],
    extension: str,
    timestamp: Optional[str] = None,
) -> str:
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    scope_part = build_org_scope_part(org_ids)
    ext = str(extension or "").strip()
    if ext and not ext.startswith("."):
        ext = f".{ext}"
    return os.path.join(config.OUTPUT_DIR, f"{prefix}_{scope_part}_{stamp}{ext}")


def build_output_paths(scope_as: Sequence[str], scope_env: str, scope_gas: Sequence[str] | None = None) -> Tuple[str, str]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (
        build_artifact_path(config.OUTPUT_PREFIX, scope_as, scope_env, scope_gas, ".xlsx", timestamp=timestamp),
        build_artifact_path(config.OUTPUT_PREFIX, scope_as, scope_env, scope_gas, ".json", timestamp=timestamp),
    )


def resolve_input_artifact(
    configured_path: str,
    prefix: str,
    extension: str,
    *,
    scope_as: Sequence[str] | None = None,
    scope_env: str = "",
    scope_gas: Sequence[str] | None = None,
    org_ids: Sequence[int] | None = None,
    label: str = "artifact",
) -> str:
    explicit = str(configured_path or "").strip()
    if explicit:
        return explicit

    ext = str(extension or "").strip()
    if ext and not ext.startswith("."):
        ext = f".{ext}"

    if not os.path.isdir(config.OUTPUT_DIR):
        raise RuntimeError(f"{label} not set and output directory does not exist: {config.OUTPUT_DIR}")

    candidates: List[Tuple[float, str, str]] = []
    for entry in os.scandir(config.OUTPUT_DIR):
        if not entry.is_file():
            continue
        name = entry.name
        if not name.startswith(f"{prefix}_"):
            continue
        if ext and not name.endswith(ext):
            continue
        candidates.append((entry.stat().st_mtime, entry.path, name))

    if not candidates:
        raise RuntimeError(f"{label} not set and no files found for prefix '{prefix}' in {config.OUTPUT_DIR}")

    preferred_stem = ""
    normalized_org_ids = [int(item) for item in (org_ids or [])]
    normalized_scope_as = normalize_values(scope_as)
    normalized_scope_env = str(scope_env or "").strip()
    normalized_scope_gas = normalize_values(scope_gas or [])

    if normalized_org_ids:
        preferred_stem = f"{prefix}_{build_org_scope_part(normalized_org_ids)}_"
    elif normalized_scope_as or normalized_scope_env or normalized_scope_gas:
        preferred_stem = f"{prefix}_{build_scope_part(normalized_scope_as, normalized_scope_env, normalized_scope_gas)}_"

    selected_pool = candidates
    if preferred_stem:
        scoped_candidates = [item for item in candidates if item[2].startswith(preferred_stem)]
        if scoped_candidates:
            selected_pool = scoped_candidates

    selected_pool.sort(key=lambda item: (item[0], item[2]), reverse=True)
    return selected_pool[0][1]


def resolve_scope_org_pairs(scope_as: Sequence[str], orgids: Sequence[int]) -> List[Tuple[str, int]]:
    as_values = normalize_values(scope_as)
    normalized_orgids = [int(value) for value in orgids]
    if not as_values:
        return []
    if not normalized_orgids:
        return [(as_value, 0) for as_value in as_values]
    if len(normalized_orgids) == 1:
        return [(as_value, normalized_orgids[0]) for as_value in as_values]
    if len(normalized_orgids) != len(as_values):
        raise RuntimeError("config.py GRAFANA_ORGIDS must be empty, one value, or match SCOPE_AS length.")
    return list(zip(as_values, normalized_orgids))


def join_sorted(values: Iterable[Any]) -> str:
    normalized = sorted({str(value).strip() for value in values if str(value).strip()})
    return ", ".join(normalized)


def sample_host_names(host_names: Iterable[str], limit: int) -> str:
    items = sorted({str(name).strip() for name in host_names if str(name).strip()})
    return ", ".join(items[:limit])


def resolve_tagfilter_tag(tag_filter: Dict[str, Any]) -> Optional[str]:
    return tag_filter.get("tag") or tag_filter.get("tag_name") or tag_filter.get("tagname")


def resolve_tagfilter_value(tag_filter: Dict[str, Any]) -> Optional[str]:
    value = tag_filter.get("value")
    if value is None:
        value = tag_filter.get("tagvalue")
    if value is None:
        value = tag_filter.get("val")
    return str(value) if value is not None else None


def collect_groupids(node: Any, hits: Set[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "groupid" and value is not None:
                hits.add(str(value))
            else:
                collect_groupids(value, hits)
        return
    if isinstance(node, list):
        for item in node:
            collect_groupids(item, hits)


def extract_action_groupids(action: Dict[str, Any]) -> Tuple[Set[str], Set[str]]:
    condition_ids: Set[str] = set()
    operation_ids: Set[str] = set()

    for condition in (action.get("filter") or {}).get("conditions") or []:
        if str(condition.get("conditiontype")) != "0":
            continue
        value = condition.get("value")
        if value is not None:
            condition_ids.add(str(value))

    for key in ("operations", "recovery_operations", "update_operations"):
        collect_groupids(action.get(key), operation_ids)

    return condition_ids, operation_ids


def extract_action_recipients(action: Dict[str, Any]) -> Tuple[Set[str], Set[str]]:
    usergroup_ids: Set[str] = set()
    user_ids: Set[str] = set()

    for key in ("operations", "recovery_operations", "update_operations"):
        for operation in action.get(key) or []:
            for row in operation.get("opmessage_grp") or []:
                if row.get("usrgrpid") is not None:
                    usergroup_ids.add(str(row.get("usrgrpid")))
            for row in operation.get("opmessage_usr") or []:
                if row.get("userid") is not None:
                    user_ids.add(str(row.get("userid")))

    return usergroup_ids, user_ids


def extract_active_media_sendto(medias: Sequence[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for media in medias or []:
        active = media.get("active")
        if active is None:
            active = media.get("status")
        if str(active) not in ("0", "False", "false", "active"):
            continue
        sendto = media.get("sendto")
        if sendto is None:
            continue
        text = str(sendto).strip()
        if text:
            out.append(text)
    return out


def build_scope_index(rows: Sequence[Dict[str, Any]], key_name: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key_name) or "")].append(row)
    return grouped
