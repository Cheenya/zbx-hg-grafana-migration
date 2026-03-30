from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from openpyxl.utils import get_column_letter  # type: ignore

import config


EXCLUDED_GROUP_PATTERNS = [re.compile(pattern) for pattern in config.EXCLUDED_GROUP_PATTERNS]


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
    return "/" not in text and re.match(r"^(BNK|DOM)-", text) is not None


def is_new_group_for_as(name: str, as_value: str) -> bool:
    text = str(name or "")
    parts = [part for part in text.split("/") if part]
    if len(parts) < 3:
        return False
    prefix, marker, group_as = parts[0], parts[1], parts[2]
    if prefix not in ("BNK", "DOM"):
        return False
    if marker != "AS":
        return False
    return group_as.strip().lower() == as_value.strip().lower()


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


def build_scope_part(scope_as: Sequence[str], scope_env: str) -> str:
    scope_chunks: List[str] = []
    as_values = normalize_values(scope_as)
    env_value = str(scope_env or "").strip()

    if as_values:
        if len(as_values) <= 3:
            scope_chunks.append("-".join(re.sub(r"[^A-Za-z0-9_-]", "_", item) for item in as_values))
        else:
            scope_chunks.append(f"AS{len(as_values)}")
    else:
        scope_chunks.append("NOAS")

    if env_value:
        scope_chunks.append(re.sub(r"[^A-Za-z0-9_-]", "_", env_value))

    return "_".join(scope_chunks)


def build_artifact_path(
    prefix: str,
    scope_as: Sequence[str],
    scope_env: str,
    extension: str,
    timestamp: Optional[str] = None,
) -> str:
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    scope_part = build_scope_part(scope_as, scope_env)
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


def build_output_paths(scope_as: Sequence[str], scope_env: str) -> Tuple[str, str]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (
        build_artifact_path(config.OUTPUT_PREFIX, scope_as, scope_env, ".xlsx", timestamp=timestamp),
        build_artifact_path(config.OUTPUT_PREFIX, scope_as, scope_env, ".json", timestamp=timestamp),
    )


def resolve_input_artifact(
    configured_path: str,
    prefix: str,
    extension: str,
    *,
    scope_as: Sequence[str] | None = None,
    scope_env: str = "",
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

    if normalized_org_ids:
        preferred_stem = f"{prefix}_{build_org_scope_part(normalized_org_ids)}_"
    elif normalized_scope_as or normalized_scope_env:
        preferred_stem = f"{prefix}_{build_scope_part(normalized_scope_as, normalized_scope_env)}_"

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
