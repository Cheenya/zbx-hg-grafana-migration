from __future__ import annotations

from config import CONFIG


# Scope задается только здесь и/или в корневом config.py.
SCOPE_AS: tuple[str, ...] = tuple(CONFIG.runtime.audit_scope_as)

# Для pilot можно указать, например: ("NONPROD",)
SCOPE_ENVS: tuple[str, ...] = ()

# Новый аудит по умолчанию ходит во все хосты, а не только monitored.
MONITORED_HOSTS_ONLY: bool = False

# Grafana в v2 можно отключить отдельно.
ENABLE_GRAFANA: bool = True

# Куда складывать артефакты нового контура.
OUTPUT_DIR: str = "v2_output"
OUTPUT_PREFIX: str = "scope_audit_v2"

# Сколько примеров хостов хранить в сводке по группам.
GROUP_SAMPLE_HOSTS: int = 10

# Сохранять JSON-инвентаризацию рядом с Excel.
SAVE_JSON_INVENTORY: bool = True
