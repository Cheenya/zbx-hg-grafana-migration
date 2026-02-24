# Zabbix / Grafana Host-Group Migration Toolkit

Набор скриптов для аудита и подготовки миграции host-groups из OLD формата `(BNK|DOM)-...` в NEW `(BNK|DOM)/AS/<AS>/...` в Zabbix 7.0+ и связанных упоминаний в Grafana.

Проект ориентирован на **локальный запуск**, **без интерактива**, с **отключенной проверкой TLS** и чтением секретов **из `config.py`**.

---

## Основные принципы

- **Zabbix 7.0+ JSON-RPC**: авторизация по username/password.
- **Grafana HTTP API**: только чтение (логин/пароль; token опционально), поиск упоминаний групп в JSON dashboards.
- **No env / no prompt**: все параметры и секреты задаются в `config.py`.
- **verify=False везде** + подавление `InsecureRequestWarning`.
- **Предсказуемость**: ошибки API выбрасывают `RuntimeError` (без «самолечения»).
- **ENV политика**: среда берётся из тега `ENV`, несоответствия помечаются в отчёте.
- **UNKNOWN-хосты**: отдельный лист отчёта (AS/ASN/группа UNKNOWN/отсутствие AS).

---

## Файлы и назначение

### Аудит Zabbix + Grafana
- `zbx_hg_mapping_audit.py`
  - Основной аудит: эталон NEW↔OLD, outliers, actions, rights, media.
  - Опционально: Grafana‑поиск (если включено в конфиге).
  - Опционально: автосоздание бэкапа.
  - Сохраняет seed для Grafana‑only отчёта.

### Аудит по списку AS
- `audit_scope.py`
  - Аудит Zabbix + Grafana только по заданным AS.
  - Выводит один XLSX на запуск.

### Grafana‑only аудит (без Zabbix)
- `grafana_only_audit.py`
  - Работает от seed JSON (из `zbx_hg_mapping_audit.py`).
  - Пишет XLSX со стилем основного отчёта.

### Бэкап и откат
- `make_backup.py`
  - Создание бэкапа Zabbix.
  - Бэкап **только по AS‑scope** (без scope запуск запрещён).
  - Формат: один JSON или JSON.GZ.
- `restore_backup.py`
  - Восстановление из бэкапа.
  - Восстанавливает users → usergroups → actions → hosts.
- `backup_model.py` / `backup_io.py`
  - Модели данных + save/load.

### Общие модули (рефакторинг)
- `api_clients.py`
  - Единые HTTP-клиенты: `ZabbixAPI`, `GrafanaAPI`.
  - Используются в audit/backup/restore/migrate/ENV detector.
- `scope_utils.py`
  - Нормализация AS-scope и генерация безопасных имён файлов по scope.
- `artifact_paths.py`
  - Единая генерация путей `*_zbx_seed.json` и `*_migration_plan.json`.

### Конфигурация
- `config.py`
  - Все параметры/секреты/флаги выполнения.

### Миграция (точечно по одной AS)
- `migrate_single_as.py`
  - Использует план миграции (`*_migration_plan.json`), а при его отсутствии — `MAPPING` из Excel.
  - **Хосты:** удаляет OLD‑группы, **NEW‑группы не добавляет** (считаются уже существующими).
  - **Actions:** заменяет groupid в условиях и операциях.
  - **User groups:** заменяет groupid в `hostgroup_rights`.
  - **Maintenance:** заменяет groupid в `groups`.
  - **Grafana:** заменяет OLD‑группы в строковых полях JSON dashboards.

---

## Конфиг (`config.py`)

Ключевые параметры:

```python
# Zabbix
ZBX_URL = "https://zabbix.example/api_jsonrpc.php"
ZBX_USER = "login"
ZBX_PASSWORD = "password"

# Grafana
GRAFANA_URL = "https://grafana.example"
GRAFANA_USER = "login"
GRAFANA_PASSWORD = "password"
GRAFANA_TOKEN = ""  # опционально

# Runtime
CONFIG.runtime.enable_grafana_audit = True
CONFIG.runtime.create_backup_on_audit = False
CONFIG.runtime.save_zabbix_seed_on_audit = True
CONFIG.runtime.zabbix_seed_path = None  # авто: <output_xlsx>_zbx_seed.json
```

Также есть параметры:
- `excluded_group_patterns` — regex исключений групп.
- `mapping` — пороги эталона.
- `excel` — имя файла и лимит листов.
- `runtime.audit_scope_as` — **обязательный** список AS для `audit_scope.py`, `make_backup.py`, `grafana_only_audit.py`.

---

## Форматы и правила

### NEW host-groups
`(BNK|DOM)/AS/<AS>/...`
- `<AS>` сравнивается **case-insensitive**.

### OLD host-groups
`(BNK|DOM)-...`
- без `/`.

### UNKNOWN
- AS == `UNKNOWN` или ASN == `UNKNOWN` или группа `UNKNOWN` или AS отсутствует.

### ENV политика
- Тег `ENV` используется для подсветки несовпадений.
- В отчёте выводится `env_mismatch`.

---

## Запуск

### 1) Полный аудит (Zabbix + Grafana)
```bash
python zbx_hg_mapping_audit.py
```

### 2) Аудит по AS
```bash
python audit_scope.py
```

### 3) Grafana‑only аудит
```bash
python grafana_only_audit.py --seed hostgroup_mapping_audit_zbx_seed.json
```

### 4) Бэкап
```bash
python make_backup.py
# или с именем файла
python make_backup.py --out my_backup.json.gz
```

### 5) Восстановление
```bash
python restore_backup.py path/to/backup.json.gz
```

### 6) Миграция (одна AS)
```bash
# Перед запуском выставьте AS_VALUE и DRY_RUN_* внутри migrate_single_as.py
python migrate_single_as.py
```

---

## Выходные файлы

- XLSX (аудиты) — `CONFIG.excel.output_xlsx` (+ `_partNNN` при большом числе AS)
- Seed — `<output_xlsx>_zbx_seed.json` (если включено)
- План миграции — `<output_xlsx>_migration_plan.json`
- Бэкап — `zbx_backup_<scope>_<timestamp>.json.gz`

---

## Ограничения и нюансы

- Grafana‑поиск ищет **точные упоминания** известных групп из Zabbix.
- Дополнительно выводятся **шаблоны/переменные/регэкспы** с `match_type = OLD_PATTERN/NEW_PATTERN`.
- При `enable_grafana_audit=True` Grafana‑ошибки **не останавливают** Zabbix‑аудит (пишется предупреждение).
- Бэкап хранит `raw` объекты (полный снимок); restore применяет whitelist ключей.
- Миграция предполагает, что NEW‑группы **уже есть** на хостах; она удаляет только OLD‑группы.

---

## Минимальные зависимости

```bash
pip install requests openpyxl urllib3
```

---

## Рекомендованный поток работ

1) Прогон аудита Zabbix → получить эталон и MAPPING.
2) Grafana‑поиск (либо вместе, либо отдельно по seed).
3) Проверить/отредактировать план миграции (`*_migration_plan.json`) — включить/исключить нужные пары.
4) (При миграции) создать бэкап по AS‑scope.
5) Выполнить изменения (`migrate_single_as.py`).
6) При необходимости — откат через restore.

---

## Безопасность

- `verify=False` везде (текущая модель запуска — локальная, без TLS‑валидации).
- Никаких env‑переменных и интерактива (только `config.py`).
