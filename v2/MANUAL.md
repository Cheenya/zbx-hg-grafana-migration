# v2 MANUAL

## 1. Назначение

`v2/` — это новый отдельный контур подготовки миграции.

Его задача сейчас:
- честно собрать read-only аудит;
- дать редактируемый `mapping_plan`;
- построить точный `impact_plan`;
- собрать backup строго по change-scope;
- проверить backup;
- уметь сделать restore.

На текущем этапе `v2`:
- ничего не меняет в Zabbix;
- ничего не меняет в Grafana;
- не выполняет саму миграцию.


## 2. Точки входа

```bash
python v2/audit_scope.py
python v2/build_impact_plan.py
python v2/make_backup.py
python v2/verify_backup.py
python v2/restore_backup.py
```

Назначение:
- `audit_scope.py` — аудит и первичный `mapping_plan.xlsx`;
- `build_impact_plan.py` — build change-scope по выбранным mappings;
- `make_backup.py` — backup по `impact_plan.json`;
- `verify_backup.py` — сверка backup против `impact_plan.json`;
- `restore_backup.py` — откат Zabbix из backup.


## 3. Что лежит в каталоге

- `config.py` — единый конфиг `v2`;
- `api_clients.py` — HTTP-клиенты для Zabbix/Grafana;
- `common.py` — общие утилиты и генерация путей артефактов;
- `zabbix_audit.py` — read-only аудит Zabbix;
- `grafana_audit.py` — read-only аудит Grafana;
- `report_writer.py` — запись audit workbook/json;
- `mapping_plan.py` — запись/чтение `mapping_plan.xlsx`;
- `impact_plan.py` — построение и запись `impact_plan`;
- `backup_model.py` / `backup_io.py` — модель и I/O backup;
- `README.md` — краткая памятка;
- `MANUAL.md` — этот документ.


## 4. Что настраивается в `v2/config.py`

### 4.1. Подключение к Zabbix

```python
ZBX_URL = ""
ZBX_USER = ""
ZBX_PASSWORD = ""
```

### 4.2. Подключение к Grafana

```python
GRAFANA_URL = ""
GRAFANA_USER = ""
GRAFANA_PASSWORD = ""
```

В `v2` Grafana сейчас работает по логину/паролю.

### 4.3. Scope

```python
SCOPE_AS: tuple[str, ...] = ()
SCOPE_ENVS: tuple[str, ...] = ()
```

- `SCOPE_AS` обязателен;
- `SCOPE_ENVS` опционален.

Логика `ENV` фиксированная:
- `PROD` -> `PROD`
- любое другое непустое значение -> `NONPROD`

То есть для pilot достаточно:

```python
SCOPE_AS = ("your_as",)
SCOPE_ENVS = ("NONPROD",)
```

### 4.4. Runtime

```python
HTTP_TIMEOUT_SEC = 90
MONITORED_HOSTS_ONLY = False
ENABLE_GRAFANA = True
OUTPUT_DIR = "v2_output"
OUTPUT_PREFIX = "scope_audit_v2"
MAPPING_PLAN_PREFIX = "mapping_plan_v2"
IMPACT_PLAN_PREFIX = "impact_plan_v2"
BACKUP_PREFIX = "scope_backup_v2"
GROUP_SAMPLE_HOSTS = 10
SAVE_JSON_INVENTORY = True
```

Практически:
- `MONITORED_HOSTS_ONLY = False` лучше оставить;
- `ENABLE_GRAFANA = True`, если нужен precheck dashboards;
- все артефакты по умолчанию идут в `v2_output/`.

### 4.5. Входные файлы следующих шагов

```python
SOURCE_AUDIT_JSON = ""
SOURCE_MAPPING_PLAN_XLSX = ""
SOURCE_IMPACT_PLAN_JSON = ""
SOURCE_BACKUP_FILE = ""
```

Использование:
- `SOURCE_AUDIT_JSON` — вход для `build_impact_plan.py`;
- `SOURCE_MAPPING_PLAN_XLSX` — вход для `build_impact_plan.py`;
- `SOURCE_IMPACT_PLAN_JSON` — вход для `make_backup.py` и `verify_backup.py`;
- `SOURCE_BACKUP_FILE` — вход для `verify_backup.py` и `restore_backup.py`.

### 4.6. Теги

```python
TAG_AS = "AS"
TAG_ASN = "ASN"
TAG_ENV = "ENV"
TAG_GAS = "GAS"
TAG_GUEST_NAME = "GUEST-NAME"
```

### 4.7. UNKNOWN

```python
UNKNOWN_TAG_VALUE = "UNKNOWN"
UNKNOWN_GROUP_NAME = "UNKNOWN"
EXCLUDE_UNKNOWN_FROM_STATS = True
```

Хост считается `UNKNOWN`, если:
- `AS == UNKNOWN`;
- `ASN == UNKNOWN`;
- есть группа `UNKNOWN`;
- отсутствует `AS`.

### 4.8. Порог для кандидатов в mapping plan

```python
MAPPING_MIN_INTERSECTION = 2
MAPPING_MIN_OLD_COVERAGE = 0.20
MAPPING_MIN_NEW_COVERAGE = 0.20
MAPPING_FORBID_ENV_MISMATCH = True
```

Это влияет только на список кандидатов в `MAPPING_PLAN`.
Это не означает автоматическую миграцию.


## 5. Что делает `audit_scope.py`

`audit_scope.py`:
- читает хосты, actions, usergroups, users, maintenances;
- выделяет `UNKNOWN_HOSTS`;
- строит `HOSTS`, `GROUPS_OLD`, `GROUPS_NEW`;
- строит `MAPPING_PLAN` с кандидатами `OLD -> NEW`;
- подтягивает Grafana matches;
- пишет:
  - audit workbook;
  - audit json;
  - отдельный `mapping_plan.xlsx`.

### 5.1. Артефакты аудита

По одному запуску создаются:
- `scope_audit_v2_<scope>_<timestamp>.xlsx`
- `scope_audit_v2_<scope>_<timestamp>.json`
- `mapping_plan_v2_<scope>_<timestamp>.xlsx`

### 5.2. Листы audit workbook

`SUMMARY`
- сводка по scope и количествам объектов.

`UNKNOWN_HOSTS`
- хосты с `AS/ASN == UNKNOWN`, группой `UNKNOWN` или без `AS`.

`HOSTS`
- основные хосты scope;
- показывает `AS`, `ASN`, `GAS`, `GUEST_NAME`, `ENV_RAW`, `ENV_SCOPE`.

`HOSTS_SKIPPED_ENV`
- хосты выбранной AS, исключённые по `SCOPE_ENVS`.

`GROUPS_OLD`
- legacy host-groups в scope.

`GROUPS_NEW`
- новые host-groups в scope.

`MAPPING_PLAN`
- кандидаты `OLD -> NEW`.

`ACTIONS`
- actions, где используются scope-группы.

`USERGROUPS`
- usergroups с правами/tag-filters/участием в recipients.

`MAINTENANCES`
- maintenances, где используются scope-группы.

`GRAFANA`
- exact/pattern matches из dashboards.

`INVENTORY`
- технический блок audit scope.


## 6. Как читать `MAPPING_PLAN`

Одна строка — один кандидат `OLD -> NEW`.

Главные поля:
- `selected` — выставляется руками в `yes`, если именно эту пару нужно использовать;
- `old_group`, `old_groupid`;
- `new_group`, `new_groupid`;
- `candidate_rank`;
- `candidate_count`;
- `intersection`;
- `old_coverage`;
- `new_coverage`;
- `jaccard`;
- `old_envs`, `new_envs`;
- `env_relation`;
- `manual_required`;
- `status`;
- `comment`.

Как использовать:
- `selected=yes` ставим только на реально подтверждённые пары;
- у одного `old_group` должна быть выбрана только одна строка;
- один `new_group` тоже не должен быть выбран для нескольких `old_group`.

Текущая автологика:
- если кандидат один, без ENV-конфликта и без конфликта по `new_group`, строка ставится `selected=yes` автоматически;
- всё неоднозначное остаётся на ручную проверку.


## 7. Что делает `build_impact_plan.py`

Вход:
- `SOURCE_AUDIT_JSON`
- `SOURCE_MAPPING_PLAN_XLSX`

Шаги:
1. Читает audit json.
2. Читает `mapping_plan.xlsx`.
3. Берёт только строки с `selected=yes`.
4. Строит change points:
   - `action.filter.conditions[*].value`
   - `action.operations/recovery/update ... groupid`
   - `usergroup.hostgroup_rights[*].groupid`
   - `maintenance.groups[*].groupid`
   - Grafana exact/pattern matches
5. Формирует `backup_scope`.
6. Пишет:
   - `impact_plan_v2_*.xlsx`
   - `impact_plan_v2_*.json`

### 7.1. Что лежит в impact plan

`SELECTED_MAPPINGS`
- только реально выбранные пары.

`ZABBIX_CHANGES`
- точные места замены `old_groupid -> new_groupid`.

`GRAFANA_CHANGES`
- exact matches и pattern matches.

`BACKUP_SCOPE`
- набор ID, который надо реально бэкапить.


## 8. Что делает `make_backup.py`

Вход:
- `SOURCE_IMPACT_PLAN_JSON`

`make_backup.py`:
- читает `backup_scope` из impact plan;
- забирает из Zabbix:
  - host-groups;
  - hosts;
  - actions;
  - usergroups;
  - users;
  - maintenances;
- сохраняет backup в `.json.gz`;
- валится, если хотя бы один ID из `backup_scope` не вернулся.

Важно:
- backup теперь строится не по broad audit scope;
- backup строится только по подтверждённому change-scope.


## 9. Что делает `verify_backup.py`

Вход:
- `SOURCE_IMPACT_PLAN_JSON`
- `SOURCE_BACKUP_FILE`

`verify_backup.py` сверяет:
- `scope_as`;
- `scope_envs`;
- hostgroups;
- hosts;
- actions;
- usergroups;
- users;
- maintenances.

Проверка успешна только если:
- нет `missing`;
- нет `extra`;
- scope совпадает.


## 10. Что делает `restore_backup.py`

Вход:
- `SOURCE_BACKUP_FILE`

Откат идёт в порядке:
1. users
2. usergroups
3. actions
4. maintenances
5. hosts

Restore работает только с Zabbix.
Grafana в backup не входит.


## 11. Рекомендуемый рабочий процесс

### Шаг 1. Audit

В `v2/config.py`:

```python
SCOPE_AS = ("your_as",)
SCOPE_ENVS = ("NONPROD",)
```

Запуск:

```bash
python v2/audit_scope.py
```

### Шаг 2. Ручная проверка

Проверить:
- `UNKNOWN_HOSTS`;
- `HOSTS`;
- `GROUPS_OLD`;
- `GROUPS_NEW`;
- `MAPPING_PLAN`;
- `ACTIONS`;
- `USERGROUPS`;
- `MAINTENANCES`;
- `GRAFANA`.

### Шаг 3. Подтвердить mappings

Открыть `mapping_plan_v2_*.xlsx` и руками отметить нужные строки:

```text
selected = yes
```

### Шаг 4. Build impact plan

В `v2/config.py` указать:

```python
SOURCE_AUDIT_JSON = r"v2_output\\scope_audit_v2_....json"
SOURCE_MAPPING_PLAN_XLSX = r"v2_output\\mapping_plan_v2_....xlsx"
```

Запуск:

```bash
python v2/build_impact_plan.py
```

### Шаг 5. Build backup

В `v2/config.py` указать:

```python
SOURCE_IMPACT_PLAN_JSON = r"v2_output\\impact_plan_v2_....json"
```

Запуск:

```bash
python v2/make_backup.py
```

### Шаг 6. Verify backup

В `v2/config.py` указать:

```python
SOURCE_BACKUP_FILE = r"v2_output\\scope_backup_v2_....json.gz"
```

Запуск:

```bash
python v2/verify_backup.py
```

### Шаг 7. Restore test

На pilot-контуре:

```bash
python v2/restore_backup.py
```

Только после успешного цикла:
- audit
- mapping review
- impact plan
- backup
- verify
- restore test

можно переходить к будущему `migrate v2`.


## 12. Ограничения текущей версии

`v2` пока не делает:
- собственно миграцию;
- postcheck после миграции;
- автоматический rewrite Grafana dashboards;
- автоматическое принятие ambiguous mappings.

Это сознательно.


## 13. Типовые проблемы

### Scope пустой

Причина:
- не заполнен `SCOPE_AS`.

### Хостов мало

Проверь:
- `TAG_AS`;
- `SCOPE_AS`;
- `SCOPE_ENVS`;
- `MONITORED_HOSTS_ONLY`.

### В `MAPPING_PLAN` нет кандидатов

Проверь:
- действительно ли OLD и NEW группы пересекаются по хостам;
- не отфильтровались ли кандидаты по `MAPPING_MIN_*`;
- нет ли полного ENV mismatch.

### Impact plan пустой

Проверь:
- есть ли строки `selected=yes` в `mapping_plan.xlsx`.

### Grafana не попала в audit

Проверь:
- `ENABLE_GRAFANA = True`;
- `GRAFANA_URL`;
- `GRAFANA_USER`;
- `GRAFANA_PASSWORD`.

Если Grafana недоступна, Zabbix-аудит всё равно сохраняется, а ошибка пишется в `SUMMARY`.
