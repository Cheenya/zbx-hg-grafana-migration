# v2 MANUAL

## 1. Назначение

Каталог `v2/` — это новый, упрощенный и изолированный контур для подготовки миграции.

Его текущая задача: **дать безопасный подготовительный контур** по выбранному scope, который уже умеет:
- делать read-only аудит;
- собирать backup по готовому inventory;
- проверять backup на полное покрытие inventory.

На текущем этапе `v2`:
- **ничего не меняет** в Zabbix;
- **ничего не меняет** в Grafana;
- только читает данные и сохраняет артефакты подготовки.


## 2. Что уже реализовано

Точки входа:

```bash
python -m v2.audit_scope
python -m v2.make_backup
python -m v2.verify_backup
```

`python -m v2.audit_scope`
1. Подключается к Zabbix.
2. Берет scope из `v2/config.py`.
3. Собирает инвентаризацию:
   - хостов в scope;
   - OLD host-groups;
   - NEW host-groups;
   - actions;
   - usergroups;
   - maintenances.
4. При включенной Grafana-части:
   - получает dashboards;
   - скачивает JSON dashboards;
   - ищет exact matches и pattern-like matches по OLD/NEW именам.
5. Сохраняет два артефакта:
   - `xlsx` отчет;
   - `json` inventory.

`python -m v2.make_backup`
1. Читает путь `SOURCE_INVENTORY_JSON` из `v2/config.py`.
2. Загружает `json inventory`, созданный `audit_scope`.
3. Достает из inventory точные ID сущностей:
   - host-groups;
   - hosts;
   - actions;
   - usergroups;
   - users;
   - maintenances.
4. Запрашивает эти сущности из Zabbix по ID.
5. Проверяет полное покрытие inventory.
6. Сохраняет backup в `json.gz`.

`python -m v2.verify_backup`
1. Читает `SOURCE_INVENTORY_JSON`.
2. Читает `SOURCE_BACKUP_FILE`.
3. Сравнивает scope и наборы ID между inventory и backup.
4. Падает с ошибкой, если есть missing или extra объекты.


## 3. Структура каталога `v2`

- `config.py`
  Единый конфиг нового контура.
- `audit_scope.py`
  CLI-скрипт для запуска scoped-аудита.
- `zabbix_audit.py`
  Логика чтения Zabbix и построения read-only inventory.
- `grafana_audit.py`
  Логика чтения Grafana dashboards и поиска совпадений.
- `api_clients.py`
  HTTP-клиенты только для `v2`.
- `common.py`
  Общие утилиты.
- `report_writer.py`
  Запись `xlsx` и `json`.
- `backup_model.py`
  Dataclass-модель backup-файла.
- `backup_io.py`
  Чтение и запись backup-файла.
- `make_backup.py`
  Сбор backup по `json inventory`.
- `verify_backup.py`
  Сверка backup против `json inventory`.
- `README.md`
  Короткая памятка.
- `MANUAL.md`
  Этот подробный документ.


## 4. Где настраивать запуск

Все параметры нового контура находятся в:

- `v2/config.py`

### 4.1. Подключение к Zabbix

```python
ZBX_URL = ""
ZBX_USER = ""
ZBX_PASSWORD = ""
```

Нужно заполнить напрямую.

### 4.2. Подключение к Grafana

```python
GRAFANA_URL = ""
GRAFANA_USER = ""
GRAFANA_PASSWORD = ""
GRAFANA_TOKEN = ""
```

Если используешь логин/пароль, заполняй их.
Если используешь token, заполняй `GRAFANA_TOKEN`.

### 4.3. Scope

```python
SCOPE_AS: tuple[str, ...] = ()
SCOPE_ENVS: tuple[str, ...] = ()
```

`SCOPE_AS`
- список AS, по которым идет аудит;
- без него запуск завершится ошибкой.

`SCOPE_ENVS`
- опциональный фильтр по **каноническому** `ENV`;
- если пустой, аудит берет все хосты выбранной AS;
- если задан, например `("NONPROD",)`, то в основной отчет попадут только эти хосты;
- остальные хосты той же AS попадут в отдельный лист `HOSTS_SKIPPED_ENV`.

Важно:
- руками перечислять `DEV`, `STAGE`, `TEST`, `UAT` и подобные значения не нужно;
- в `v2` зафиксирована логика:
  - `PROD` -> `PROD`
  - любое другое непустое значение -> `NONPROD`
- значит, для pilot достаточно задавать:
  - `SCOPE_ENVS = ("NONPROD",)`
  - или `SCOPE_ENVS = ("PROD",)`

### 4.4. Runtime-параметры

```python
HTTP_TIMEOUT_SEC = 90
MONITORED_HOSTS_ONLY = False
ENABLE_GRAFANA = True
OUTPUT_DIR = "v2_output"
OUTPUT_PREFIX = "scope_audit_v2"
GROUP_SAMPLE_HOSTS = 10
SAVE_JSON_INVENTORY = True
BACKUP_PREFIX = "scope_backup_v2"
```

Рекомендации:
- `MONITORED_HOSTS_ONLY = False` оставить так, это безопаснее для полного аудита;
- `ENABLE_GRAFANA = True`, если Grafana должна участвовать в precheck;
- `OUTPUT_DIR` менять только если нужен другой каталог артефактов.

### 4.5. Входные файлы для backup/verify

```python
SOURCE_INVENTORY_JSON = ""
SOURCE_BACKUP_FILE = ""
```

`SOURCE_INVENTORY_JSON`
- путь к `json`, который создал `python -m v2.audit_scope`;
- обязателен для `python -m v2.make_backup`;
- обязателен для `python -m v2.verify_backup`.

`SOURCE_BACKUP_FILE`
- путь к backup-файлу, который создал `python -m v2.make_backup`;
- обязателен для `python -m v2.verify_backup`.

### 4.6. Теги

```python
TAG_AS = "AS"
TAG_ASN = "ASN"
TAG_ENV = "ENV"
```

Если в Zabbix используются другие имена тегов, менять нужно здесь.

### 4.7. Логика нормализации ENV

В `v2/config.py` зафиксированы значения:

```python
ENV_PROD_LABEL = "PROD"
ENV_NONPROD_LABEL = "NONPROD"
PROD_ENV_VALUES = ("PROD",)
```

Это означает:
- если raw `ENV` равен `PROD`, то scope-класс будет `PROD`;
- любой другой непустой raw `ENV` попадает в класс `NONPROD`.

Примеры:
- `PROD` -> `PROD`
- `NONPROD` -> `NONPROD`
- `DEV` -> `NONPROD`
- `STAGE` -> `NONPROD`
- `TEST` -> `NONPROD`
- `UAT` -> `NONPROD`
- `PREPROD` -> `NONPROD`

### 4.8. Исключаемые группы

```python
EXCLUDED_GROUP_PATTERNS = (...)
```

Это regex-группы, которые не должны участвовать в audit scope.


## 5. Что считается scope

Scope в `v2` определяется так:

1. Хост должен иметь тег `AS`.
2. Значение `AS` должно входить в `SCOPE_AS`.
3. Если задан `SCOPE_ENVS`, то **канонический** `ENV` хоста должен входить в этот список.

Если `SCOPE_ENVS` задан:
- подходящие хосты попадают в основную выборку;
- хосты той же AS, но с другим `ENV`, попадают в `HOSTS_SKIPPED_ENV`.

Это сделано специально для pilot-прогонов по `NONPROD`.


## 6. Как запускать

### 6.1. Базовый запуск

```bash
python -m v2.audit_scope
```

### 6.2. С пользовательскими именами файлов

```bash
python -m v2.audit_scope --out-xlsx my_scope.xlsx --out-json my_scope.json
```

### 6.3. Рекомендуемый первый pilot

В `v2/config.py`:

```python
SCOPE_AS = ("your_as",)
SCOPE_ENVS = ("NONPROD",)
```

После этого:

```bash
python -m v2.audit_scope
```

### 6.4. Сбор backup по inventory

1. Выполнить `python -m v2.audit_scope`.
2. Вписать путь к созданному `json` в `SOURCE_INVENTORY_JSON`.
3. Запустить:

```bash
python -m v2.make_backup
```

### 6.5. Проверка backup

1. Вписать путь к backup-файлу в `SOURCE_BACKUP_FILE`.
2. Запустить:

```bash
python -m v2.verify_backup
```


## 7. Что будет создано

По умолчанию артефакты кладутся в каталог:

- `v2_output/`

Имя файлов строится так:

- `<OUTPUT_PREFIX>_<scope>_<timestamp>.xlsx`
- `<OUTPUT_PREFIX>_<scope>_<timestamp>.json`
- `<BACKUP_PREFIX>_<scope>_<timestamp>.json.gz`

Пример:

- `v2_output/scope_audit_v2_dom_itmon_NONPROD_20260320_120000.xlsx`
- `v2_output/scope_audit_v2_dom_itmon_NONPROD_20260320_120000.json`
- `v2_output/scope_backup_v2_dom_itmon_NONPROD_20260320_121500.json.gz`


## 8. Что лежит в XLSX

### `SUMMARY`

Сводные числа по запуску:
- scope AS;
- scope ENV;
- сколько хостов попало в scope;
- сколько хостов пропущено по ENV;
- сколько OLD/NEW групп найдено;
- сколько actions/usergroups/maintenances попало в отчет;
- сколько записей пришло из Grafana;
- текст ошибки Grafana, если она была.

### `HOSTS`

Хосты, которые реально попали в scope.

Поля:
- `hostid`
- `host`
- `name`
- `status`
- `AS`
- `ASN`
- `ENV_RAW`
- `ENV_SCOPE`
- `old_groups`
- `new_groups`
- `other_groups`

Назначение:
- быстро проверить, что scope вообще собран правильно;
- видеть одновременно исходный `ENV` и канонический класс `PROD/NONPROD`;
- увидеть, какие OLD/NEW группы реально висят на хостах.

### `HOSTS_SKIPPED_ENV`

Хосты выбранной AS, которые не попали в итоговую выборку из-за фильтра `SCOPE_ENVS`.

Это важный лист для pilot:
- показывает, что именно было исключено;
- позволяет не перепутать `NONPROD` и `PROD`.

Поля `ENV_RAW` и `ENV_SCOPE` нужны для прозрачности:
- `ENV_RAW` показывает исходное значение тега;
- `ENV_SCOPE` показывает, как `v2` классифицировал хост для pilot scope.

### `GROUPS_OLD`

Сводка по legacy-группам в scope.

Поля:
- `group_name`
- `groupid`
- `hosts_count`
- `as_values`
- `env_values`
- `sample_hosts`

### `GROUPS_NEW`

Сводка по новым группам в scope.

Поля те же, что и у `GROUPS_OLD`.

### `ACTIONS`

Trigger actions, которые затрагивают группы из scope.

Поля:
- `actionid`
- `name`
- `status`
- `where_found`
- `matched_groupids`
- `matched_group_names`
- `recipient_usergroups`
- `recipient_users`
- `recipients_media`

`where_found`:
- `conditions`
- `operations`
- `both`

### `USERGROUPS`

Группы пользователей, связанные со scope через:
- права на host-groups;
- tag filters;
- участие как recipients в actions.

Поля:
- `usrgrpid`
- `name`
- `rights_on_scope_groups`
- `matching_tag_filters`
- `users`
- `users_media`
- `is_action_recipient`

### `MAINTENANCES`

Maintenances, связанные с host-groups из scope.

Поля:
- `maintenanceid`
- `name`
- `matched_groupids`
- `matched_group_names`
- `active_since`
- `active_till`

### `GRAFANA`

Все найденные в dashboards упоминания scope-групп.

Поля:
- `AS`
- `dashboard_uid`
- `dashboard_title`
- `match_type`
- `matched_string`
- `json_path`
- `count`

`match_type`:
- `OLD`
- `NEW`
- `OLD_PATTERN`
- `NEW_PATTERN`

### `INVENTORY`

Технический лист, где лежат сериализованные блоки будущего migration scope:
- `hostids`
- `hostgroups`
- `actionids`
- `usergroupids`
- `userids`
- `maintenanceids`

Этот лист нужен как мост к будущему `backup v2`.


## 9. Что лежит в JSON

JSON — это машинно-читаемая версия отчета.

Основные разделы:
- `meta`
- `summary`
- `inventory`
- `hosts`
- `hosts_skipped_env`
- `groups_old`
- `groups_new`
- `actions`
- `usergroups`
- `maintenances`
- `grafana`

Именно этот файл потом удобно использовать как вход для:
- `backup v2`
- `verify_backup v2`
- `migrate v2`


## 10. Как читать результаты

### 10.1. Если цель — pilot на NONPROD

Проверь по порядку:

1. `SUMMARY`
   Убедиться, что scope собран по нужной AS и нужному каноническому `ENV`.
2. `HOSTS`
   Убедиться, что попали только нужные NONPROD хосты.
3. `HOSTS_SKIPPED_ENV`
   Убедиться, что PROD не попал в основной scope.
4. `GROUPS_OLD` и `GROUPS_NEW`
   Проверить, что список host-groups выглядит ожидаемо.
5. `ACTIONS`
   Проверить, какие actions реально завязаны на scope.
6. `USERGROUPS`
   Проверить права и адресатов.
7. `MAINTENANCES`
   Проверить связанные maintenance windows.
8. `GRAFANA`
   Проверить dashboards, где встречаются OLD/NEW строки.

### 10.2. Если цель — подготовка backup v2

Смотри в первую очередь:
- `INVENTORY`
- `ACTIONS`
- `USERGROUPS`
- `MAINTENANCES`

Там должен быть понятный и конечный список того, что в будущем пойдет в backup.


## 11. Как работает backup v2

`backup v2` строится по точным ID из `json inventory`, а не по догадкам и не по повторному вычислению scope.

Что именно попадает в backup:
- host-groups из `inventory.hostgroups`;
- hosts из `inventory.hostids`;
- actions из `inventory.actionids`;
- usergroups из `inventory.usergroupids`;
- users из `inventory.userids`;
- maintenances из `inventory.maintenanceids`.

Что важно:
- backup не выбирает scope сам;
- backup не пересчитывает связи заново;
- backup берет только то, что уже зафиксировано в `json inventory`.

Если Zabbix не вернул хотя бы один объект из inventory, `make_backup` завершится `RuntimeError`.


## 12. Как работает verify backup

`verify_backup` не ходит в Zabbix повторно. Он сравнивает два локальных артефакта:
- `SOURCE_INVENTORY_JSON`;
- `SOURCE_BACKUP_FILE`.

Он проверяет:
- `scope_as`;
- `scope_envs`;
- набор `hostgroups`;
- набор `hosts`;
- набор `actions`;
- набор `usergroups`;
- набор `users`;
- набор `maintenances`.

Проверка считается успешной только если:
- нет missing ID;
- нет extra ID;
- scope совпадает.


## 13. Ограничения текущей версии

Важно понимать, чего `v2` пока не делает:

1. Не строит автоматический migration plan.
2. Не говорит, какой OLD надо менять на какой NEW.
3. Не блокирует ambiguous-ситуации автоматически.
4. Не делает restore.
5. Не применяет изменения в Zabbix.
6. Не применяет изменения в Grafana.
7. Не проверяет содержимое backup на семантическую корректность полей, кроме состава scope и ID.

Это сознательное ограничение. Сначала нужна честная инвентаризация и проверяемый backup, а не “умная” автоматизация.


## 14. Рекомендуемый рабочий процесс

### Шаг 1. Pilot audit по NONPROD

В `v2/config.py`:

```python
SCOPE_AS = ("your_as",)
SCOPE_ENVS = ("NONPROD",)
```

Запуск:

```bash
python -m v2.audit_scope
```

### Шаг 2. Ручная проверка отчета

Проверить:
- scope хостов;
- списки OLD/NEW групп;
- actions;
- usergroups;
- maintenances;
- Grafana matches.

### Шаг 3. Backup v2

В `v2/config.py` указать:

```python
SOURCE_INVENTORY_JSON = r"v2_output\\scope_audit_v2_dom_itmon_NONPROD_20260320_120000.json"
```

Запуск:

```bash
python -m v2.make_backup
```

### Шаг 4. Verify backup v2

В `v2/config.py` указать:

```python
SOURCE_BACKUP_FILE = r"v2_output\\scope_backup_v2_dom_itmon_NONPROD_20260320_121500.json.gz"
```

Запуск:

```bash
python -m v2.verify_backup
```

### Шаг 5. Только потом проектировать migrate v2

И только после этого можно делать:
- restore v2;
- dry-run на `NONPROD`;
- apply на `NONPROD`;
- postcheck;
- и только потом выходить к `PROD`.


## 15. Типовые проблемы

### Ошибка: scope пустой

Причина:
- не заполнен `SCOPE_AS`.

Что делать:
- указать `SCOPE_AS` в `v2/config.py`.

### Хостов слишком мало

Проверь:
- правильность `SCOPE_AS`;
- правильность `TAG_AS`;
- не сужает ли выборку `SCOPE_ENVS`;
- как выглядят `ENV_RAW` и `ENV_SCOPE` у нужных хостов;
- действительно ли нужные хосты имеют нужный `AS`.

### Grafana не попала в отчет

Проверь:
- `ENABLE_GRAFANA = True`;
- `GRAFANA_URL`;
- `GRAFANA_USER` / `GRAFANA_PASSWORD` или `GRAFANA_TOKEN`.

Если Grafana недоступна, Zabbix-отчет все равно сохранится, а ошибка должна появиться в `SUMMARY`.

### В `GRAFANA` много строк `*_PATTERN`

Это не ошибка. Это значит, что в dashboard JSON найдено не точное имя группы, а строка-паттерн:
- regex;
- переменная;
- query string;
- шаблон фильтра.

Такие места как раз и нужны для ручного анализа перед миграцией.


## 16. Что делать дальше

После того как связка `audit -> backup -> verify` станет стабильной и понятной, следующий модуль должен быть:

- `restore v2`
- `migrate v2`

Причем строить их нужно от тех же артефактов:
- `json inventory` из `audit_scope v2`;
- проверенного backup из `make_backup v2`.

Только так получится:
- предсказуемый scope;
- проверяемый backup;
- повторяемая миграция;
- понятный rollback.
