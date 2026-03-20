# v2 MANUAL

## 1. Назначение

Каталог `v2/` — это новый, упрощенный и изолированный контур для подготовки миграции.

Его текущая задача одна: **сделать понятный read-only аудит** по выбранному scope, чтобы дальше на этой базе строить:
- `backup v2`
- `verify_backup v2`
- `migrate v2`
- `postcheck v2`

На текущем этапе `v2`:
- **ничего не меняет** в Zabbix;
- **ничего не меняет** в Grafana;
- только читает данные и сохраняет отчеты.


## 2. Что уже реализовано

Точка входа:

```bash
python -m v2.audit_scope
```

Скрипт делает следующее:
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


## 3. Структура каталога `v2`

- `config.py`
  Единый конфиг нового контура.
- `audit_scope.py`
  CLI-скрипт для запуска scoped-аудита.
- `zabbix_audit.py`
  Логика чтения Zabbix и построения read-only inventory.
- `grafana_audit.py`
  Логика чтения Grafana dashboards и поиска совпадений.
- `common.py`
  Общие утилиты.
- `report_writer.py`
  Запись `xlsx` и `json`.
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
- опциональный фильтр по `ENV`;
- если пустой, аудит берет все хосты выбранной AS;
- если задан, например `("NONPROD",)`, то в основной отчет попадут только эти хосты;
- остальные хосты той же AS попадут в отдельный лист `HOSTS_SKIPPED_ENV`.

### 4.4. Runtime-параметры

```python
HTTP_TIMEOUT_SEC = 90
MONITORED_HOSTS_ONLY = False
ENABLE_GRAFANA = True
OUTPUT_DIR = "v2_output"
OUTPUT_PREFIX = "scope_audit_v2"
GROUP_SAMPLE_HOSTS = 10
SAVE_JSON_INVENTORY = True
```

Рекомендации:
- `MONITORED_HOSTS_ONLY = False` оставить так, это безопаснее для полного аудита;
- `ENABLE_GRAFANA = True`, если Grafana должна участвовать в precheck;
- `OUTPUT_DIR` менять только если нужен другой каталог артефактов.

### 4.5. Теги

```python
TAG_AS = "AS"
TAG_ASN = "ASN"
TAG_ENV = "ENV"
```

Если в Zabbix используются другие имена тегов, менять нужно здесь.

### 4.6. Исключаемые группы

```python
EXCLUDED_GROUP_PATTERNS = (...)
```

Это regex-группы, которые не должны участвовать в audit scope.


## 5. Что считается scope

Scope в `v2` определяется так:

1. Хост должен иметь тег `AS`.
2. Значение `AS` должно входить в `SCOPE_AS`.
3. Если задан `SCOPE_ENVS`, то `ENV` хоста должен входить в этот список.

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


## 7. Что будет создано

По умолчанию артефакты кладутся в каталог:

- `v2_output/`

Имя файлов строится так:

- `<OUTPUT_PREFIX>_<scope>_<timestamp>.xlsx`
- `<OUTPUT_PREFIX>_<scope>_<timestamp>.json`

Пример:

- `v2_output/scope_audit_v2_dom_itmon_NONPROD_20260320_120000.xlsx`
- `v2_output/scope_audit_v2_dom_itmon_NONPROD_20260320_120000.json`


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
- `ENV`
- `old_groups`
- `new_groups`
- `other_groups`

Назначение:
- быстро проверить, что scope вообще собран правильно;
- увидеть, какие OLD/NEW группы реально висят на хостах.

### `HOSTS_SKIPPED_ENV`

Хосты выбранной AS, которые не попали в итоговую выборку из-за фильтра `SCOPE_ENVS`.

Это важный лист для pilot:
- показывает, что именно было исключено;
- позволяет не перепутать `NONPROD` и `PROD`.

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
   Убедиться, что scope собран по нужной AS и нужному ENV.
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


## 11. Ограничения текущей версии

Важно понимать, чего `v2` пока не делает:

1. Не строит автоматический migration plan.
2. Не говорит, какой OLD надо менять на какой NEW.
3. Не блокирует ambiguous-ситуации автоматически.
4. Не делает backup.
5. Не делает restore.
6. Не меняет dashboards.
7. Не применяет изменения в Zabbix.

Это сознательное ограничение. Сначала нужна честная инвентаризация, а не “умная” автоматизация.


## 12. Рекомендуемый рабочий процесс

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

### Шаг 3. Проектирование backup v2

На основе `json inventory` определить:
- какие объекты обязаны попасть в backup;
- по каким ID backup должен проверяться;
- какие связи надо валидировать до миграции.

### Шаг 4. Backup v2

Будущий `backup v2` должен собирать данные **не по догадке**, а по `json inventory`.

### Шаг 5. Verify backup v2

Будущий `verify_backup v2` должен подтверждать:
- coverage всех ожидаемых объектов;
- корректность ссылок и ID;
- соответствие scope.

### Шаг 6. Только потом migrate v2

И только после этого можно делать:
- dry-run на `NONPROD`;
- apply на `NONPROD`;
- postcheck;
- restore test;
- и только потом выходить к `PROD`.


## 13. Типовые проблемы

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
- действительно ли нужные хосты имеют нужный `AS` и `ENV`.

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


## 14. Что делать дальше

После того как этот аудит станет стабильным и понятным, следующий модуль должен быть:

- `backup v2`

Причем строить его нужно не “по текущим хостам”, а по `json inventory`, который уже создает `audit_scope v2`.

Только так получится:
- предсказуемый scope;
- проверяемый backup;
- повторяемая миграция;
- понятный rollback.
