# Safe Prep Flow

Подробный документ:
- `MANUAL.md`

Это основной контур подготовки миграции.

Что уже есть:
- `python audit_scope.py` — read-only аудит Zabbix/Grafana;
- `python grafana_org_audit.py` — отдельный аудит Grafana по `orgId`, без привязки к AS;
- `python build_grafana_plan.py` — сбор плана замены old/new host-groups в Grafana variables;
- `python apply_grafana_plan.py` — dry-run/apply этого плана в Grafana;
- `python build_impact_plan.py` — сбор change-scope по подтверждённому `mapping_plan.xlsx`;
- `python make_backup.py` — backup строго по `impact_plan.json`;
- `python verify_backup.py` — проверка backup против `impact_plan.json`;
- `python apply_zabbix_plan.py` — dry-run/apply донасыщения host-groups на хостах по `impact_plan.json`;
- `python apply_changes.py` — единый запуск Zabbix/Grafana с preview и подтверждением;
- `python restore_backup.py` — откат Zabbix из backup.

Что делает аудит:
- читает Zabbix без изменений;
- берёт scope по `SCOPE_AS` и опционально по каноническому `SCOPE_ENV`;
- при необходимости фильтрует хосты по `SCOPE_GAS`;
- опционально маппит `SCOPE_AS` на `GRAFANA_ORGIDS`;
- учитывает `AS`, `ENV`, `GAS`, `GUEST-NAME`;
- формирует ожидаемые standard host-groups по правилам:
  - `$ORG/ENV/#ENV`
  - `$ORG/AS/#AS`
  - `$ORG/AS/#AS/#ENV`
  - `$ORG/GAS/#GAS`
  - `$ORG/GAS/#GAS/#ENV`
  - `$ORG/OS/(LINUX|WINDOWS)`
  - `$ORG/OS/(LINUX|WINDOWS)/#ENV`
- проверяет существование этих групп в `Data collection -> Host groups`;
- отдельно показывает `UNKNOWN`-хосты;
- отдельно показывает `MISMATCHES`:
  - домен хоста vs legacy ORG;
  - домен хоста vs ORG прокси;
  - ENV в legacy-группе vs реальный тег `ENV`;
- строит `MAPPING_PLAN` c кандидатами `OLD -> NEW`;
- не использует частотный анализ для выбора пары `OLD -> NEW`;
- строит `HOST_ENRICHMENT` по хостам;
- ищет только `OLD`-группы в Grafana dashboards;
- сохраняет:
  - `scope_audit_v2_*.xlsx`
  - `scope_audit_v2_*.json`
  - `mapping_plan_v2_*.xlsx`
  - `grafana_audit_v2_*.xlsx`

Что делает `grafana_org_audit.py`:
- берёт `GRAFANA_AUDIT_ORGIDS` из `config.py`;
- берёт только datasource с `type = alexanderzobnin-zabbix-datasource` (или из `GRAFANA_ZABBIX_DATASOURCE_TYPES`);
- в каждой org находит все Zabbix datasources;
- обходит все dashboards в этой org;
- показывает всё, что завязано на Zabbix datasource:
  - dashboards
  - variables
  - panels
  - детали по query/regex/template/group-like строкам;
- сохраняет:
  - `grafana_org_audit_*.xlsx`
  - `grafana_org_audit_*.json`
  - `grafana_org_audit_log_*.log`

Это диагностический контур. В apply-цепочке Grafana он сам по себе больше не обязателен.

Что делает `build_grafana_plan.py`:
- читает `SOURCE_IMPACT_PLAN_JSON`;
- берёт `grafana_changes` из `impact_plan.json`;
- строит исполняемый план только для Grafana variables;
- делит строки на режимы:
  - `exact` — жёстко производные от Zabbix mapping;
  - `manual_regex` — ручные regex/query случаи;
- сохраняет:
  - `grafana_plan_*.xlsx`
  - `grafana_plan_*.json`

Что делает `apply_grafana_plan.py`:
- читает `SOURCE_GRAFANA_PLAN_XLSX`;
- валидирует строки против `SOURCE_IMPACT_PLAN_JSON`;
- берёт только строки, где `apply=yes`;
- по умолчанию работает как dry-run;
- если `GRAFANA_APPLY_CHANGES = True`, реально обновляет dashboard variables через Grafana API;
- сохраняет:
  - `grafana_apply_*.xlsx`
  - `grafana_apply_*.json`

Что делает `build_impact_plan.py`:
- читает `SOURCE_AUDIT_JSON`;
- читает `SOURCE_MAPPING_PLAN_XLSX`;
- берёт только строки, где в `mapping_plan.xlsx` стоит `selected=yes`;
- строит:
  - точные change points в Zabbix;
  - exact/pattern impact по Grafana;
  - `backup_scope` для backup/restore;
- раскладывает Zabbix-изменения на:
  - `HOST_ENRICH_PLAN`
  - `OBJECT_MAPPING_PLAN`
- для `usergroups` планирует только добавление недостающих permissions на новые группы, без удаления old rights;
- сохраняет:
  - `impact_plan_v2_*.xlsx`
  - `impact_plan_v2_*.json`

Что делает `make_backup.py`:
- читает `SOURCE_IMPACT_PLAN_JSON`;
- забирает из Zabbix сущности только из `backup_scope`;
- сохраняет backup в `scope_backup_v2_*.json.gz`;
- падает, если coverage неполный.

Что делает `verify_backup.py`:
- читает `SOURCE_IMPACT_PLAN_JSON` и `SOURCE_BACKUP_FILE`;
- сверяет scope и набор ID между impact plan и backup;
- падает, если есть missing/extra объекты.

Что делает `restore_backup.py`:
- читает `SOURCE_BACKUP_FILE`;
- откатывает Zabbix в порядке:
  - users
  - usergroups
  - actions
  - maintenances
  - hosts

Что делает `apply_zabbix_plan.py`:
- читает `SOURCE_IMPACT_PLAN_JSON`;
- по умолчанию работает как dry-run;
- в apply-режиме требует валидный backup того же scope;
- на текущем этапе применяет только host enrichment:
  - не удаляет old groups;
  - не трогает actions/usergroups/maintenances;
- сохраняет:
  - `zabbix_apply_*.xlsx`
  - `zabbix_apply_*.json`

Что делает `apply_changes.py`:
- это единая точка запуска apply;
- умеет `--target zabbix|grafana|both`;
- по умолчанию делает dry-run;
- при `--apply` сначала печатает preview и спрашивает подтверждение `y/n`;
- `--yes` отключает интерактивное подтверждение.

Логика `ENV`:
- `PROD` -> `PROD`
- любое другое непустое значение -> `NONPROD`

Фильтр `GAS`:
- если `SCOPE_GAS = ()`, GAS не фильтруется;
- если `SCOPE_GAS` заполнен, в аудит и дальнейшие планы попадают только хосты с указанными значениями `GAS`.

Что важно:
- Grafana сейчас работает по логину/паролю;
- Zabbix может работать либо по логину/паролю, либо по `ZBX_API_TOKEN`; если token заполнен, он приоритетнее;
- Zabbix apply сейчас ограничен только донасыщением host groups на хостах;
- старые host-groups на хостах аудит не удаляет и не планирует к удалению;
- если нужного тега нет, соответствующая standard group для хоста не строится;
- Grafana меняется только отдельным `apply_grafana_plan.py` и по умолчанию идёт в dry-run;
- если `SOURCE_*` путь не задан, соответствующий скрипт берёт самый свежий файл из `OUTPUT_DIR`;
- change-scope для Zabbix объектов пока только готовится и проверяется, без автоматического apply.

Формат scope:
- ORG по доменам задаётся в `ORG_DOMAIN_SUFFIXES`, например:
  - `BNK`: `rosgap.com`, `bnkrf.ru`
  - `DOM`: `ahuel1.ru`, `dom.ru`
- одна AS: `SCOPE_AS = ("dom_itmon",)`
- несколько AS: `SCOPE_AS = ("dom_itmon", "risk_calc")`
- одна org на все AS: `GRAFANA_ORGIDS = (17,)`
- org по позиции: `GRAFANA_ORGIDS = (17, 23)`
- org-only audit: `GRAFANA_AUDIT_ORGIDS = (17,)`
- grafana apply mode: `GRAFANA_APPLY_CHANGES = False`
- все env: `SCOPE_ENV = ""`
- только nonprod: `SCOPE_ENV = "NONPROD"`
- только prod: `SCOPE_ENV = "PROD"`

Минимальный поток:
1. В `config.py` задать `SCOPE_AS` и при необходимости `SCOPE_ENV`.
2. Запустить `python audit_scope.py`.
3. Проверить `mapping_plan_v2_*.xlsx` и отметить нужные строки `selected=yes`.
4. Если нужен отдельный разбор Grafana:
   - задать `GRAFANA_AUDIT_ORGIDS`
   - запустить `python grafana_org_audit.py`
   - запустить `python build_grafana_plan.py`
   - отметить `apply=yes` в `grafana_plan_*.xlsx`
   - запустить `python apply_grafana_plan.py`
5. В `config.py` указать:
   - `SOURCE_AUDIT_JSON`
   - `SOURCE_MAPPING_PLAN_XLSX`
6. Запустить `python build_impact_plan.py`.
7. В `config.py` указать:
   - `SOURCE_IMPACT_PLAN_JSON`
8. Запустить `python make_backup.py`.
9. В `config.py` указать:
   - `SOURCE_BACKUP_FILE`
10. Запустить `python verify_backup.py`.
11. Для dry-run:
   - `python apply_changes.py --target zabbix`
   - `python apply_changes.py --target grafana`
12. Для реального применения:
   - `python apply_changes.py --target zabbix --apply`
   - `python apply_changes.py --target grafana --apply`
