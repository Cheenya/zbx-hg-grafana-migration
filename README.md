# Safe Prep Flow

Подробный документ:
- `MANUAL.md`

Это основной контур подготовки миграции.

Что уже есть:
- `python audit_scope.py` — read-only аудит Zabbix/Grafana;
- `python build_impact_plan.py` — сбор change-scope по подтверждённому `mapping_plan.xlsx`;
- `python make_backup.py` — backup строго по `impact_plan.json`;
- `python verify_backup.py` — проверка backup против `impact_plan.json`;
- `python restore_backup.py` — откат Zabbix из backup.

Что делает аудит:
- читает Zabbix без изменений;
- берёт scope по `SCOPE_AS` и опционально по каноническому `SCOPE_ENV`;
- опционально маппит `SCOPE_AS` на `GRAFANA_ORGIDS`;
- учитывает `AS`, `ASN`, `ENV`, `GAS`, `GUEST-NAME`;
- отдельно показывает `UNKNOWN`-хосты;
- строит `MAPPING_PLAN` c кандидатами `OLD -> NEW`;
- строит `HOST_ENRICHMENT` по хостам;
- ищет только `OLD`-группы в Grafana dashboards;
- сохраняет:
  - `scope_audit_v2_*.xlsx`
  - `scope_audit_v2_*.json`
  - `mapping_plan_v2_*.xlsx`
  - `grafana_audit_v2_*.xlsx`

Что делает `build_impact_plan.py`:
- читает `SOURCE_AUDIT_JSON`;
- читает `SOURCE_MAPPING_PLAN_XLSX`;
- берёт только строки, где в `mapping_plan.xlsx` стоит `selected=yes`;
- строит:
  - точные change points в Zabbix;
  - exact/pattern impact по Grafana;
  - `backup_scope` для backup/restore;
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

Логика `ENV`:
- `PROD` -> `PROD`
- любое другое непустое значение -> `NONPROD`

Что важно:
- Grafana сейчас работает по логину/паролю;
- контур пока не применяет миграцию;
- контур пока только готовит и проверяет change-scope.

Формат scope:
- одна AS: `SCOPE_AS = ("dom_itmon",)`
- несколько AS: `SCOPE_AS = ("dom_itmon", "risk_calc")`
- одна org на все AS: `GRAFANA_ORGIDS = (17,)`
- org по позиции: `GRAFANA_ORGIDS = (17, 23)`
- все env: `SCOPE_ENV = ""`
- только nonprod: `SCOPE_ENV = "NONPROD"`
- только prod: `SCOPE_ENV = "PROD"`

Минимальный поток:
1. В `config.py` задать `SCOPE_AS` и при необходимости `SCOPE_ENV`.
2. Запустить `python audit_scope.py`.
3. Проверить `mapping_plan_v2_*.xlsx` и отметить нужные строки `selected=yes`.
4. В `config.py` указать:
   - `SOURCE_AUDIT_JSON`
   - `SOURCE_MAPPING_PLAN_XLSX`
5. Запустить `python build_impact_plan.py`.
6. В `config.py` указать:
   - `SOURCE_IMPACT_PLAN_JSON`
7. Запустить `python make_backup.py`.
8. В `config.py` указать:
   - `SOURCE_BACKUP_FILE`
9. Запустить `python verify_backup.py`.
10. Только после этого идти к будущему `migrate`.
