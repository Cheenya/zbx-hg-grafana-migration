# v2 Read-Only Audit

Подробный документ:
- `v2/MANUAL.md`

Новый контур `v2/` живет отдельно от старого пайплайна и пока решает только одну задачу: дать понятную, read-only инвентаризацию перед любыми изменениями.

Что делает `python -m v2.audit_scope`:
- читает Zabbix без изменений;
- берет scope по AS из `v2/config.py`;
- опционально режет выборку по каноническому `ENV` для pilot-прогонов;
- собирает инвентаризацию хостов, OLD/NEW групп, actions, usergroups, maintenances;
- отдельно ищет упоминания в Grafana dashboards;
- сохраняет `xlsx` и `json` в каталог `v2_output/`.

Логика `ENV` в `v2` фиксированная:
- `PROD` остается `PROD`;
- любое другое непустое значение считается `NONPROD`.

Что принципиально не делает:
- не строит боевой migration plan;
- не генерирует backup;
- не меняет Zabbix;
- не меняет Grafana.

Точки настройки:
- `v2/config.py`
  - URL/логины/пароли
  - `SCOPE_AS`
  - `SCOPE_ENVS`
  - имена тегов
  - regex исключаемых групп
  - runtime-параметры нового контура

Рекомендуемый pilot:
1. В `v2/config.py` задать одну AS.
2. Для первого прогона задать `SCOPE_ENVS = ("NONPROD",)`.
3. Запустить `python -m v2.audit_scope`.
4. Проверить `v2_output/*.xlsx` и `v2_output/*.json`.
5. Только после этого проектировать `backup v2` и `migrate v2`.
