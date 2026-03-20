# v2 Audit And Backup

Подробный документ:
- `v2/MANUAL.md`

Новый контур `v2/` живет отдельно от старого пайплайна и сейчас покрывает первые три безопасных шага:
- read-only аудит;
- backup по готовому `json inventory`;
- verify backup по тому же inventory.

Что делает `python -m v2.audit_scope`:
- читает Zabbix без изменений;
- берет scope по AS из `v2/config.py`;
- опционально режет выборку по каноническому `ENV` для pilot-прогонов;
- собирает инвентаризацию хостов, OLD/NEW групп, actions, usergroups, maintenances;
- отдельно ищет упоминания в Grafana dashboards;
- сохраняет `xlsx` и `json` в каталог `v2_output/`.

Что делает `python -m v2.make_backup`:
- читает `SOURCE_INVENTORY_JSON` из `v2/config.py`;
- забирает из Zabbix все сущности по точным ID из inventory;
- сохраняет backup в `json.gz` в каталог `v2_output/`;
- падает с `RuntimeError`, если хотя бы часть inventory не покрыта backup.

Что делает `python -m v2.verify_backup`:
- читает `SOURCE_INVENTORY_JSON` и `SOURCE_BACKUP_FILE`;
- сверяет scope и набор ID между inventory и backup;
- падает с `RuntimeError`, если есть missing/extra объекты.

Логика `ENV` в `v2` фиксированная:
- `PROD` остается `PROD`;
- любое другое непустое значение считается `NONPROD`.

Что принципиально не делает:
- не строит боевой migration plan;
- не делает restore;
- не меняет Zabbix;
- не меняет Grafana.

Точки настройки:
- `v2/config.py`
  - URL/логины/пароли
  - `SCOPE_AS`
  - `SCOPE_ENVS`
  - `SOURCE_INVENTORY_JSON`
  - `SOURCE_BACKUP_FILE`
  - имена тегов
  - regex исключаемых групп
  - runtime-параметры нового контура

Рекомендуемый pilot:
1. В `v2/config.py` задать одну AS.
2. Для первого прогона задать `SCOPE_ENVS = ("NONPROD",)`.
3. Запустить `python -m v2.audit_scope`.
4. Проверить `v2_output/*.xlsx` и `v2_output/*.json`.
5. Прописать путь к JSON в `SOURCE_INVENTORY_JSON`.
6. Запустить `python -m v2.make_backup`.
7. Прописать путь к backup в `SOURCE_BACKUP_FILE`.
8. Запустить `python -m v2.verify_backup`.
9. Только после этого переходить к будущему `migrate v2`.
