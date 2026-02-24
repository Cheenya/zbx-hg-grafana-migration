# План безопасного запуска (точечно по AS)

Документ описывает безопасный порядок действий для аудита и миграции **только по одной или нескольким AS**.

## 0) Подготовка

1. Убедитесь, что заполнены:
   - `config.py`: `ZBX_URL`, `ZBX_USER`, `ZBX_PASSWORD`
   - `config.py`: `GRAFANA_URL`, `GRAFANA_USER`, `GRAFANA_PASSWORD` (token опционален)
2. Проверьте `CONFIG.runtime.limit_as = None` (не ограничивайте список случайно).
3. Заполните `CONFIG.runtime.audit_scope_as` в `config.py` (скрипты используют только конфиг).

## 1) Scoped аудит Zabbix (+ Grafana при необходимости)

1. Запустите аудит **по AS**:
   ```bash
   python audit_scope.py
   ```
2. Убедитесь, что:
   - листы `UNKNOWN_HOSTS`, `MAPPING` заполнены корректно;
   - `env_mismatch` отмечает проблемные пары;
   - секция `3) Trigger actions` содержит ожидаемые recipients/media;
   - секция `4) User groups` содержит ожидаемые права и `users_media`;
   - в Grafana‑секции присутствуют `OLD_PATTERN/NEW_PATTERN` (если есть регэкспы/переменные).
3. Проверьте и при необходимости отредактируйте план миграции:
   - файл `<output_xlsx>_migration_plan.json`
   - можно выключать пары через `enabled=false` или править `old_group/new_group`.

Если Grafana недоступна — можно отключить `CONFIG.runtime.enable_grafana_audit = False` и запустить Grafana отдельно (см. шаг 3).

## 2) Scoped бэкап (обязателен перед изменениями)

1. Сделайте **частный бэкап** по тем же AS:
   ```bash
   python make_backup.py
   ```
2. Убедитесь, что файл создан и не пустой (`.json.gz`).

## 3) Grafana‑only аудит (если нужно отдельно)

1. Убедитесь, что есть seed от Zabbix:
   - файл `<output_xlsx>_zbx_seed.json` (создаётся автоматически при аудите).
2. Запустите Grafana‑only:
   ```bash
   python grafana_only_audit.py --seed hostgroup_mapping_audit_zbx_seed.json
   ```
3. Проверьте листы Grafana по AS.

## 4) Миграция (точечно по одной AS)

1. Скрипт: `migrate_single_as.py`.
2. Берёт данные из плана миграции (`*_migration_plan.json`) и работает **только с одной AS**.
3. Что меняет:
   - **Хосты:** удаляет только OLD‑группы (NEW‑группы считаются уже существующими).
   - **Actions:** заменяет groupid в условиях и операциях.
   - **User groups:** заменяет groupid в `hostgroup_rights`.
   - **Maintenance:** заменяет groupid в `groups`.
   - **Grafana:** заменяет OLD‑группы в строковых полях JSON dashboards.
4. Изменяйте только заранее согласованный список полей.
5. Если добавляется новое поле к изменению — расширьте whitelist в `restore_backup.py`.

## 5) Откат (если нужно)

1. Используйте бэкап, созданный **под эти AS**:
   ```bash
   python restore_backup.py path/to/backup.json.gz
   ```
2. Проверьте на Zabbix:
   - группы хостов;
   - actions;
   - usergroups;
   - users.

## 6) Контрольные проверки

- Не запускать без AS‑scope (через `CONFIG.runtime.audit_scope_as`).
- Всегда сверяйте AS‑scope аудита и бэкапа.
- Для Grafana‑only убедитесь, что seed свежий.
