# dags/table_config.py
"""
Конфиг миграции таблиц.
Добавлять новые таблицы — просто дописывать элемент в TABLES.
Поля:
 - source_table: имя таблицы в источнике (schema.table)
 - dest_table: имя таблицы в приёмнике (schema.table)
 - mode: 'replace' | 'append' | 'skip_if_exists'
 - create_if_not_exists: True/False (если True, будет попытка скопировать структуру)
"""

TABLES = [
    {
        "source_table": "public.flowers",
        "dest_table": "public.flowers",
        "mode": "replace",
        "create_if_not_exists": True,
    },
    # Пример добавления второй таблицы:
    # {
    #   "source_table": "public.employees",
    #   "dest_table": "public.employees",
    #   "mode": "append",
    #   "create_if_not_exists": True,
    # },
]
