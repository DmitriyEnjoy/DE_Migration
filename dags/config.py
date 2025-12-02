# dags/config.py
# Список таблиц, которые нужно мигрировать. Каждая запись — dict:
# {
#   "schema": "public",
#   "table": "users",
#   "mode": "replace" | "append",
#   "where": "created_at > '2025-01-01'"  # optional
# }

TABLES_TO_MIGRATE = [
    {"schema": "public", "table": "users", "mode": "replace"},
    {"schema": "public", "table": "orders", "mode": "append", "where": "created_at >= '2025-01-01'"},
]
