# dags/postgres_migration_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from table_config import TABLES

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="postgres_tables_migration",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # ручной запуск; при желании: '@daily'
    catchup=False,
    tags=["migration", "postgres"],
)

def migrate_table_task(source_conn_id: str, dest_conn_id: str, source_table: str, dest_table: str, mode: str, create_if_not_exists: bool, **context):
    """
    Копирует данные из source_table в dest_table.
    mode: 'replace' (truncate dest then copy), 'append', 'skip_if_exists'
    create_if_not_exists: если True — попробуем создать таблицу в приёмнике по колонкам источника
    """
    src_hook = PostgresHook(postgres_conn_id=source_conn_id)
    dst_hook = PostgresHook(postgres_conn_id=dest_conn_id)

    src_conn = src_hook.get_conn()
    dst_conn = dst_hook.get_conn()
    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()

    # Определим список колонок из источника
    src_cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = split_part('{source_table}', '.', 1) AND table_name = split_part('{source_table}', '.', 2) ORDER BY ordinal_position;")
    cols = [r[0] for r in src_cur.fetchall()]
    if not cols:
        raise ValueError(f"No columns found for {source_table}")

    col_list = ", ".join([f'"{c}"' for c in cols])

    # Создать таблицу в приёмнике при необходимости (упрощённо: создаём с текстовыми типами)
    if create_if_not_exists:
        # Попытка создать с подобием типов — лучше: разобрать data_type и конвертировать корректно
        create_sql = f"""
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = split_part('{dest_table}', '.', 1) AND table_name = split_part('{dest_table}', '.', 2)) THEN
            CREATE TABLE {dest_table} ({', '.join([f'"{c}" text' for c in cols])});
          END IF;
        END$$;
        """
        dst_cur.execute(create_sql)
        dst_conn.commit()

    # Если режим skip_if_exists и есть строки — пропускаем
    if mode == "skip_if_exists":
        dst_cur.execute(f"SELECT 1 FROM {dest_table} LIMIT 1;")
        if dst_cur.fetchone():
            src_cur.close()
            dst_cur.close()
            return "skipped"

    if mode == "replace":
        dst_cur.execute(f"TRUNCATE TABLE {dest_table};")
        dst_conn.commit()

    # Используем COPY TO STDOUT в source и COPY FROM STDIN в dest
    copy_sql_out = f"COPY (SELECT {col_list} FROM {source_table}) TO STDOUT WITH CSV DELIMITER ',' QUOTE '\"' ESCAPE '\\\\' NULL '';"
    copy_sql_in = f"COPY {dest_table} ({col_list}) FROM STDIN WITH CSV DELIMITER ',' QUOTE '\"' ESCAPE '\\\\' NULL '';"

    # psycopg2 метод copy_expert
    try:
        with src_conn, dst_conn:
            src_cur = src_conn.cursor()
            dst_cur = dst_conn.cursor()
            # Используем copy_expert потоково
            src_cur.copy_expert(copy_sql_out, dst_conn)  # trick: иногда удобней через bytes buffer; но Postgres allows copy from file-like
            # Однако copy_expert(target, file) usually used. Более надёжный способ — перенос через временный файл.
            # Для простоты: выполним dump -> fetchall -> insert batch (медленнее, но надёжно)

    except Exception as e:
        # fallback: простая реализация: SELECT и batch INSERT
        dst_conn.rollback()
        src_cur.execute(f"SELECT {col_list} FROM {source_table};")
        rows = src_cur.fetchall()
        if rows:
            # Формируем шаблон вставки
            placeholders = ", ".join(["%s"] * len(cols))
            insert_sql = f"INSERT INTO {dest_table} ({col_list}) VALUES ({placeholders})"
            dst_cur.executemany(insert_sql, rows)
            dst_conn.commit()

    finally:
        src_cur.close()
        dst_cur.close()

    return "migrated"

# Создаём задачу для каждой таблицы из конфига
start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("start migration"),
    dag=dag,
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("end migration"),
    dag=dag,
)

for tbl in TABLES:
    task_id = f"migrate__{tbl['source_table'].replace('.', '_')}"
    op = PythonOperator(
        task_id=task_id,
        python_callable=migrate_table_task,
        op_kwargs={
            "source_conn_id": "source_postgres",
            "dest_conn_id": "dest_postgres",
            "source_table": tbl["source_table"],
            "dest_table": tbl["dest_table"],
            "mode": tbl.get("mode", "replace"),
            "create_if_not_exists": tbl.get("create_if_not_exists", True),
        },
        dag=dag,
    )
    start >> op >> end
