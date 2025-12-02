# dags/pg_migration_dag.py
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
import io

from dags.config import TABLES_TO_MIGRATE

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='pg_to_pg_table_migration',
    default_args=default_args,
    schedule_interval=None,  # запуск вручную или по расписанию на твой вкус
    start_date=days_ago(1),
    catchup=False,
    tags=['migration','postgres'],
) as dag:

    @task
    def migrate_table(table_config: dict):
        """
        table_config keys: schema, table, mode ('replace' or 'append'), optional where
        """
        schema = table_config['schema']
        table = table_config['table']
        mode = table_config.get('mode', 'replace')
        where = table_config.get('where')

        src_hook = PostgresHook(postgres_conn_id='src_postgres')
        dst_hook = PostgresHook(postgres_conn_id='dst_postgres')

        src_conn = src_hook.get_conn()
        dst_conn = dst_hook.get_conn()
        src_cur = src_conn.cursor()
        dst_cur = dst_conn.cursor()

        # 1) Ensure destination table exists (simple DDL build from information_schema)
        dst_cur.execute("""
            SELECT EXISTS(
              SELECT 1 FROM information_schema.tables
              WHERE table_schema=%s AND table_name=%s
            )
        """, (schema, table))
        exists = dst_cur.fetchone()[0]

        if not exists:
            # Build basic CREATE TABLE using source metadata
            src_cur.execute("""
                SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position
            """, (schema, table))
            cols = src_cur.fetchall()
            col_defs = []
            for col in cols:
                col_name, data_type, char_len, num_prec, num_scale, is_nullable = col
                # Basic mapping for common types
                if data_type == 'character varying' and char_len:
                    typ = f"VARCHAR({char_len})"
                elif data_type == 'character':
                    typ = 'CHAR'
                elif data_type == 'integer':
                    typ = 'INTEGER'
                elif data_type == 'bigint':
                    typ = 'BIGINT'
                elif data_type == 'numeric' and num_prec:
                    if num_scale:
                        typ = f"NUMERIC({num_prec},{num_scale})"
                    else:
                        typ = f"NUMERIC({num_prec})"
                elif data_type == 'timestamp without time zone':
                    typ = 'TIMESTAMP'
                elif data_type == 'timestamp with time zone':
                    typ = 'TIMESTAMPTZ'
                else:
                    typ = data_type.upper()
                null = '' if is_nullable == 'NO' else 'NULL'
                col_defs.append(f'"{col_name}" {typ} {null}')
            cols_ddl = ", ".join(col_defs) if col_defs else ''
            create_sql = f'CREATE SCHEMA IF NOT EXISTS "{schema}"; CREATE TABLE "{schema}"."{table}" ({cols_ddl});'
            dst_cur.execute(create_sql)
            dst_conn.commit()

        # 2) If replace mode -> truncate dest table
        if mode == 'replace':
            dst_cur.execute(f'TRUNCATE TABLE "{schema}"."{table}"')
            dst_conn.commit()

        # 3) COPY from source to memory buffer and COPY into dest
        select_sql = f'SELECT * FROM "{schema}"."{table}"'
        if where:
            select_sql = select_sql + f' WHERE {where}'
        copy_out_sql = f"COPY ({select_sql}) TO STDOUT WITH CSV"

        buffer = io.StringIO()
        src_cur.copy_expert(copy_out_sql, buffer)
        buffer.seek(0)
        copy_in_sql = f'COPY "{schema}"."{table}" FROM STDIN WITH CSV'
        dst_cur.copy_expert(copy_in_sql, buffer)
        dst_conn.commit()

        # close cursors
        src_cur.close()
        dst_cur.close()
        src_conn.close()
        dst_conn.close()

        return f"Finished migrating {schema}.{table}"

    # динамически создаём таски для каждой таблицы
    tasks = []
    for t in TABLES_TO_MIGRATE:
        tasks.append(migrate_table.override(task_id=f"migrate__{t['schema']}__{t['table']}")(t))

    # можно задать порядок, если нужно: например все параллельно:
    # tasks are independent; если хочешь последовательность, можно chain
