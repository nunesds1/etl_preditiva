import sys
sys.path.append("/opt/airflow/scripts")
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta

from database import get_postgres_cursor, get_sqlserver_cursor


default_args = {
    "owner": "etl",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id="carga_dim_formapagamento",
    default_args=default_args,
    description="Carga incremental da DimFormaPagamento a cada 15 dias",
    schedule_interval="0 0 */15 * *",   # a cada 15 dias
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_dim_formapagamento():
        print("\n=== Iniciando carga da DimFormaPagamento ===")

        # ---------------------------
        # Conexões
        # ---------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # ---------------------------
        # 1. Busca maior ID já carregado
        # ---------------------------
        sql_cur.execute("SELECT ISNULL(MAX(FormaPagamentoID), 0) FROM dw.DimFormaPagamento;")
        last_id = sql_cur.fetchone()[0]

        print(f"Último FormaPagamentoID carregado: {last_id}")

        # ---------------------------
        # 2. Buscar novas formas de pagamento no PostgreSQL
        # ---------------------------
        pg_cur.execute("""
            SELECT id, descricao
            FROM vendas.forma_pagamento
            WHERE id > %s
            ORDER BY id;
        """, (last_id,))

        rows = pg_cur.fetchall()
        print(f"Novas formas de pagamento encontradas: {len(rows)}")

        # ---------------------------
        # 3. Inserir no SQL Server
        # ---------------------------
        inserted = 0
        for forma_id, descricao in rows:
            sql_cur.execute("""
                INSERT INTO dw.DimFormaPagamento (FormaPagamentoID, Descricao)
                VALUES (?, ?)
            """, (forma_id, descricao))
            inserted += 1

        sql_conn.commit()

        # ---------------------------
        # Fechar conexões
        # ---------------------------
        pg_cur.close()
        pg_conn.close()
        sql_cur.close()
        sql_conn.close()

        print(f"DimFormaPagamento - {inserted} registros inseridos.")

    load_dim_formapagamento()
