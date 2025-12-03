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
    dag_id="carga_dim_fornecedor",
    default_args=default_args,
    description="Carga incremental da DimFornecedor",
    schedule_interval="0 0 * * *",   # meia-noite diariamente
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_dim_fornecedor():
        print("\n=== Iniciando carga da DimFornecedor ===")

        # -----------------------------------------
        # Conexões
        # -----------------------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # -----------------------------------------
        # 1. Busca o maior ID já carregado no DW
        # -----------------------------------------
        sql_cur.execute("SELECT ISNULL(MAX(FornecedorID), 0) FROM dw.DimFornecedor;")
        last_id = sql_cur.fetchone()[0]

        print(f"Último FornecedorID carregado: {last_id}")

        # -----------------------------------------
        # 2. Buscar novos fornecedores no Postgres
        # -----------------------------------------
        pg_query = """
            SELECT DISTINCT 
                a.id_fornecedor AS FornecedorID,
                b.razao_social AS Nome
            FROM vendas.produto a
            LEFT JOIN geral.pessoa_juridica b 
                ON a.id_fornecedor = b.id
            WHERE a.id_fornecedor IS NOT NULL
              AND a.id_fornecedor > %s
            ORDER BY a.id_fornecedor;
        """

        pg_cur.execute(pg_query, (last_id,))
        new_rows = pg_cur.fetchall()

        print(f"Encontrados {len(new_rows)} novos fornecedores.")

        # -----------------------------------------
        # 3. Inserir no SQL Server
        # -----------------------------------------
        inserted = 0

        for fornecedor_id, nome in new_rows:
            sql_cur.execute("""
                INSERT INTO dw.DimFornecedor (FornecedorID, Nome)
                VALUES (?, ?)
            """, (fornecedor_id, nome))
            inserted += 1

        sql_conn.commit()

        # Fechar conexões
        pg_conn.close()
        sql_conn.close()

        print(f"DimFornecedor - {inserted} registros inseridos com sucesso.")

    load_dim_fornecedor()
