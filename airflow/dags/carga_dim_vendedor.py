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
    dag_id="carga_dim_vendedor",
    default_args=default_args,
    description="Carga incremental da DimVendedor",
    schedule_interval="*/5 * * * *",   # a cada 5 minutos
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_dim_vendedor():
        print("\n=== Iniciando carga da DimVendedor ===")

        # -----------------------------------------
        # Conexões
        # -----------------------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # -----------------------------------------
        # 1. Buscar maior VendedorID já carregado
        # -----------------------------------------
        sql_cur.execute("SELECT ISNULL(MAX(VendedorID), 0) FROM dw.DimVendedor;")
        last_id = sql_cur.fetchone()[0]

        print(f"Último VendedorID carregado: {last_id}")

        # -----------------------------------------
        # 2. Buscar novos vendedores no Postgres
        # -----------------------------------------
        pg_query = """
            SELECT DISTINCT 
                id_vendedor,
                vendedor
            FROM (
                SELECT 
                    nf.id_vendedor,
                    pfv.nome AS vendedor
                FROM vendas.nota_fiscal nf
                JOIN geral.pessoa_fisica pfv 
                    ON pfv.id = nf.id_vendedor
            ) tb
            WHERE id_vendedor > %s
            ORDER BY id_vendedor ASC;
        """

        pg_cur.execute(pg_query, (last_id,))
        rows = pg_cur.fetchall()

        print(f"Encontrados {len(rows)} novos vendedores.")

        # -----------------------------------------
        # 3. Inserir no DW
        # -----------------------------------------
        inserted = 0

        for vendedor_id, vendedor_nome in rows:
            sql_cur.execute("""
                INSERT INTO dw.DimVendedor (
                    VendedorID,
                    Nome
                )
                VALUES (?, ?)
            """, (vendedor_id, vendedor_nome))

            inserted += 1

        # -----------------------------------------
        # 4. Commit & fechar
        # -----------------------------------------
        sql_conn.commit()
        pg_conn.close()
        sql_conn.close()

        print(f"DimVendedor - {inserted} registros inseridos com sucesso.")

    load_dim_vendedor()
