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
    dag_id="carga_dim_cliente",
    default_args=default_args,
    description="Carga incremental da DimCliente a cada 10 minutos",
    schedule_interval="*/10 * * * *",  # a cada 10 minutos
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_dim_cliente():
        print("\n=== Iniciando carga da DimCliente ===")

        # ---------------------------
        # Conexões
        # ---------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # ---------------------------
        # 1. Busca maior ID já carregado
        # ---------------------------
        sql_cur.execute("SELECT ISNULL(MAX(ClienteID), 0) FROM dw.DimCliente;")
        last_id = sql_cur.fetchone()[0]

        print(f"Último ClienteID carregado: {last_id}")

        # ---------------------------
        # 2. Buscar novos clientes no Postgres
        # ---------------------------
        pg_sql = """
            SELECT DISTINCT id_cliente, cliente, tipo_pessoa
            FROM (
                SELECT pf.id AS id_cliente, 
                       pf.nome AS cliente,
                       'Pessoa Física'::text AS tipo_pessoa
                FROM vendas.nota_fiscal nf
                JOIN geral.pessoa p ON p.id = nf.id_cliente
                JOIN geral.pessoa_fisica pf ON pf.id = p.id
                GROUP BY pf.id, pf.nome

                UNION ALL

                SELECT pj.id AS id_cliente, 
                       pj.razao_social AS cliente,
                       'Pessoa Jurídica'::text AS tipo_pessoa
                FROM vendas.nota_fiscal nf
                JOIN geral.pessoa p ON p.id = nf.id_cliente
                JOIN geral.pessoa_juridica pj ON pj.id = p.id
                GROUP BY pj.id, pj.razao_social
            ) tb
            WHERE id_cliente > %s
            ORDER BY id_cliente ASC;
        """

        pg_cur.execute(pg_sql, (last_id,))
        rows = pg_cur.fetchall()

        print(f"Novos clientes encontrados: {len(rows)}")

        # ---------------------------
        # 3. Inserir no SQL Server
        # ---------------------------
        inserted = 0
        for cliente_id, nome, tipo_pessoa in rows:
            sql_cur.execute("""
                INSERT INTO dw.DimCliente (ClienteID, Nome, Tipo_Pessoa)
                VALUES (?, ?, ?)
            """, (cliente_id, nome, tipo_pessoa))
            inserted += 1

        sql_conn.commit()

        # Fechar conexões
        pg_cur.close()
        pg_conn.close()
        sql_cur.close()
        sql_conn.close()

        print(f"DimCliente - {inserted} registros inseridos.")

    load_dim_cliente()
