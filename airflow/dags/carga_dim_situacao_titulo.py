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
    dag_id="carga_dim_situacao_titulo",
    default_args=default_args,
    description="Carga incremental da DimSituacaoTitulo",
    schedule_interval="*/5 * * * *",  # a cada 5 minutos
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_dim_situacao_titulo():
        print("\n=== Iniciando carga da DimSituacaoTitulo ===")

        # -----------------------------------------
        # Conexões
        # -----------------------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # -----------------------------------------
        # 1. Buscar o maior SituacaoTituloID já carregado
        # -----------------------------------------
        sql_cur.execute("SELECT ISNULL(MAX(SituacaoTituloID), 0) FROM dw.DimSituacaoTitulo;")
        last_id = sql_cur.fetchone()[0]

        print(f"Último SituacaoTituloID carregado: {last_id}")

        # -----------------------------------------
        # 2. Buscar novas situações de título no Postgres
        # -----------------------------------------
        pg_query = """
            SELECT 
                id AS SituacaoTituloID,
                descricao AS Descricao
            FROM financeiro.situacao_titulo
            WHERE id > %s
            ORDER BY id;
        """

        pg_cur.execute(pg_query, (last_id,))
        rows = pg_cur.fetchall()

        print(f"Encontrados {len(rows)} novos registros.")

        # -----------------------------------------
        # 3. Inserção no SQL Server
        # -----------------------------------------
        inserted = 0

        for situacao_id, descricao in rows:
            sql_cur.execute("""
                INSERT INTO dw.DimSituacaoTitulo (
                    SituacaoTituloID,
                    Descricao
                )
                VALUES (?, ?)
            """, (situacao_id, descricao))

            inserted += 1

        # -----------------------------------------
        # 4. Commit e fechamento
        # -----------------------------------------
        sql_conn.commit()
        pg_conn.close()
        sql_conn.close()

        print(f"DimSituacaoTitulo - {inserted} registros inseridos com sucesso.")

    load_dim_situacao_titulo()
