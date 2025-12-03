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
    dag_id="carga_dim_notafiscal",
    default_args=default_args,
    description="Carga incremental da DimNotaFiscal a cada 5 minutos",
    schedule_interval="*/5 * * * *",  # a cada 5 min
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_dim_nota_fiscal():
        print("\n=== Iniciando carga da DimNotaFiscal ===")

        # -----------------------------------------
        # Conexões
        # -----------------------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # -----------------------------------------
        # 1. Busca o maior NotaFiscalID já carregado
        # -----------------------------------------
        sql_cur.execute("SELECT ISNULL(MAX(NotaFiscalID), 0) FROM dw.DimNotaFiscal;")
        last_id = sql_cur.fetchone()[0]

        print(f"Último NotaFiscalID carregado: {last_id}")

        # -----------------------------------------
        # 2. Buscar novas notas fiscais no Postgres
        # -----------------------------------------
        pg_query = """
            SELECT 
                id AS NotaFiscalID,
                numero_nf AS NumeroNF,
                valor AS ValorTotal,
                data_venda
            FROM vendas.nota_fiscal
            WHERE id > %s
            ORDER BY id;
        """

        pg_cur.execute(pg_query, (last_id,))
        rows = pg_cur.fetchall()

        print(f"Encontrados {len(rows)} novos registros de Nota Fiscal.")

        # -----------------------------------------
        # 3. Insert no SQL Server
        # -----------------------------------------
        inserted = 0

        for nf_id, numero_nf, valor_total, data_venda in rows:

            # DateKey convertido para inteiro YYYYMMDD
            date_key = int(data_venda.strftime("%Y%m%d"))

            sql_cur.execute("""
                INSERT INTO dw.DimNotaFiscal (
                    NotaFiscalID,
                    NumeroNF,
                    ValorTotal,
                    DateKey
                )
                VALUES (?, ?, ?, ?)
            """, (nf_id, numero_nf, valor_total, date_key))

            inserted += 1

        # -----------------------------------------
        # 4. Commit e fechamento
        # -----------------------------------------
        sql_conn.commit()
        pg_conn.close()
        sql_conn.close()

        print(f"DimNotaFiscal - {inserted} registros inseridos com sucesso.")

    load_dim_nota_fiscal()
