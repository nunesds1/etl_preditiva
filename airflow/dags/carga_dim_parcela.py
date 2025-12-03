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
    dag_id="carga_dim_parcela",
    default_args=default_args,
    description="Carga incremental da DimParcela",
    schedule_interval="*/5 * * * *",   # a cada 5 minutos
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_dim_parcela():
        print("\n=== Iniciando carga da DimParcela ===")

        # -----------------------------------------
        # Conexões
        # -----------------------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # -----------------------------------------
        # 1. Buscar o maior ParcelaID já carregado
        # -----------------------------------------
        sql_cur.execute("SELECT ISNULL(MAX(ParcelaID), 0) FROM dw.DimParcela;")
        last_id = sql_cur.fetchone()[0]

        print(f"Último ParcelaID carregado: {last_id}")

        # -----------------------------------------
        # 2. Buscar novas parcelas no Postgres
        # -----------------------------------------
        pg_query = """
            SELECT 
                id AS ParcelaID,
                id_nota_fiscal AS NotaFiscalID,
                numero AS NumeroParcela,
                valor AS ValorParcela,
                vencimento
            FROM vendas.parcela
            WHERE id > %s
            ORDER BY id;
        """

        pg_cur.execute(pg_query, (last_id,))
        rows = pg_cur.fetchall()

        print(f"Encontrados {len(rows)} novos registros de parcela.")

        # -----------------------------------------
        # 3. Inserir no SQL Server
        # -----------------------------------------
        inserted = 0

        for parcela_id, nota_id, numero, valor, vencimento in rows:

            # Se não tiver data → 1900-01-01
            if vencimento is None:
                date_key = 19000101
            else:
                date_key = int(vencimento.strftime("%Y%m%d"))

            sql_cur.execute("""
                INSERT INTO dw.DimParcela (
                    ParcelaID,
                    NotaFiscalID,
                    NumeroParcela,
                    ValorParcela,
                    Vencimento,
                    DateKey
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (parcela_id, nota_id, numero, valor, vencimento, date_key))

            inserted += 1

        # -----------------------------------------
        # 4. Commit e fechamento
        # -----------------------------------------
        sql_conn.commit()
        pg_conn.close()
        sql_conn.close()

        print(f"DimParcela - {inserted} registros inseridos com sucesso.")

    load_dim_parcela()
