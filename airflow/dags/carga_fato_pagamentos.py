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
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="carga_fato_pagamentos",
    default_args=default_args,
    description="Carga incremental da FatoPagamentos",
    schedule_interval="0 0 * * *",  # todo dia à meia-noite
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_fato_pagamentos():
        print("\n=== Iniciando carga da FatoPagamentos ===")

        # -----------------------------------------
        # Conexões
        # -----------------------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # -----------------------------------------
        # 1. Buscar maior PagamentoID já carregado
        # -----------------------------------------
        sql_cur.execute("SELECT ISNULL(MAX(PagamentoID), 0) FROM dw.FatoPagamentos;")
        last_id = sql_cur.fetchone()[0]

        print(f"Último PagamentoID carregado: {last_id}")

        # -----------------------------------------
        # 2. Buscar novos pagamentos no Postgres
        # -----------------------------------------
        pg_query = """
            SELECT 
                cp.id AS PagamentoID,
                cp.documento,
                cp.emissao,
                cp.vencimento,
                cp.data_pagamento,
                cp.valor_original,
                cp.valor_atual,
                cp.id_forma_pagamento,
                cp.id_situacao,
                cp.descricao
            FROM financeiro.conta_pagar cp
            WHERE cp.id > %s
            ORDER BY cp.id;
        """

        pg_cur.execute(pg_query, (last_id,))
        rows = pg_cur.fetchall()

        print(f"Encontrados {len(rows)} novos pagamentos.")

        inserted = 0

        # -----------------------------------------
        # 3. Processamento linha a linha
        # -----------------------------------------
        for (
            pag_id,
            documento,
            emissao,
            vencimento,
            data_pagamento,
            valor_original,
            valor_atual,
            forma_pagto_id,
            situacao_id,
            descricao
        ) in rows:

            # ---- Dim Forma de Pagamento ----
            sql_cur.execute("""
                SELECT FormaPagamentoKey 
                FROM dw.DimFormaPagamento 
                WHERE FormaPagamentoID = ?
            """, (forma_pagto_id,))
            fp_row = sql_cur.fetchone()
            forma_pagto_key = fp_row[0] if fp_row else None

            # ---- Dim Situação ----
            sql_cur.execute("""
                SELECT SituacaoTituloKey 
                FROM dw.DimSituacaoTitulo 
                WHERE SituacaoTituloID = ?
            """, (situacao_id,))
            sit_row = sql_cur.fetchone()
            situacao_key = sit_row[0] if sit_row else None

            # ---- Date Keys ----
            emissao_key = int(emissao.strftime("%Y%m%d"))
            venc_key = int(vencimento.strftime("%Y%m%d"))

            if data_pagamento:
                pagto_key = int(data_pagamento.strftime("%Y%m%d"))
                dias_atraso = (data_pagamento - vencimento).days
                valor_pago = valor_atual
            else:
                pagto_key = None
                dias_atraso = None
                valor_pago = None

            # ---- INSERT Fato ----
            sql_cur.execute("""
                INSERT INTO dw.FatoPagamentos (
                    PagamentoID,
                    Documento,
                    FormaPagamentoKey,
                    SituacaoTituloKey,
                    DataEmissaoKey,
                    DataVencimentoKey,
                    DataPagamentoKey,
                    ValorOriginal,
                    ValorAtual,
                    ValorPago,
                    DiasEmAtraso
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pag_id,
                documento,
                forma_pagto_key,
                situacao_key,
                emissao_key,
                venc_key,
                pagto_key,
                valor_original,
                valor_atual,
                valor_pago,
                dias_atraso
            ))

            inserted += 1

        # -----------------------------------------
        # 4. Commit e fechamento
        # -----------------------------------------
        sql_conn.commit()
        pg_conn.close()
        sql_conn.close()

        print(f"FatoPagamentos - {inserted} registros inseridos com sucesso.")

    load_fato_pagamentos()
