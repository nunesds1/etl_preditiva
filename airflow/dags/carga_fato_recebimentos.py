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
    dag_id="carga_fato_recebimentos",
    default_args=default_args,
    description="Carga incremental da FatoRecebimentos",
    schedule_interval="0 0 * * *",  # todo dia à meia-noite
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_fato_recebimentos():
        print("\n=== Iniciando carga da FatoRecebimentos ===")

        # --------------------------
        # Conexões
        # --------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # --------------------------
        # 1. Buscar maior RecebimentoID carregado
        # --------------------------
        sql_cur.execute("SELECT ISNULL(MAX(RecebimentoID), 0) FROM dw.FatoRecebimentos;")
        last_id = sql_cur.fetchone()[0]

        print(f"Último RecebimentoID carregado: {last_id}")

        # --------------------------
        # 2. Buscar novos registros no PostgreSQL
        # --------------------------
        pg_query = """
            SELECT 
                cr.id AS RecebimentoID,
                cr.id_parcela AS ParcelaID,
                cr.valor_original,
                cr.valor_atual,
                cr.data_recebimento,
                cr.vencimento,
                cr.id_forma_pagamento,
                cr.id_situacao,
                p.id_nota_fiscal,
                nf.id_cliente
            FROM financeiro.conta_receber cr
            JOIN vendas.parcela p ON p.id = cr.id_parcela
            JOIN vendas.nota_fiscal nf ON nf.id = p.id_nota_fiscal
            WHERE cr.id > %s
            ORDER BY cr.id;
        """

        pg_cur.execute(pg_query, (last_id,))
        rows = pg_cur.fetchall()

        print(f"Encontrados {len(rows)} novos títulos a receber.")

        # --------------------------
        # 3. Inserção na FatoRecebimentos
        # --------------------------
        inserted = 0

        for (
            rec_id,
            parcela_id,
            valor_original,
            valor_atual,
            data_recebimento,
            vencimento,
            forma_pagto_id,
            situacao_id,
            nota_fiscal_id,
            cliente_id
        ) in rows:

            # -------------------------
            # LOOKUPS das dimensões
            # -------------------------

            # Parcela
            sql_cur.execute("SELECT ParcelaKey FROM dw.DimParcela WHERE ParcelaID = ?", (parcela_id,))
            parcela_row = sql_cur.fetchone()
            if not parcela_row:
                print(f"[WARN] ParcelaID {parcela_id} não encontrada. Pulando.")
                continue
            parcela_key = parcela_row[0]

            # Cliente
            sql_cur.execute("SELECT ClienteKey FROM dw.DimCliente WHERE ClienteID = ?", (cliente_id,))
            cliente_row = sql_cur.fetchone()
            if not cliente_row:
                print(f"[WARN] ClienteID {cliente_id} não encontrado. Pulando.")
                continue
            cliente_key = cliente_row[0]

            # Forma Pagamento
            sql_cur.execute("SELECT FormaPagamentoKey FROM dw.DimFormaPagamento WHERE FormaPagamentoID = ?", (forma_pagto_id,))
            fp_row = sql_cur.fetchone()
            forma_pagto_key = fp_row[0] if fp_row else None

            # Situação do Título
            sql_cur.execute("SELECT SituacaoTituloKey FROM dw.DimSituacaoTitulo WHERE SituacaoTituloID = ?", (situacao_id,))
            sit_row = sql_cur.fetchone()
            situacao_key = sit_row[0] if sit_row else None

            # Nota Fiscal
            sql_cur.execute("SELECT NotaFiscalKey FROM dw.DimNotaFiscal WHERE NotaFiscalID = ?", (nota_fiscal_id,))
            nf_row = sql_cur.fetchone()
            nota_fiscal_key = nf_row[0] if nf_row else None

            # -------------------------
            # Date Keys
            # -------------------------
            venc_key = int(vencimento.strftime("%Y%m%d"))

            if data_recebimento:
                receb_key = int(data_recebimento.strftime("%Y%m%d"))
                dias_atraso = (data_recebimento - vencimento).days
                valor_recebido = valor_atual
            else:
                receb_key = None
                dias_atraso = None
                valor_recebido = None

            # -------------------------
            # Inserção
            # -------------------------
            sql_cur.execute("""
                INSERT INTO dw.FatoRecebimentos (
                    RecebimentoID,
                    ParcelaKey,
                    ClienteKey,
                    FormaPagamentoKey,
                    SituacaoTituloKey,
                    NotaFiscalKey,
                    DataVencimentoKey,
                    DataRecebimentoKey,
                    ValorOriginal,
                    ValorAtual,
                    ValorRecebido,
                    DiasEmAtraso
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rec_id,
                parcela_key,
                cliente_key,
                forma_pagto_key,
                situacao_key,
                nota_fiscal_key,
                venc_key,
                receb_key,
                valor_original,
                valor_atual,
                valor_recebido,
                dias_atraso
            ))

            inserted += 1

        # --------------------------
        # Commit e fechamento
        # --------------------------
        sql_conn.commit()
        pg_conn.close()
        sql_conn.close()

        print(f"FatoRecebimentos - {inserted} registros inseridos com sucesso.")

    load_fato_recebimentos()
