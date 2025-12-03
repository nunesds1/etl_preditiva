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
    dag_id="carga_fato_vendas",
    default_args=default_args,
    description="Carga incremental da FatoVendas",
    schedule_interval="0 0 * * *",   # todo dia à meia-noite
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_fato_vendas():
        print("\n=== Iniciando carga da FatoVendas ===")

        # -----------------------------------------
        # Conexões
        # -----------------------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # -----------------------------------------
        # 1. Buscar maior chave já carregada
        # -----------------------------------------
        sql_cur.execute("SELECT ISNULL(MAX(VendasKey), 0) FROM dw.FatoVendas;")
        last_vendas_key = sql_cur.fetchone()[0]

        print(f"Último VendasKey carregado: {last_vendas_key}")

        # -----------------------------------------
        # 2. Buscar novos itens de venda no Postgres
        # -----------------------------------------
        pg_query = """
            SELECT 
                inf.id AS ItemID,
                inf.id_nota_fiscal,
                inf.id_produto,
                inf.quantidade,
                inf.valor_unitario,
                inf.valor_venda_real,
                p.valor_custo,
                nf.id_cliente,
                nf.id_vendedor,
                nf.id_forma_pagto,
                nf.data_venda,
                nf.numero_nf
            FROM vendas.item_nota_fiscal inf
            JOIN vendas.nota_fiscal nf ON nf.id = inf.id_nota_fiscal
            JOIN vendas.produto p ON p.id = inf.id_produto
            WHERE inf.id > %s
            ORDER BY inf.id;
        """

        pg_cur.execute(pg_query, (last_vendas_key,))
        rows = pg_cur.fetchall()

        print(f"Encontrados {len(rows)} novos registros para carga.")

        # -----------------------------------------
        # 3. Inserção na FatoVendas
        # -----------------------------------------
        inserted = 0

        for (
            item_id,
            nota_id,
            produto_id,
            quantidade,
            valor_unitario,
            valor_venda_real,
            valor_custo,
            cliente_id,
            vendedor_id,
            forma_pagto_id,
            data_venda,
            numero_nf
        ) in rows:

            # Lookup Produto
            sql_cur.execute("SELECT ProdutoKey FROM dw.DimProduto WHERE ProdutoID = ?", (produto_id,))
            produto_row = sql_cur.fetchone()
            if not produto_row:
                print(f"[WARN] ProdutoID {produto_id} não encontrado. Pulando.")
                continue
            produto_key = produto_row[0]

            # Lookup Cliente
            sql_cur.execute("SELECT ClienteKey FROM dw.DimCliente WHERE ClienteID = ?", (cliente_id,))
            cliente_row = sql_cur.fetchone()
            if not cliente_row:
                print(f"[WARN] ClienteID {cliente_id} não encontrado. Pulando.")
                continue
            cliente_key = cliente_row[0]

            # Lookup Vendedor
            sql_cur.execute("SELECT VendedorKey FROM dw.DimVendedor WHERE VendedorID = ?", (vendedor_id,))
            vendedor_row = sql_cur.fetchone()
            if not vendedor_row:
                print(f"[WARN] VendedorID {vendedor_id} não encontrado. Pulando.")
                continue
            vendedor_key = vendedor_row[0]

            # Lookup FormaPagamento
            sql_cur.execute("SELECT FormaPagamentoKey FROM dw.DimFormaPagamento WHERE FormaPagamentoID = ?", (forma_pagto_id,))
            fp_row = sql_cur.fetchone()
            if not fp_row:
                print(f"[WARN] FormaPagamentoID {forma_pagto_id} não encontrado. Pulando.")
                continue
            forma_pagto_key = fp_row[0]

            # Lookup NotaFiscal
            sql_cur.execute("SELECT NotaFiscalKey FROM dw.DimNotaFiscal WHERE NotaFiscalID = ?", (nota_id,))
            nf_row = sql_cur.fetchone()
            if not nf_row:
                print(f"[WARN] NotaFiscalID {nota_id} não encontrado. Pulando.")
                continue
            nota_fiscal_key = nf_row[0]

            # DateKey
            date_key = int(data_venda.strftime("%Y%m%d"))

            # Medidas
            valor_custo_total = valor_custo * quantidade
            margem_bruta = valor_venda_real - valor_custo_total

            # Insert
            sql_cur.execute("""
                INSERT INTO dw.FatoVendas (
                    DateKey,
                    ProdutoKey,
                    ClienteKey,
                    VendedorKey,
                    FormaPagamentoKey,
                    NotaFiscalKey,
                    NumeroNF,
                    Quantidade,
                    ValorUnitario,
                    ValorVendaReal,
                    ValorCustoTotal,
                    MargemBruta
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date_key,
                produto_key,
                cliente_key,
                vendedor_key,
                forma_pagto_key,
                nota_fiscal_key,
                numero_nf,
                quantidade,
                valor_unitario,
                valor_venda_real,
                valor_custo_total,
                margem_bruta
            ))

            inserted += 1

        # -----------------------------------------
        # 4. Commit & fechar
        # -----------------------------------------
        sql_conn.commit()
        pg_conn.close()
        sql_conn.close()

        print(f"FatoVendas - {inserted} registros inseridos com sucesso.")

    load_fato_vendas()
