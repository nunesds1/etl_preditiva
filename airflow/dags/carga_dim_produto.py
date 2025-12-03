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
    dag_id="carga_dim_produto",
    default_args=default_args,
    description="Carga incremental da DimProduto",
    schedule_interval="*/5 * * * *",   # a cada 5 minutos
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    @task
    def load_dim_produto():
        print("\n=== Iniciando carga da DimProduto ===")

        # -----------------------------------------
        # Conexões
        # -----------------------------------------
        pg_conn, pg_cur = get_postgres_cursor()
        sql_conn, sql_cur = get_sqlserver_cursor()

        # -----------------------------------------
        # 1. Buscar maior ProdutoID já carregado
        # -----------------------------------------
        sql_cur.execute("SELECT ISNULL(MAX(ProdutoID), 0) FROM dw.DimProduto;")
        last_id = sql_cur.fetchone()[0]

        print(f"Último ProdutoID carregado: {last_id}")

        # -----------------------------------------
        # 2. Buscar novos produtos no Postgres
        # -----------------------------------------
        pg_query = """
            SELECT
                id AS ProdutoID,
                id_categoria AS CategoriaID,
                id_fornecedor AS FornecedorID,
                nome,
                valor_venda,
                valor_custo
            FROM vendas.produto
            WHERE id > %s
            ORDER BY id;
        """

        pg_cur.execute(pg_query, (last_id,))
        rows = pg_cur.fetchall()

        print(f"Encontrados {len(rows)} novos produtos.")

        # -----------------------------------------
        # 3. Inserção no SQL Server
        # -----------------------------------------
        inserted = 0

        for produto_id, categoria_id, fornecedor_id, nome, valor_venda, valor_custo in rows:

            # Obter CategoriaKey
            sql_cur.execute(
                "SELECT CategoriaKey FROM dw.DimCategoria WHERE CategoriaID = ?",
                (categoria_id,)
            )
            categoria_row = sql_cur.fetchone()
            categoria_key = categoria_row[0] if categoria_row else None

            # Obter FornecedorKey
            sql_cur.execute(
                "SELECT FornecedorKey FROM dw.DimFornecedor WHERE FornecedorID = ?",
                (fornecedor_id,)
            )
            fornecedor_row = sql_cur.fetchone()
            fornecedor_key = fornecedor_row[0] if fornecedor_row else None

            # Inserir produto
            sql_cur.execute("""
                INSERT INTO dw.DimProduto (
                    ProdutoID,
                    CategoriaKey,
                    FornecedorKey,
                    Nome,
                    ValorVenda,
                    ValorCusto
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                produto_id,
                categoria_key,
                fornecedor_key,
                nome,
                valor_venda,
                valor_custo
            ))

            inserted += 1

        # -----------------------------------------
        # 4. Commit e fechar conexões
        # -----------------------------------------
        sql_conn.commit()
        pg_conn.close()
        sql_conn.close()

        print(f"DimProduto - {inserted} registros inseridos com sucesso.")

    load_dim_produto()
