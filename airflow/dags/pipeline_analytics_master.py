from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "etl",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="pipeline_analytics_master",
    description="Orquestra ETL completo + geração de slices + upload no HDFS",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Executa diariamente às 02:00
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
):

    # ============================
    #  DIMENSÕES
    # ============================

    dim_categoria = TriggerDagRunOperator(
        task_id="dim_categoria",
        trigger_dag_id="carga_dim_categoria",
        wait_for_completion=True
    )

    dim_cliente = TriggerDagRunOperator(
        task_id="dim_cliente",
        trigger_dag_id="carga_dim_cliente",
        wait_for_completion=True
    )

    dim_produto = TriggerDagRunOperator(
        task_id="dim_produto",
        trigger_dag_id="carga_dim_produto",
        wait_for_completion=True
    )

    dim_fornecedor = TriggerDagRunOperator(
        task_id="dim_fornecedor",
        trigger_dag_id="carga_dim_fornecedor",
        wait_for_completion=True
    )

    dim_forma_pag = TriggerDagRunOperator(
        task_id="dim_forma_pag",
        trigger_dag_id="carga_dim_formapagamento",
        wait_for_completion=True
    )

    dim_vendedor = TriggerDagRunOperator(
        task_id="dim_vendedor",
        trigger_dag_id="carga_dim_vendedor",
        wait_for_completion=True
    )

    dim_situacao = TriggerDagRunOperator(
        task_id="dim_situacao_titulo",
        trigger_dag_id="carga_dim_situacao_titulo",
        wait_for_completion=True
    )

    dim_nf = TriggerDagRunOperator(
        task_id="dim_nota_fiscal",
        trigger_dag_id="carga_dim_notafiscal",
        wait_for_completion=True
    )

    dim_parcela = TriggerDagRunOperator(
        task_id="dim_parcela",
        trigger_dag_id="carga_dim_parcela",
        wait_for_completion=True
    )

    # Listas de tarefas
    dim_tasks = [
        dim_categoria,
        dim_cliente,
        dim_produto,
        dim_fornecedor,
        dim_forma_pag,
        dim_vendedor,
        dim_situacao,
        dim_nf,
        dim_parcela
    ]

    # ============================
    #  FATOS
    # ============================

    fato_vendas = TriggerDagRunOperator(
        task_id="fato_vendas",
        trigger_dag_id="carga_fato_vendas",
        wait_for_completion=True
    )

    fato_receb = TriggerDagRunOperator(
        task_id="fato_recebimentos",
        trigger_dag_id="carga_fato_recebimentos",
        wait_for_completion=True
    )

    fato_pag = TriggerDagRunOperator(
        task_id="fato_pagamentos",
        trigger_dag_id="carga_fato_pagamentos",
        wait_for_completion=True
    )

    fato_tasks = [fato_vendas, fato_receb, fato_pag]

    # ============================
    #  SLICES + HDFS
    # ============================

    gerar_slices_csv = TriggerDagRunOperator(
        task_id="gerar_slices_csv",
        trigger_dag_id="extract_slices_csv",
        wait_for_completion=True
    )

    upload_parquet_hdfs = TriggerDagRunOperator(
        task_id="upload_parquet_hdfs",
        trigger_dag_id="upload_slices_hdfs",
        wait_for_completion=True
    )

    # ============================
    #  DEPENDÊNCIAS
    # ============================

    # Todas as dimensões → todas as fatos
    for d in dim_tasks:
        for f in fato_tasks:
            d >> f

    # Todas as fatos → gerar slices
    for f in fato_tasks:
        f >> gerar_slices_csv

    # Gerar slices → salvar parquet no HDFS
    gerar_slices_csv >> upload_parquet_hdfs
