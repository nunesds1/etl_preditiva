from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys

# Caminho absoluto para a pasta scripts/
SCRIPTS_DIR = "/opt/airflow/scripts"
sys.path.append(SCRIPTS_DIR)

# Importa o script real
from extract_slices_to_csv import main as run_extract_csv


default_args = {
    "owner": "etl",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="extract_slices_csv",
    default_args=default_args,
    description="Extrai os slices analíticos do DW e salva CSV no host",
    schedule_interval=None,  # Será acionada pela DAG master
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
):

    @task
    def extract_slices():
        print("=== Executando extração dos slices para CSV ===")
        run_extract_csv()
        print("=== Slice CSV gerado com sucesso ===")

    extract_slices()
