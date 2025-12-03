from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys

# Caminho absoluto para scripts/
SCRIPTS_DIR = "/opt/airflow/scripts"
sys.path.append(SCRIPTS_DIR)

# Importa script real
from load_csv_to_hdfs import main as run_upload_hdfs


default_args = {
    "owner": "etl",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="upload_slices_hdfs",
    default_args=default_args,
    description="Carrega Slices CSV para Parquet no HDFS",
    schedule_interval=None,  # chamada pela DAG master
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
):

    @task
    def upload_to_hdfs():
        print("=== Enviando slices para o HDFS em formato Parquet ===")
        run_upload_hdfs()
        print("=== Upload concluído com sucesso ===")

    upload_to_hdfs()
