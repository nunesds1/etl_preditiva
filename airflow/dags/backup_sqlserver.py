from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'backup_sqlserver_semanal',
    default_args=default_args,
    schedule_interval='0 3 * * 0',  # Domingo às 03:00
    catchup=False
):

    backup = BashOperator(
        task_id='backup_sqlserver',
        bash_command="""
            docker run --rm \
            --network etl_preditiva_v3_etl_net \
            -v {{ var.value.project_path }}/mssql_backups:/var/opt/mssql/backup \
            mcr.microsoft.com/mssql-tools \
            /opt/mssql-tools/bin/sqlcmd -S etl_sqlserver -U SA -P "$MSSQL_SA_PASSWORD" \
            -Q "BACKUP DATABASE projeto_etl_preditivo TO DISK='/var/opt/mssql/backup/projeto_etl_preditivo_{{ ds }}.bak' WITH INIT;"
            """
    )
