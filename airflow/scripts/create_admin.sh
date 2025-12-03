#!/usr/bin/env bash
set -e

echo ">>> Verificando se admin já existe..."
airflow users list | grep "${AIRFLOW_ADMIN_USERNAME}" && echo ">>> Usuário já existe. OK!" && exit 0

echo ">>> Criando usuário admin..."
airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL}"

echo ">>> Usuário admin criado com sucesso!"
