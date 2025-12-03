#!/usr/bin/env bash
set -e

CMD="$1"
shift || true

echo ">>> [init.sh] Inicializando Airflow..."

# Garantir logs com permissão
mkdir -p /opt/airflow/logs
chmod -R 777 /opt/airflow/logs || true

echo ">>> [init.sh] Verificando banco..."
airflow db check || true

echo ">>> [init.sh] Migrando banco..."
airflow db migrate

echo ">>> [init.sh] Garantindo usuário admin..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" || true

echo ">>> [init.sh] Subindo comando: $CMD $*"
exec $CMD "$@"
