#!/bin/bash

echo ">>> Iniciando entrypoint personalizado..."

# Inicia SQL Server em segundo plano
/opt/mssql/bin/sqlservr &

# ----------------------------------------------------------
# HEALTHCHECK REAL — aguarda o SQL aceitar conexões
# ----------------------------------------------------------
echo ">>> Aguardando SQL Server ficar online..."

MAX_TRIES=30
TRY=1

until /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" &> /dev/null
do
    if [ $TRY -gt $MAX_TRIES ]; then
        echo "❌ ERRO: SQL Server não ficou pronto após $MAX_TRIES tentativas."
        exit 1
    fi

    echo "   - Tentativa $TRY/$MAX_TRIES... aguardando 2s"
    TRY=$((TRY+1))
    sleep 2
done

echo "✔ SQL Server online!"


# ----------------------------------------------------------
# AJUSTA PERMISSÕES AUTOMATICAMENTE
# ----------------------------------------------------------
echo ">>> Ajustando permissões dos arquivos de backup .bak..."

# Corrige permissões
chmod 644 /var/opt/mssql/backup/*.bak 2>/dev/null || true
chown mssql:mssql /var/opt/mssql/backup/*.bak 2>/dev/null || true

echo "✔ Permissões ajustadas."


# ----------------------------------------------------------
# Executa scripts SQL (restore, criação de views, etc)
# ----------------------------------------------------------
echo ">>> Executando scripts SQL..."

for script in /mssql_init/*.sql; do
    echo "Executando: $script"
    /opt/mssql-tools/bin/sqlcmd \
        -S localhost \
        -U SA \
        -P "$MSSQL_SA_PASSWORD" \
        -i "$script"
done

echo ">>> Inicialização finalizada com sucesso!"
wait
