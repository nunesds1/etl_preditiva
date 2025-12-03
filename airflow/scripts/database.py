import os
import time
import psycopg2
import pyodbc
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1. Carrega variáveis do .env
# ---------------------------------------------------------
load_dotenv()


# ---------------------------------------------------------
# 2. Configurações do Postgres a partir do .env
# ---------------------------------------------------------
PG_CONFIG = {
    "host": os.getenv("POSTGRES_DATADT_HOST"),
    "dbname": os.getenv("POSTGRES_DATADT_DB"),
    "user": os.getenv("POSTGRES_DATADT_USER"),
    "password": os.getenv("POSTGRES_DATADT_PASSWORD"),
    "port": os.getenv("POSTGRES_DATADT_PORT", 5432),
}


# ---------------------------------------------------------
# 3. Configurações do SQL Server a partir do .env
# ---------------------------------------------------------
SQLSERVER_CONFIG = {
    "driver": "ODBC Driver 18 for SQL Server",
    "server": os.getenv("MSSQL_HOST"),
    "database": os.getenv("MSSQL_DB"),
    "user": os.getenv("MSSQL_USER"),
    "password": os.getenv("MSSQL_SA_PASSWORD"),
    
}


# ---------------------------------------------------------
# 4. Função com Retry Automático
# ---------------------------------------------------------
def retry_connection(func):
    def wrapper(*args, **kwargs):
        retries = 5
        delay = 3
        for attempt in range(1, retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"[ERRO] Tentativa {attempt}/{retries} ao conectar:")
                print(e)
                if attempt < retries:
                    print(f"Novamente em {delay}s...\n")
                    time.sleep(delay)
                else:
                    print("Falha após várias tentativas.")
                    raise
    return wrapper


# ---------------------------------------------------------
# 5. Conectar ao Postgres
# ---------------------------------------------------------
@retry_connection
def get_postgres_conn():
    return psycopg2.connect(**PG_CONFIG)


# ---------------------------------------------------------
# 6. Conectar ao SQL Server
# ---------------------------------------------------------
@retry_connection
def get_sqlserver_conn():
    conn_str = (
        f"DRIVER={{{SQLSERVER_CONFIG['driver']}}};"
        f"SERVER=etl_sqlserver,1433;"
        f"DATABASE={SQLSERVER_CONFIG['database']};"
        f"UID={SQLSERVER_CONFIG['user']};"
        f"PWD={SQLSERVER_CONFIG['password']};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


# ---------------------------------------------------------
# 7. Criar cursores
# ---------------------------------------------------------
def get_postgres_cursor():
    conn = get_postgres_conn()
    return conn, conn.cursor()


def get_sqlserver_cursor():
    conn = get_sqlserver_conn()
    return conn, conn.cursor()


# ---------------------------------------------------------
# 8. Teste de Diagnóstico
# ---------------------------------------------------------
def test_connections():
    print("🔍 Testando conexão com Postgres...")
    try:
        pg_conn, pg_cur = get_postgres_cursor()
        pg_cur.execute("SELECT version();")
        print("Postgres OK ->", pg_cur.fetchone())
        pg_conn.close()
    except Exception as e:
        print("Erro Postgres:", e)

    print("\n🔍 Testando conexão com SQL Server...")
    try:
        sql_conn, sql_cur = get_sqlserver_cursor()
        sql_cur.execute("SELECT @@VERSION;")
        print("SQL Server OK ->", sql_cur.fetchone())
        sql_conn.close()
    except Exception as e:
        print("Erro SQL Server:", e)


# ---------------------------------------------------------
# Execução direta (debug)
# ---------------------------------------------------------
if __name__ == "__main__":
    test_connections()
