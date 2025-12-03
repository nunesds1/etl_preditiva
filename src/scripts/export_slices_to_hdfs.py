import os
from io import BytesIO
import pandas as pd
from hdfs import InsecureClient
from datetime import datetime

# ⚠️ IMPORTANTE: SOMENTE SQL SERVER (Postgres removido)
from database import get_sqlserver_cursor


# -------------------------------------------------------------
# Configurações
# -------------------------------------------------------------
BASE_SQL_DIR = os.getenv("SQL_DIR", "./src/sql")

# Últimos N anos (default = 3)
LAST_N_YEARS = int(os.getenv("SLICE_LAST_N_YEARS", 3))
CURRENT_YEAR = datetime.now().year
MIN_YEAR = CURRENT_YEAR - LAST_N_YEARS + 1

print(f"[INFO] Filtrando dados a partir de {MIN_YEAR} (últimos {LAST_N_YEARS} anos)")


# -------------------------------------------------------------
# HDFS
# -------------------------------------------------------------
def get_hdfs_client():
    nn_host = os.getenv("HDFS_NAMENODE", "localhost")
    http_port = os.getenv("HDFS_HTTP_PORT", "9870")
    user = os.getenv("HDFS_USER", "root")

    url = f"http://{nn_host}:{http_port}"
    return InsecureClient(url, user=user)


# -------------------------------------------------------------
# Carrega SQL
# -------------------------------------------------------------
def load_sql(filename: str) -> str:
    path = os.path.join(BASE_SQL_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# -------------------------------------------------------------
# Executa a query corretamente (SEM CHAMAR fetchall 2x!)
# -------------------------------------------------------------
def run_query(cursor, filename):
    query = load_sql(filename)

    print("\n======== QUERY LIDA DO DISCO ========")
    print(repr(query))
    print("=====================================\n")

    cursor.execute(query)

    # colunas *antes* de fetchall
    colnames = [desc[0] for desc in cursor.description]

    # lê linhas uma única vez
    rows = cursor.fetchall()

    print(f"Retornou linhas: {len(rows):,}")
    if rows:
        print("Exemplo:", rows[0])

    return pd.DataFrame(rows, columns=colnames)


# -------------------------------------------------------------
# Salva particionado no HDFS
# -------------------------------------------------------------
def write_partitioned(df: pd.DataFrame, base_path: str, date_col: str, hdfs_client: InsecureClient):
    if df.empty:
        print(f"[SKIP] Nenhum dado para {base_path}")
        return

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])

    df["ano"] = df[date_col].dt.year
    df["mes"] = df[date_col].dt.month

    for (ano, mes), part_df in df.groupby(["ano", "mes"]):
        dir_path = f"{base_path}/ano={ano}/mes={mes:02d}"
        file_path = f"{dir_path}/dados.parquet"

        hdfs_client.makedirs(dir_path)

        buf = BytesIO()
        part_df.to_parquet(buf, index=False)
        buf.seek(0)

        with hdfs_client.write(file_path, overwrite=True) as f:
            f.write(buf.read())

        print(f"[OK] {file_path} (linhas={len(part_df)})")


# -------------------------------------------------------------
# EXPORTADORES
# -------------------------------------------------------------
def export_recompra(sql_cur, hdfs_client):
    print("\n>> Exportando Recompra 90 dias...")
    df = run_query(sql_cur, "slice_recompra_90dias.sql")

    if df.empty:
        print("[Recompra] Nenhum registro retornado pela view.")
        return

    print(f"[INFO] Linhas totais retornadas: {len(df):,}")

    # garantir datas válidas
    df["DataVenda"] = pd.to_datetime(df["DataVenda"], errors="coerce")
    df = df.dropna(subset=["DataVenda"])

    # filtro por ano
    df_filtered = df[df["DataVenda"].dt.year >= MIN_YEAR]

    print(f"[INFO] Linhas >= {MIN_YEAR}: {len(df_filtered):,}")

    if df_filtered.empty:
        print(f"[Recompra] Nenhum registro encontrado a partir de {MIN_YEAR}.")
        print("[DICA] Verifique se o DW possui dados recentes.")
        return

    # salvar
    write_partitioned(
        df_filtered,
        base_path="/datalake/analytics/recompra",
        date_col="DataVenda",
        hdfs_client=hdfs_client,
    )


def export_atrasos(sql_cur, hdfs_client):
    print("\n>> Exportando Atrasos > 30 dias...")
    df = run_query(sql_cur, "slice_atraso_30dias.sql")

    if df.empty:
        print("[Atrasos] Nenhum registro.")
        return

    df["DataVencimentoKey"] = df["DataVencimentoKey"].astype(int)
    df["ano"] = (df["DataVencimentoKey"] // 10000).astype(int)

    df_filtered = df[df["ano"] >= MIN_YEAR]

    print(f"[INFO] Linhas >= {MIN_YEAR}: {len(df_filtered):,}")

    if df_filtered.empty:
        print(f"[Atrasos] Nenhum registro encontrado a partir de {MIN_YEAR}.")
        return

    df_filtered["mes"] = (df_filtered["DataVencimentoKey"] // 100 % 100).astype(int)

    base = "/datalake/analytics/atrasos"

    for (ano, mes), part_df in df_filtered.groupby(["ano", "mes"]):
        dir_path = f"{base}/ano={ano}/mes={mes:02d}"
        file_path = f"{dir_path}/dados.parquet"

        hdfs_client.makedirs(dir_path)

        buf = BytesIO()
        part_df.to_parquet(buf, index=False)
        buf.seek(0)

        with hdfs_client.write(file_path, overwrite=True) as f:
            f.write(buf.read())

        print(f"[Atrasos OK] {file_path} (linhas={len(part_df)})")


def export_valor_30dias(sql_cur, hdfs_client):
    print("\n>> Exportando Valor próximos 30 dias...")
    df = run_query(sql_cur, "slice_valor_30dias.sql")

    if df.empty:
        print("[Valor30Dias] Nenhum registro.")
        return

    df["Dia"] = pd.to_datetime(df["Dia"], errors="coerce")
    df = df.dropna(subset=["Dia"])
    df_filtered = df[df["Dia"].dt.year >= MIN_YEAR]

    print(f"[INFO] Linhas >= {MIN_YEAR}: {len(df_filtered):,}")

    if df_filtered.empty:
        print(f"[Valor30Dias] Nenhum registro encontrado a partir de {MIN_YEAR}.")
        return

    write_partitioned(
        df_filtered,
        base_path="/datalake/analytics/valor_30dias",
        date_col="Dia",
        hdfs_client=hdfs_client,
    )


# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
def main():
    print("\n🔌 Conectando ao SQL Server via database.py...")
    sql_conn, sql_cur = get_sqlserver_cursor()

    print("🔌 Conectando ao HDFS...")
    hdfs_client = get_hdfs_client()

    print("\n==========================================")
    print(f"Processando últimos {LAST_N_YEARS} anos (desde {MIN_YEAR})")
    print("==========================================")

    export_recompra(sql_cur, hdfs_client)
    export_atrasos(sql_cur, hdfs_client)
    export_valor_30dias(sql_cur, hdfs_client)

    sql_cur.close()
    sql_conn.close()

    print("\n✔ Finalizado com sucesso!\n")


if __name__ == "__main__":
    main()
