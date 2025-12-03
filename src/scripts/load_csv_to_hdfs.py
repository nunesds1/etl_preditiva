import os
import pandas as pd
from io import BytesIO
from hdfs import InsecureClient
import socket

INPUT_DIR = "./data_slices_csv"


# ------------------------------
# HDFS CLIENT
# ------------------------------
def get_hdfs_client():
    # host = os.getenv("HDFS_NAMENODE", "localhost")
    # port = os.getenv("HDFS_HTTP_PORT", "9870")
    # user = os.getenv("HDFS_USER", "root")
    # return InsecureClient(f"http://{host}:{port}", user=user)
    """
    Detecta automaticamente se o script está rodando dentro de um container
    e escolhe o host correto do Hadoop.
    """

    # Testa se host 'namenode' existe (ou seja, estamos em container Docker)
    try:
        socket.gethostbyname("namenode")
        print(">> Rodando em Docker — usando namenode:9870")
        return InsecureClient("http://namenode:9870", user="root")
    except Exception:
        print(">> Rodando no host — usando localhost:9870")
        return InsecureClient("http://localhost:9870", user="root")


# ------------------------------
# SALVAR PARTICIONADO
# ------------------------------
def write_partitioned(df, base_path, date_col, client):
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])

    df["ano"] = df[date_col].dt.year
    df["mes"] = df[date_col].dt.month

    for (ano, mes), part_df in df.groupby(["ano", "mes"]):
        dir_path = f"{base_path}/ano={ano}/mes={mes:02d}"
        file_path = f"{dir_path}/dados.parquet"

        client.makedirs(dir_path)

        buf = BytesIO()
        part_df.to_parquet(buf, index=False)
        buf.seek(0)

        with client.write(file_path, overwrite=True) as w:
            w.write(buf.read())

        print(f"[OK] {file_path} ({len(part_df)} linhas)")


# ------------------------------
# PROCESSADORES
# ------------------------------
def load_recompra(client):
    path = f"{INPUT_DIR}/recompra.csv"
    df = pd.read_csv(path)
    write_partitioned(df, "/datalake/analytics/recompra", "DataVenda", client)


def load_atrasos(client):
    path = f"{INPUT_DIR}/atrasos.csv"
    df = pd.read_csv(path)
    df["DataVencimentoKey"] = df["DataVencimentoKey"].astype(int)
    df["ano"] = (df["DataVencimentoKey"] // 10000).astype(int)
    df["mes"] = (df["DataVencimentoKey"] // 100 % 100).astype(int)
    write_partitioned(df, "/datalake/analytics/atrasos", "DataVencimentoKey", client)


def load_valor_30dias(client):
    path = f"{INPUT_DIR}/valor_30dias.csv"
    df = pd.read_csv(path)
    write_partitioned(df, "/datalake/analytics/valor_30dias", "Dia", client)


# ------------------------------
# MAIN
# ------------------------------
def main():
    print("🔌 Conectando ao HDFS...")
    client = get_hdfs_client()

    # load_recompra(client)
    load_atrasos(client)
    # load_valor_30dias(client)

    print("\n✔ Upload para o HDFS concluído!\n")


if __name__ == "__main__":
    main()
