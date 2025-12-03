import os
import pandas as pd
from datetime import datetime
from database import get_sqlserver_cursor
import pyodbc

OUTPUT_DIR = "./data_slices_csv"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CURRENT_YEAR = datetime.now().year
LAST_N = int(os.getenv("SLICE_LAST_N_YEARS", 3))
MIN_YEAR = CURRENT_YEAR - LAST_N + 1


# ===============================================
#  SQL DIRETO (SEM ARQUIVO .SQL)
# ===============================================

SQL_RECOMPRA = """
SELECT 
    ClienteKey,
    DateKey,
    DataVenda,
    Valor,
    Recompra90Dias
FROM dw.vw_Recompra90Dias;
"""

SQL_ATRASOS = """
SELECT
    RecebimentoID,
    ParcelaKey,
    ClienteKey,
    FormaPagamentoKey,
    SituacaoTituloKey,
    NotaFiscalKey,
    DataVencimentoKey,
    DataRecebimentoKey,
    ValorOriginal,
    ValorAtual,
    ValorRecebido,
    DiasEmAtraso,
    Atraso30Dias
FROM dw.vw_Atraso30Dias;
"""

SQL_VALOR30 = """
SELECT
    ClienteKey,
    Dia,
    ValorDia,
    ValorProximos30Dias
FROM dw.vw_ValorProximos30Dias;
"""


# ===============================================
#  Função simples para query → DataFrame
# ===============================================
def query_to_df(cur, sql):
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=cols)


# ===============================================
#  Extract Recompra
# ===============================================
def extract_recompra(conn):
    print("\n>> Recompra 90 dias")

    df = pd.read_sql("SELECT * FROM dw.vw_Recompra90Dias", conn)
    # df = query_to_df(cur, SQL_RECOMPRA)
    print(f"[INFO] Linhas totais: {len(df):,}")

    df["DataVenda"] = pd.to_datetime(df["DataVenda"], errors="coerce")
    df = df[df["DataVenda"].dt.year >= MIN_YEAR]

    out = f"{OUTPUT_DIR}/recompra.csv"
    df.to_csv(out, index=False)
    print(f"[OK] Salvo {len(df):,} linhas em {out}")


# ===============================================
#  Extract Atrasos
# ===============================================
def extract_atrasos(conn):
    print("\n>> Atrasos > 30 dias")

    df = pd.read_sql("SELECT * FROM dw.vw_Atraso30Dias", conn)
    # df = query_to_df(cur, SQL_ATRASOS)
    print(f"[INFO] Linhas totais: {len(df):,}")

    df["DataVencimentoKey"] = df["DataVencimentoKey"].astype(int)
    df["ano"] = (df["DataVencimentoKey"] // 10000).astype(int)
    df = df[df["ano"] >= MIN_YEAR]

    out = f"{OUTPUT_DIR}/atrasos.csv"
    df.to_csv(out, index=False)
    print(f"[OK] Salvo {len(df):,} linhas em {out}")


# ===============================================
#  Extract Valor 30 dias
# ===============================================
def extract_valor_30dias(conn):
    print("\n>> Valor próximos 30 dias")

    df = pd.read_sql("SELECT * FROM dw.vw_ValorProximos30Dias", conn)
    # df = query_to_df(cur, SQL_VALOR30)
    print(f"[INFO] Linhas totais: {len(df):,}")

    df["Dia"] = pd.to_datetime(df["Dia"], errors="coerce")
    df = df[df["Dia"].dt.year >= MIN_YEAR]

    out = f"{OUTPUT_DIR}/valor_30dias.csv"
    df.to_csv(out, index=False)
    print(f"[OK] Salvo {len(df):,} linhas em {out}")


# ===============================================
# MAIN
# ===============================================
def main():
    conn, cur = get_sqlserver_cursor()

    extract_recompra(conn)
    extract_atrasos(conn)
    extract_valor_30dias(conn)

    cur.close()
    conn.close()

    print("\n✔ Extração concluída!\n")


if __name__ == "__main__":
    main()
