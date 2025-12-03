WITH Vendas AS (
    SELECT
        f.ClienteKey,
        t.Data,
        f.ValorVendaReal AS Valor
    FROM dw.FatoVendas f
    JOIN dw.DimTempo t
        ON t.DateKey = f.DateKey
),
AggPorDia AS (
    SELECT
        ClienteKey,
        CAST(Data AS date) AS Dia,
        SUM(Valor) AS ValorDia
    FROM Vendas
    GROUP BY ClienteKey, CAST(Data AS date)
),
Futuro AS (
    SELECT
        a.ClienteKey,
        a.Dia,
        a.ValorDia,
        (
            SELECT SUM(a2.ValorDia)
            FROM AggPorDia a2
            WHERE a2.ClienteKey = a.ClienteKey
              AND a2.Dia > a.Dia
              AND a2.Dia <= DATEADD(DAY, 30, a.Dia)
        ) AS ValorProximos30Dias
    FROM AggPorDia a
)
SELECT
    ClienteKey,
    Dia,
    ValorDia,
    ValorProximos30Dias
FROM Futuro
WHERE ValorProximos30Dias IS NOT NULL;
