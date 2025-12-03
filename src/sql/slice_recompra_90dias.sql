WITH Vendas AS (
    SELECT
        f.ClienteKey,
        f.DateKey,
        t.Data AS DataVenda,
        f.ValorVendaReal AS Valor
    FROM dw.FatoVendas f
    JOIN dw.DimTempo t
        ON t.DateKey = f.DateKey
)
SELECT
    v.ClienteKey,
    v.DateKey,
    v.DataVenda,
    v.Valor,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM Vendas v2
            WHERE v2.ClienteKey = v.ClienteKey
              AND v2.DataVenda > v.DataVenda
              AND v2.DataVenda <= DATEADD(DAY, 90, v.DataVenda)
        )
        THEN 1 ELSE 0
    END AS Recompra90Dias
FROM Vendas v
ORDER BY v.ClienteKey, v.DataVenda;
