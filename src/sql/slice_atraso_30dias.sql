SELECT
    fr.RecebimentoID,
    fr.ParcelaKey,
    fr.ClienteKey,
    fr.FormaPagamentoKey,
    fr.SituacaoTituloKey,
    fr.NotaFiscalKey,

    -- Convertendo DataVencimentoKey (YYYYMMDD int) para DATE
    TRY_CONVERT(date,
        STUFF(STUFF(CAST(fr.DataVencimentoKey AS varchar(8)), 5, 0, '-'), 8, 0, '-')
    ) AS DataVencimento,

    -- Convertendo DataRecebimentoKey (YYYYMMDD int) para DATE
    TRY_CONVERT(date,
        STUFF(STUFF(CAST(fr.DataRecebimentoKey AS varchar(8)), 5, 0, '-'), 8, 0, '-')
    ) AS DataRecebimento,

    fr.ValorOriginal,
    fr.ValorAtual,
    fr.ValorRecebido,
    fr.DiasEmAtraso,

    CASE WHEN fr.DiasEmAtraso > 30 THEN 1 ELSE 0 END AS Atraso30Dias
FROM dw.FatoRecebimentos fr;