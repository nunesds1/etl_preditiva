------------------------------------------------------------
-- CRIA O BANCO DE DADOS
------------------------------------------------------------
IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'projeto_etl_preditivo')
BEGIN
    CREATE DATABASE projeto_etl_preditivo;
END
GO

USE projeto_etl_preditivo;
GO

------------------------------------------------------------
-- CRIA O SCHEMA
------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dw')
BEGIN
    EXEC('CREATE SCHEMA dw');
END
GO


------------------------------------------------------------
-- DIMENSÕES
------------------------------------------------------------

-- DimCategoria
CREATE TABLE dw.DimCategoria (
    CategoriaKey int IDENTITY(1,1) NOT NULL,
    CategoriaID int NOT NULL,
    Descricao nvarchar(100) NOT NULL,
    CONSTRAINT PK_DimCategoria PRIMARY KEY (CategoriaKey),
    CONSTRAINT UQ_DimCategoria_CategoriaID UNIQUE (CategoriaID)
);
GO

-- DimCliente
CREATE TABLE dw.DimCliente (
    ClienteKey int IDENTITY(1,1) NOT NULL,
    ClienteID int NOT NULL,
    Nome nvarchar(200) NOT NULL,
    Tipo_Pessoa nvarchar(50) NULL,
    CONSTRAINT PK_DimCliente PRIMARY KEY (ClienteKey),
    CONSTRAINT UQ_DimCliente_ClienteID UNIQUE (ClienteID)
);
GO

-- DimFormaPagamento
CREATE TABLE dw.DimFormaPagamento (
    FormaPagamentoKey int IDENTITY(1,1) NOT NULL,
    FormaPagamentoID int NOT NULL,
    Descricao nvarchar(100) NOT NULL,
    CONSTRAINT PK_DimFormaPagamento PRIMARY KEY (FormaPagamentoKey),
    CONSTRAINT UQ_DimFormaPagamento_FormaPagamentoID UNIQUE (FormaPagamentoID)
);
GO

-- DimFornecedor
CREATE TABLE dw.DimFornecedor (
    FornecedorKey int IDENTITY(1,1) NOT NULL,
    FornecedorID int NOT NULL,
    Nome nvarchar(200) NOT NULL,
    CONSTRAINT PK_DimFornecedor PRIMARY KEY (FornecedorKey),
    CONSTRAINT UQ_DimFornecedor_FornecedorID UNIQUE (FornecedorID)
);
GO

-- DimNotaFiscal
CREATE TABLE dw.DimNotaFiscal (
    NotaFiscalKey int IDENTITY(1,1) NOT NULL,
    NotaFiscalID int NOT NULL,
    NumeroNF nvarchar(50) NOT NULL,
    ValorTotal decimal(18,2),
    DateKey int NULL,
    CONSTRAINT PK_DimNotaFiscal PRIMARY KEY (NotaFiscalKey),
    CONSTRAINT UQ_DimNotaFiscal_NotaFiscalID UNIQUE (NotaFiscalID)
);
GO

-- DimParcela
CREATE TABLE dw.DimParcela (
    ParcelaKey int IDENTITY(1,1) NOT NULL,
    ParcelaID int NOT NULL,
    NotaFiscalID int NOT NULL,
    NumeroParcela int NOT NULL,
    ValorParcela decimal(18,2) NOT NULL,
    Vencimento date NOT NULL,
    DateKey int NULL,
    CONSTRAINT PK_DimParcela PRIMARY KEY (ParcelaKey),
    CONSTRAINT UQ_DimParcela_ParcelaID UNIQUE (ParcelaID)
);
GO

-- DimSituacaoTitulo
CREATE TABLE dw.DimSituacaoTitulo (
    SituacaoTituloKey int IDENTITY(1,1) NOT NULL,
    SituacaoTituloID int NOT NULL,
    Descricao nvarchar(100) NOT NULL,
    CONSTRAINT PK_DimSituacaoTitulo PRIMARY KEY (SituacaoTituloKey),
    CONSTRAINT UQ_DimSituacaoTitulo_SituacaoTituloID UNIQUE (SituacaoTituloID)
);
GO

-- DimTempo
CREATE TABLE dw.DimTempo (
    DateKey int NOT NULL,
    [Data] date NOT NULL,
    Dia tinyint NOT NULL,
    Mes tinyint NOT NULL,
    Ano smallint NOT NULL,
    NomeMes nvarchar(20) NOT NULL,
    NomeDiaSemana nvarchar(20) NOT NULL,
    Trimestre tinyint NOT NULL,
    SeFimSemana bit NOT NULL,
    CONSTRAINT PK_DimTempo PRIMARY KEY (DateKey)
);
GO

-- DimVendedor
CREATE TABLE dw.DimVendedor (
    VendedorKey int IDENTITY(1,1) NOT NULL,
    VendedorID int NOT NULL,
    Nome nvarchar(200) NOT NULL,
    CONSTRAINT PK_DimVendedor PRIMARY KEY (VendedorKey),
    CONSTRAINT UQ_DimVendedor_VendedorID UNIQUE (VendedorID)
);
GO

-- DimProduto
CREATE TABLE dw.DimProduto (
    ProdutoKey int IDENTITY(1,1) NOT NULL,
    ProdutoID int NOT NULL,
    Nome nvarchar(200) NOT NULL,
    CategoriaKey int NULL,
    FornecedorKey int NULL,
    ValorCusto decimal(18,2),
    ValorVenda decimal(18,2),
    CONSTRAINT PK_DimProduto PRIMARY KEY (ProdutoKey),
    CONSTRAINT UQ_DimProduto_ProdutoID UNIQUE (ProdutoID),
    CONSTRAINT FK_DimProduto_DimCategoria FOREIGN KEY (CategoriaKey) REFERENCES dw.DimCategoria(CategoriaKey),
    CONSTRAINT FK_DimProduto_DimFornecedor FOREIGN KEY (FornecedorKey) REFERENCES dw.DimFornecedor(FornecedorKey)
);
GO


------------------------------------------------------------
-- TABELAS FATO
------------------------------------------------------------

-- FatoPagamentos
CREATE TABLE dw.FatoPagamentos (
    FatoPagamentosKey bigint IDENTITY(1,1) NOT NULL,
    PagamentoID int NOT NULL,
    Documento nvarchar(100),
    FormaPagamentoKey int NULL,
    SituacaoTituloKey int NULL,
    DataEmissaoKey int NOT NULL,
    DataVencimentoKey int NOT NULL,
    DataPagamentoKey int NULL,
    ValorOriginal decimal(18,2) NOT NULL,
    ValorAtual decimal(18,2) NOT NULL,
    ValorPago decimal(18,2),
    DiasEmAtraso int NULL,
    CONSTRAINT PK_FatoPagamentos PRIMARY KEY (FatoPagamentosKey),
    CONSTRAINT FK_FP_DataEmissao FOREIGN KEY (DataEmissaoKey) REFERENCES dw.DimTempo(DateKey),
    CONSTRAINT FK_FP_DataPagamento FOREIGN KEY (DataPagamentoKey) REFERENCES dw.DimTempo(DateKey),
    CONSTRAINT FK_FP_DataVenc FOREIGN KEY (DataVencimentoKey) REFERENCES dw.DimTempo(DateKey),
    CONSTRAINT FK_FP_Forma FOREIGN KEY (FormaPagamentoKey) REFERENCES dw.DimFormaPagamento(FormaPagamentoKey),
    CONSTRAINT FK_FP_Situacao FOREIGN KEY (SituacaoTituloKey) REFERENCES dw.DimSituacaoTitulo(SituacaoTituloKey)
);
GO

CREATE INDEX IX_FatoPagamentos_PagamentoID ON dw.FatoPagamentos (PagamentoID);
GO


-- FatoRecebimentos
CREATE TABLE dw.FatoRecebimentos (
    RecebimentoKey bigint IDENTITY(1,1) NOT NULL,
    RecebimentoID int NOT NULL,
    ParcelaKey int NOT NULL,
    ClienteKey int NOT NULL,
    FormaPagamentoKey int NULL,
    SituacaoTituloKey int NULL,
    NotaFiscalKey int NULL,
    DataVencimentoKey int NOT NULL,
    DataRecebimentoKey int NULL,
    ValorOriginal decimal(18,2) NOT NULL,
    ValorAtual decimal(18,2) NOT NULL,
    ValorRecebido decimal(18,2),
    DiasEmAtraso int NULL,
    CONSTRAINT PK_FatoRecebimentos PRIMARY KEY (RecebimentoKey),
    CONSTRAINT FK_FR_Cliente FOREIGN KEY (ClienteKey) REFERENCES dw.DimCliente(ClienteKey),
    CONSTRAINT FK_FR_Forma FOREIGN KEY (FormaPagamentoKey) REFERENCES dw.DimFormaPagamento(FormaPagamentoKey),
    CONSTRAINT FK_FR_NF FOREIGN KEY (NotaFiscalKey) REFERENCES dw.DimNotaFiscal(NotaFiscalKey),
    CONSTRAINT FK_FR_Parcela FOREIGN KEY (ParcelaKey) REFERENCES dw.DimParcela(ParcelaKey),
    CONSTRAINT FK_FR_Situacao FOREIGN KEY (SituacaoTituloKey) REFERENCES dw.DimSituacaoTitulo(SituacaoTituloKey),
    CONSTRAINT FK_FR_DtReceb FOREIGN KEY (DataRecebimentoKey) REFERENCES dw.DimTempo(DateKey),
    CONSTRAINT FK_FR_DtVenc FOREIGN KEY (DataVencimentoKey) REFERENCES dw.DimTempo(DateKey)
);
GO

CREATE INDEX IX_FatoRecebimentos_RecebimentoID ON dw.FatoRecebimentos (RecebimentoID);
GO


-- FatoVendas
CREATE TABLE dw.FatoVendas (
    VendasKey bigint IDENTITY(1,1) NOT NULL,
    DateKey int NOT NULL,
    ProdutoKey int NOT NULL,
    ClienteKey int NOT NULL,
    VendedorKey int NOT NULL,
    FormaPagamentoKey int NOT NULL,
    NotaFiscalKey int NOT NULL,
    NumeroNF nvarchar(50),
    Quantidade decimal(18,4) NOT NULL,
    ValorUnitario decimal(18,4) NOT NULL,
    ValorVendaReal decimal(18,2) NOT NULL,
    ValorCustoTotal decimal(18,2),
    MargemBruta decimal(18,2),
    CONSTRAINT PK_FatoVendas PRIMARY KEY (VendasKey),
    CONSTRAINT FK_FV_Cliente FOREIGN KEY (ClienteKey) REFERENCES dw.DimCliente(ClienteKey),
    CONSTRAINT FK_FV_Forma FOREIGN KEY (FormaPagamentoKey) REFERENCES dw.DimFormaPagamento(FormaPagamentoKey),
    CONSTRAINT FK_FV_NF FOREIGN KEY (NotaFiscalKey) REFERENCES dw.DimNotaFiscal(NotaFiscalKey),
    CONSTRAINT FK_FV_Produto FOREIGN KEY (ProdutoKey) REFERENCES dw.DimProduto(ProdutoKey),
    CONSTRAINT FK_FV_Tempo FOREIGN KEY (DateKey) REFERENCES dw.DimTempo(DateKey),
    CONSTRAINT FK_FV_Vendedor FOREIGN KEY (VendedorKey) REFERENCES dw.DimVendedor(VendedorKey)
);
GO


------------------------------------------------------------
-- VIEWS
------------------------------------------------------------

-- vw_Atraso30Dias
CREATE OR ALTER VIEW dw.vw_Atraso30Dias AS
SELECT
    fr.RecebimentoID,
    fr.ParcelaKey,
    fr.ClienteKey,
    fr.FormaPagamentoKey,
    fr.SituacaoTituloKey,
    fr.NotaFiscalKey,
    CONVERT(date, CONVERT(varchar(8), fr.DataVencimentoKey)) AS DataVencimento,
    CONVERT(date, CONVERT(varchar(8), fr.DataRecebimentoKey)) AS DataRecebimento,
    fr.ValorOriginal,
    fr.ValorAtual,
    fr.ValorRecebido,
    fr.DiasEmAtraso,
    CASE WHEN fr.DiasEmAtraso > 30 THEN 1 ELSE 0 END AS Atraso30Dias
FROM dw.FatoRecebimentos fr;
GO


-- vw_Recompra90Dias
CREATE OR ALTER VIEW dw.vw_Recompra90Dias AS
WITH Vendas AS (
    SELECT
        f.ClienteKey,
        f.DateKey,
        t.Data AS DataVenda,
        f.ValorVendaReal AS Valor
    FROM dw.FatoVendas f
    JOIN dw.DimTempo t ON t.DateKey = f.DateKey
)
SELECT
    v.ClienteKey,
    v.DateKey,
    v.DataVenda,
    v.Valor,
    CASE WHEN EXISTS (
        SELECT 1
        FROM Vendas v2
        WHERE v2.ClienteKey = v.ClienteKey
          AND v2.DataVenda > v.DataVenda
          AND v2.DataVenda <= DATEADD(DAY, 90, v.DataVenda)
    )
    THEN 1 ELSE 0 END AS Recompra90Dias
FROM Vendas v;
GO


-- vw_ValorProximos30Dias
CREATE OR ALTER VIEW dw.vw_ValorProximos30Dias AS
WITH Vendas AS (
    SELECT
        f.ClienteKey,
        t.Data AS DataVenda,
        f.ValorVendaReal AS Valor
    FROM dw.FatoVendas f
    JOIN dw.DimTempo t ON t.DateKey = f.DateKey
),
AggPorDia AS (
    SELECT
        ClienteKey,
        CAST(DataVenda AS date) AS Dia,
        SUM(Valor) AS ValorDia
    FROM Vendas
    GROUP BY ClienteKey, CAST(DataVenda AS date)
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
GO
