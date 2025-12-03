PRINT '--------------------------------------------------------------';
PRINT 'Iniciando restauração do banco projeto_etl_preditivo...';
PRINT '--------------------------------------------------------------';
GO

-- Verifica se o banco já existe
IF DB_ID('projeto_etl_preditivo') IS NULL
BEGIN
    PRINT '>>> Banco não existe. Iniciando restauração...';

    RESTORE DATABASE projeto_etl_preditivo
    FROM DISK = '/var/opt/mssql/backup/projeto_etl_preditivo.bak'
    WITH REPLACE,
         MOVE 'projeto_etl_preditivo'
              TO '/var/opt/mssql/data/projeto_etl_preditivo.mdf',
         MOVE 'projeto_etl_preditivo_log'
              TO '/var/opt/mssql/data/projeto_etl_preditivo_log.ldf';

    PRINT '>>> Restauração concluída com sucesso.';
END
ELSE
BEGIN
    PRINT '>>> Banco projeto_etl_preditivo já existe. Nenhuma restauração realizada.';
END
GO
