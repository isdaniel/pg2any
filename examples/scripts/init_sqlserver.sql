IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'cdc_db')
    CREATE DATABASE cdc_db;
GO

USE cdc_db;
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 't1' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE dbo.t1 (
    ID INT NOT NULL PRIMARY KEY,
    val INT NOT NULL,
    col1 CHAR(36) NOT NULL,
    col2 CHAR(36) NOT NULL
);
GO
