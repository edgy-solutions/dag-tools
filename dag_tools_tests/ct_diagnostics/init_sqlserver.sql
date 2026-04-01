IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'SOURCE_DB')
BEGIN
    CREATE DATABASE SOURCE_DB;
END
GO

USE SOURCE_DB;
GO

-- 0. Create Other Schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'other')
BEGIN
    EXEC('CREATE SCHEMA other');
END
GO

-- 1. Enable Database Change Tracking
IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID('SOURCE_DB'))
BEGIN
    ALTER DATABASE SOURCE_DB SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
END
GO

-- 2. Create the Source Tables
-- Table 1: dbo.TestCT
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TestCT]') AND type in (N'U'))
BEGIN
    CREATE TABLE dbo.TestCT (
        id INT PRIMARY KEY,
        name VARCHAR(100),
        value INT,
        updated_at DATETIME DEFAULT GETDATE()
    );
    ALTER TABLE dbo.TestCT ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
    INSERT INTO dbo.TestCT (id, name, value) VALUES (1, 'Initial Row 1', 100), (2, 'Initial Row 2', 200);
END
GO

-- Table 2: dbo.TestCT_Extra
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TestCT_Extra]') AND type in (N'U'))
BEGIN
    CREATE TABLE dbo.TestCT_Extra (
        id INT PRIMARY KEY,
        name VARCHAR(100)
    );
    ALTER TABLE dbo.TestCT_Extra ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
    INSERT INTO dbo.TestCT_Extra (id, name) VALUES (1, 'Extra Row 1');
END
GO

-- Table 3: other.TestCT (Schema Collision Test)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[other].[TestCT]') AND type in (N'U'))
BEGIN
    CREATE TABLE other.TestCT (
        id INT PRIMARY KEY,
        description VARCHAR(100)
    );
    ALTER TABLE other.TestCT ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
    INSERT INTO other.TestCT (id, description) VALUES (1, 'Other Schema Row 1');
END
GO
