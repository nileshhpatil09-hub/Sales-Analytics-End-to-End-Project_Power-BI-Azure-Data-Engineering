/*
=============================================================
Create Database and Schemas
=============================================================
*/

USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'SalesDataWarehouse')
BEGIN
    ALTER DATABASE SalesDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE SalesDataWarehouse;
END;
GO

-- Create the 'SalesDataWarehouse' database
CREATE DATABASE SalesDataWarehouse;
GO

USE SalesDataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
