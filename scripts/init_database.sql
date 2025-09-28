/*
============================================================================================
Creatye database and schemas
============================================================================================
Script Purpose:
    This script creates a new database named "DATAWAREHOUSE" aafter checking is it already exists.
    If the database already exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database :"bronze","silver","gold"

Warning:
    Running this script will drop the entire "DATAWAREHOUSE" database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this scripts.
*/


USE master;

--drop and recreate the "DATAWAREHOUSE" database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DATAWAREHOUSE')
BEGIN 
	ALTER DATABASE DATAWAREHOUSE SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DATAWAREHOUSE;
END;
GO

--create database "DATAWAREHOUSE"
  
CREATE DATABASE DATAWAREHOUSE;

USE DATAWAREHOUSE;


	CREATE SCHEMA bronze;
	GO  --it tells system to first execute the one command fully then do the second one
	CREATE SCHEMA silver;
	GO
	CREATE SCHEMA gold;
	GO


