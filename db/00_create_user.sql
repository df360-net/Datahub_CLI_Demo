-- =====================================================================
-- 00_create_user.sql
-- DCF_DB setup for datahub_proxy_app1 (Murphy MSSQL) — workplace-style grants
--
-- Run as:  sa  (or any sysadmin login)
-- Target:  Murphy at 192.168.0.16,1433
-- Prereq:  DCF_DB already exists; SQL Server in Mixed Mode auth;
--          TCP/IP enabled; port 1433 reachable from the dev workstation.
--
-- What this creates:
--   * Login + database user 'DCFDBUSR' (server + database principal)
--   * 4 schemas owned by dbo:  CARD_STG, CARD_INT, CARD_RPT, CARD_REF
--   * Database-wide DML for DCFDBUSR via db_datareader + db_datawriter
--   * CONNECT + EXECUTE + REFERENCES grants
--
-- DCFDBUSR is a DML user only. DDL (CREATE/ALTER/DROP TABLE) is done by sa,
-- not by the app user. See db/01_init.sql for table DDL.
--
-- IMPORTANT: replace <replace-with-strong-password> below before running.
-- The same password goes into the dev workstation's .env as MSSQL_PASSWORD.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. SERVER-LEVEL: create the login
-- ---------------------------------------------------------------------
USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'DCFDBUSR')
BEGIN
    CREATE LOGIN DCFDBUSR
        WITH PASSWORD       = N'<replace-with-strong-password>',
             CHECK_POLICY   = ON,
             CHECK_EXPIRATION = OFF,
             DEFAULT_DATABASE = DCF_DB;
END
GO


-- ---------------------------------------------------------------------
-- 2. DATABASE-LEVEL: map login to a user inside DCF_DB
-- ---------------------------------------------------------------------
USE DCF_DB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'DCFDBUSR')
BEGIN
    CREATE USER DCFDBUSR FOR LOGIN DCFDBUSR;
END
GO

GRANT CONNECT TO DCFDBUSR;
GO


-- ---------------------------------------------------------------------
-- 3. SCHEMAS — owned by dbo (DBA-controlled), not by DCFDBUSR
--
--    'CARD_INT' is NOT a reserved T-SQL keyword (only bare 'INT' is),
--    so no brackets are needed around the schema name.
--    CREATE SCHEMA must be the first statement in a batch, so we wrap
--    it in EXEC() to keep the IF guard.
-- ---------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'CARD_STG')
    EXEC('CREATE SCHEMA CARD_STG AUTHORIZATION dbo');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'CARD_INT')
    EXEC('CREATE SCHEMA CARD_INT AUTHORIZATION dbo');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'CARD_RPT')
    EXEC('CREATE SCHEMA CARD_RPT AUTHORIZATION dbo');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'CARD_REF')
    EXEC('CREATE SCHEMA CARD_REF AUTHORIZATION dbo');
GO


-- ---------------------------------------------------------------------
-- 4. DATABASE-WIDE DML — via built-in fixed roles
--
--    db_datareader  →  SELECT on every table/view, current AND future
--    db_datawriter  →  INSERT/UPDATE/DELETE on every table/view, current AND future
--
--    No need to re-grant when the DBA adds new tables or schemas later.
-- ---------------------------------------------------------------------
ALTER ROLE db_datareader ADD MEMBER DCFDBUSR;
ALTER ROLE db_datawriter ADD MEMBER DCFDBUSR;
GO


-- ---------------------------------------------------------------------
-- 5. Database-wide EXECUTE + REFERENCES
--
--    EXECUTE     → run any stored procedure / scalar function in DCF_DB
--    REFERENCES  → create FKs that reference DBA-owned base tables
-- ---------------------------------------------------------------------
GRANT EXECUTE    TO DCFDBUSR;
GRANT REFERENCES TO DCFDBUSR;
GO


-- =====================================================================
-- Verify
-- =====================================================================

-- (a) Role memberships — expect 2 rows: db_datareader, db_datawriter
SELECT
    USER_NAME(rm.member_principal_id)  AS user_name,
    USER_NAME(rm.role_principal_id)    AS role_name
FROM   sys.database_role_members rm
WHERE  USER_NAME(rm.member_principal_id) = 'DCFDBUSR'
ORDER  BY role_name;

-- (b) Database-level grants — expect 3 rows: CONNECT, EXECUTE, REFERENCES
SELECT
    dp.name             AS principal_name,
    p.permission_name,
    p.state_desc        AS grant_state,
    p.class_desc        AS scope
FROM   sys.database_permissions p
JOIN   sys.database_principals dp ON dp.principal_id = p.grantee_principal_id
WHERE  dp.name = 'DCFDBUSR'
  AND  p.class_desc = 'DATABASE'
ORDER  BY p.permission_name;

-- (c) Schemas — expect 4 rows
SELECT name AS schema_name
FROM   sys.schemas
WHERE  name IN ('CARD_STG','CARD_INT','CARD_RPT','CARD_REF')
ORDER  BY name;
GO
