-- =====================================================================
-- 01_init.sql
-- Table DDL for datahub_proxy_app1 / CardCompass (Murphy MSSQL).
--
-- Run as:  sa  (DDL is the DBA's job; DCFDBUSR does DML only)
-- Target:  DCF_DB on Murphy at 192.168.0.16,1433
-- Prereq:  db/00_create_user.sql has run (schemas CARD_STG/INT/RPT/REF exist).
--
-- Creates 9 tables: 2 stage + 2 integration + 2 report + 3 reference.
-- Idempotent: each CREATE is guarded by OBJECT_ID, so re-running is safe and
-- never drops existing data. To rebuild a table, drop it manually first.
--
-- Design choices (see docs/Sample_ETL_Application_design.md):
--   * Stage tables have NO primary key — raw landing must accept the
--     generator's intentional duplicate rows so DUPLICATE_FEED can fire.
--   * No physical FOREIGN KEY constraints in v1 — the daily
--     DELETE-by-business-date + re-INSERT flow stays simple; the ETL joins
--     enforce referential integrity logically.
-- =====================================================================

USE DCF_DB;
GO


-- ---------------------------------------------------------------------
-- STAGE — CARD_STG  (raw landing, all nvarchar, no PK)
-- ---------------------------------------------------------------------
IF OBJECT_ID('CARD_STG.card_auth', 'U') IS NULL
CREATE TABLE CARD_STG.card_auth (
    txn_id              nvarchar(40)   NOT NULL,
    card_number_hash    nvarchar(64)   NULL,
    auth_ts             nvarchar(32)   NULL,
    auth_amount         nvarchar(20)   NULL,
    txn_currency        nvarchar(3)    NULL,
    merchant_id         nvarchar(20)   NULL,
    mcc                 nvarchar(4)    NULL,
    txn_type            nvarchar(20)   NULL,
    response_code       nvarchar(4)    NULL,
    file_seq            nvarchar(20)   NULL,
    load_business_date  date           NOT NULL,
    loaded_at           datetime2(3)   NOT NULL CONSTRAINT DF_card_auth_loaded_at DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('CARD_STG.card_post', 'U') IS NULL
CREATE TABLE CARD_STG.card_post (
    txn_id              nvarchar(40)   NOT NULL,
    posting_ref         nvarchar(40)   NOT NULL,
    posting_ts          nvarchar(32)   NULL,
    settled_amount      nvarchar(20)   NULL,
    settlement_currency nvarchar(3)    NULL,
    acquirer_id         nvarchar(20)   NULL,
    file_seq            nvarchar(20)   NULL,
    load_business_date  date           NOT NULL,
    loaded_at           datetime2(3)   NOT NULL CONSTRAINT DF_card_post_loaded_at DEFAULT SYSUTCDATETIME()
);
GO

-- Non-unique indexes to keep the integration join and idempotent DELETE quick.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_card_auth_bizdate' AND object_id = OBJECT_ID('CARD_STG.card_auth'))
    CREATE INDEX IX_card_auth_bizdate ON CARD_STG.card_auth (load_business_date);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_card_post_bizdate' AND object_id = OBJECT_ID('CARD_STG.card_post'))
    CREATE INDEX IX_card_post_bizdate ON CARD_STG.card_post (load_business_date, txn_id);
GO


-- ---------------------------------------------------------------------
-- INTEGRATION — CARD_INT  (typed, joined, deduped)
-- ---------------------------------------------------------------------
IF OBJECT_ID('CARD_INT.card_txn', 'U') IS NULL
CREATE TABLE CARD_INT.card_txn (
    txn_id          nvarchar(40)   NOT NULL CONSTRAINT PK_card_txn PRIMARY KEY,
    card_id         bigint         NULL,
    auth_ts         datetime2(3)   NULL,
    posting_ts      datetime2(3)   NULL,
    txn_date        date           NOT NULL,
    auth_amount     decimal(18,2)  NULL,
    settled_amount  decimal(18,2)  NULL,
    txn_currency    char(3)        NULL,
    merchant_id     nvarchar(20)   NULL,
    merchant_name   nvarchar(100)  NULL,
    mcc             char(4)        NULL,
    mcc_category    nvarchar(50)   NULL,
    txn_type        nvarchar(20)   NULL,
    is_approved     bit            NOT NULL,
    is_posted       bit            NOT NULL,
    decline_reason  nvarchar(100)  NULL,
    loaded_at       datetime2(3)   NOT NULL CONSTRAINT DF_card_txn_loaded_at DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_card_txn_date' AND object_id = OBJECT_ID('CARD_INT.card_txn'))
    CREATE INDEX IX_card_txn_date ON CARD_INT.card_txn (txn_date);
GO

IF OBJECT_ID('CARD_INT.card_txn_decline', 'U') IS NULL
CREATE TABLE CARD_INT.card_txn_decline (
    txn_id              nvarchar(40)   NOT NULL CONSTRAINT PK_card_txn_decline PRIMARY KEY,
    card_id             bigint         NULL,
    txn_date            date           NOT NULL,
    auth_amount         decimal(18,2)  NULL,
    decline_reason      nvarchar(100)  NULL,
    flagged_for_review  bit            NOT NULL,
    loaded_at           datetime2(3)   NOT NULL CONSTRAINT DF_card_txn_decline_loaded_at DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_card_txn_decline_date' AND object_id = OBJECT_ID('CARD_INT.card_txn_decline'))
    CREATE INDEX IX_card_txn_decline_date ON CARD_INT.card_txn_decline (txn_date);
GO


-- ---------------------------------------------------------------------
-- REPORT — CARD_RPT  (aggregated, business-facing)
-- ---------------------------------------------------------------------
IF OBJECT_ID('CARD_RPT.daily_card_summary', 'U') IS NULL
CREATE TABLE CARD_RPT.daily_card_summary (
    txn_date              date           NOT NULL,
    mcc_category          nvarchar(50)   NOT NULL,
    txn_type              nvarchar(20)   NOT NULL,
    txn_count             int            NOT NULL,
    approved_count        int            NOT NULL,
    decline_count         int            NOT NULL,
    auth_amount_total     decimal(18,2)  NOT NULL,
    settled_amount_total  decimal(18,2)  NOT NULL,
    loaded_at             datetime2(3)   NOT NULL CONSTRAINT DF_daily_card_summary_loaded_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_daily_card_summary PRIMARY KEY (txn_date, mcc_category, txn_type)
);
GO

-- [rank] is bracketed: RANK is a reserved T-SQL keyword.
IF OBJECT_ID('CARD_RPT.card_top_merchants', 'U') IS NULL
CREATE TABLE CARD_RPT.card_top_merchants (
    txn_date              date           NOT NULL,
    [rank]                int            NOT NULL,
    merchant_id           nvarchar(20)   NOT NULL,
    merchant_name         nvarchar(100)  NULL,
    txn_count             int            NOT NULL,
    settled_amount_total  decimal(18,2)  NOT NULL,
    loaded_at             datetime2(3)   NOT NULL CONSTRAINT DF_card_top_merchants_loaded_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_card_top_merchants PRIMARY KEY (txn_date, [rank])
);
GO


-- ---------------------------------------------------------------------
-- REFERENCE — CARD_REF  (static lookups; DDL here, data in 02_seed_reference.sql)
-- ---------------------------------------------------------------------
IF OBJECT_ID('CARD_REF.card', 'U') IS NULL
CREATE TABLE CARD_REF.card (
    card_id           bigint         NOT NULL CONSTRAINT PK_card PRIMARY KEY,
    card_number_hash  nvarchar(64)   NOT NULL CONSTRAINT UQ_card_hash UNIQUE,
    customer_id       bigint         NOT NULL,
    issue_date        date           NOT NULL,
    is_active         bit            NOT NULL
);
GO

IF OBJECT_ID('CARD_REF.merchant', 'U') IS NULL
CREATE TABLE CARD_REF.merchant (
    merchant_id    nvarchar(20)   NOT NULL CONSTRAINT PK_merchant PRIMARY KEY,
    merchant_name  nvarchar(100)  NOT NULL,
    mcc            nvarchar(4)    NOT NULL,
    country        nvarchar(2)    NOT NULL
);
GO

IF OBJECT_ID('CARD_REF.mcc_category', 'U') IS NULL
CREATE TABLE CARD_REF.mcc_category (
    mcc           nvarchar(4)    NOT NULL CONSTRAINT PK_mcc_category PRIMARY KEY,
    mcc_category  nvarchar(50)   NOT NULL
);
GO


-- =====================================================================
-- Verify — expect 9 rows
-- =====================================================================
SELECT s.name AS [schema], t.name AS [table]
FROM   sys.tables t
JOIN   sys.schemas s ON s.schema_id = t.schema_id
WHERE  s.name IN ('CARD_STG','CARD_INT','CARD_RPT','CARD_REF')
ORDER  BY s.name, t.name;
GO
