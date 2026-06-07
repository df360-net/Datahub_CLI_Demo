-- =====================================================================
-- 02_seed_reference.sql
-- Reference data for CardCompass — CARD_REF.{mcc_category, merchant, card}.
--
-- Run as:  sa  (one-shot; reference data has a different cadence than the
--               daily transactional flow, so it is seeded outside the ETL)
-- Target:  DCF_DB on Murphy. Prereq: db/01_init.sql has run.
--
-- Deterministic + idempotent: clears then re-inserts a fixed set, so the
-- generated mock files (tools/generate_daily_file.py, which reads these
-- tables) are reproducible. Re-running yields identical rows.
--
-- Volumes (design defaults): 100 MCC codes, 100 merchants, 200 cards.
-- The 100 MCC codes collapse to ~12 distinct category labels, which keeps
-- CARD_RPT.daily_card_summary at the expected ~15-20 rows/day.
-- =====================================================================

USE DCF_DB;
SET NOCOUNT ON;
GO

BEGIN TRANSACTION;

-- No FKs between these tables, so delete order is free.
DELETE FROM CARD_REF.merchant;
DELETE FROM CARD_REF.mcc_category;
DELETE FROM CARD_REF.card;


-- ---------------------------------------------------------------------
-- mcc_category — 100 codes (5001..5100), 12 rotating category labels
-- ---------------------------------------------------------------------
;WITH N AS (
    SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects
)
INSERT INTO CARD_REF.mcc_category (mcc, mcc_category)
SELECT
    RIGHT('0000' + CAST(5000 + n AS varchar(10)), 4) AS mcc,
    CASE (n - 1) % 12
        WHEN 0  THEN 'Groceries'
        WHEN 1  THEN 'Restaurants'
        WHEN 2  THEN 'Travel'
        WHEN 3  THEN 'Fuel'
        WHEN 4  THEN 'Retail'
        WHEN 5  THEN 'Entertainment'
        WHEN 6  THEN 'Utilities'
        WHEN 7  THEN 'Healthcare'
        WHEN 8  THEN 'Electronics'
        WHEN 9  THEN 'Apparel'
        WHEN 10 THEN 'Services'
        ELSE         'Other'
    END AS mcc_category
FROM N;


-- ---------------------------------------------------------------------
-- merchant — 100 merchants (M0001..M0100), each mapped 1:1 to an MCC above
-- ---------------------------------------------------------------------
;WITH N AS (
    SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects
)
INSERT INTO CARD_REF.merchant (merchant_id, merchant_name, mcc, country)
SELECT
    'M' + RIGHT('0000' + CAST(n AS varchar(10)), 4)        AS merchant_id,
    'Merchant ' + RIGHT('0000' + CAST(n AS varchar(10)), 4) AS merchant_name,
    RIGHT('0000' + CAST(5000 + n AS varchar(10)), 4)        AS mcc,
    CASE n % 6
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'GB'
        WHEN 2 THEN 'CA'
        WHEN 3 THEN 'DE'
        WHEN 4 THEN 'FR'
        ELSE        'AU'
    END AS country
FROM N;


-- ---------------------------------------------------------------------
-- card — 200 cards. card_number_hash = SHA-256 of card_id (deterministic,
-- unique, 64 hex chars) so the generator can emit matching hashes.
-- ~8% inactive (every 25th card).
-- ---------------------------------------------------------------------
;WITH N AS (
    SELECT TOP (200) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects
)
INSERT INTO CARD_REF.card (card_id, card_number_hash, customer_id, issue_date, is_active)
SELECT
    n AS card_id,
    CONVERT(char(64), HASHBYTES('SHA2_256', CONVERT(varchar(20), n)), 2) AS card_number_hash,
    100000 + n AS customer_id,
    DATEADD(DAY, -((n * 11) % 1500), '2025-01-01') AS issue_date,
    CASE WHEN n % 25 = 0 THEN 0 ELSE 1 END AS is_active
FROM N;

COMMIT;
GO


-- =====================================================================
-- Verify — expect 100 / 100 / 200, and ~12 distinct category labels
-- =====================================================================
SELECT 'mcc_category' AS tbl, COUNT(*) AS rows, COUNT(DISTINCT mcc_category) AS distinct_categories FROM CARD_REF.mcc_category
UNION ALL
SELECT 'merchant', COUNT(*), COUNT(DISTINCT mcc) FROM CARD_REF.merchant
UNION ALL
SELECT 'card', COUNT(*), SUM(CAST(is_active AS int)) FROM CARD_REF.card;
GO
