-- Sales OLTP — application users. Run as root.
-- sales_app    : database-wide DML (the order-entry app + the daily generator)
-- sales_extract: read-only (the daily feed extractor)
-- Passwords are the lab-simple values in sales_oltp_app/.env (not for production).
-- Idempotent: drop then recreate so re-runs reset the passwords cleanly.

DROP USER IF EXISTS 'sales_app'@'%';
DROP USER IF EXISTS 'sales_extract'@'%';

CREATE USER 'sales_app'@'%'     IDENTIFIED BY 'sales_app';
CREATE USER 'sales_extract'@'%' IDENTIFIED BY 'sales_extract';

GRANT SELECT, INSERT, UPDATE, DELETE ON sales_oltp.* TO 'sales_app'@'%';
GRANT SELECT ON sales_oltp.* TO 'sales_extract'@'%';

FLUSH PRIVILEGES;
