-- Streaming hello-world setup (run as root on Zeenie's MySQL). See
-- lakehouse_fundamentals/docs/05_Kafka_hello_world_design.md.
--
-- (1) Debezium replication user. Lab-simple password (LAN-only, like sales_app).
--     REPLICATION SLAVE/CLIENT -> read the binlog stream + position; SELECT -> initial
--     snapshot; RELOAD -> brief consistent-snapshot lock; SHOW DATABASES -> discovery.
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- (2) The single source table this hello-world watches.
CREATE TABLE IF NOT EXISTS sales_oltp.kafka_test (
  id         INT PRIMARY KEY,
  note       VARCHAR(200),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
