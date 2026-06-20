CREATE NAMESPACE IF NOT EXISTS demo.smoke;
CREATE TABLE IF NOT EXISTS demo.smoke.t (id bigint, msg string) USING iceberg;
INSERT INTO demo.smoke.t VALUES (1,'hello lakehouse'),(2,'iceberg on minio');
SELECT count(*) AS row_count FROM demo.smoke.t;
SELECT * FROM demo.smoke.t ORDER BY id;
