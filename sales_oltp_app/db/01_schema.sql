-- Sales OLTP — schema. Run as root in the sales_oltp database.
-- Authoritative DDL matches docs/Sales_OLTP_Design.md. Idempotent: drops in reverse-FK order first.
USE sales_oltp;

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS stores;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS product_categories;

-- ---------- reference ----------
CREATE TABLE product_categories (
  category_id    INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  category_name  VARCHAR(80)     NOT NULL,
  created_at     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (category_id),
  UNIQUE KEY uq_product_categories_name (category_name)
) ENGINE=InnoDB;

CREATE TABLE customers (
  customer_id  BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  first_name   VARCHAR(60)      NOT NULL,
  last_name    VARCHAR(60)      NOT NULL,
  email        VARCHAR(255)     NOT NULL,
  segment      VARCHAR(20)      NOT NULL DEFAULT 'CONSUMER',
  city         VARCHAR(80)              ,
  state        VARCHAR(40)              ,
  country      VARCHAR(40)      NOT NULL DEFAULT 'USA',
  signup_date  DATE             NOT NULL,
  created_at   TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (customer_id),
  UNIQUE KEY uq_customers_email (email),
  KEY ix_customers_updated_at (updated_at),
  CONSTRAINT chk_customers_segment CHECK (segment IN ('CONSUMER','SMB','ENTERPRISE'))
) ENGINE=InnoDB;

CREATE TABLE products (
  product_id    BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  sku           VARCHAR(40)      NOT NULL,
  product_name  VARCHAR(150)     NOT NULL,
  category_id   INT UNSIGNED     NOT NULL,
  unit_price    DECIMAL(10,2)    NOT NULL,
  active_flag   TINYINT(1)       NOT NULL DEFAULT 1,
  created_at    TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (product_id),
  UNIQUE KEY uq_products_sku (sku),
  KEY ix_products_category (category_id),
  KEY ix_products_updated_at (updated_at),
  CONSTRAINT fk_products_category
    FOREIGN KEY (category_id) REFERENCES product_categories(category_id),
  CONSTRAINT chk_products_price CHECK (unit_price >= 0)
) ENGINE=InnoDB;

CREATE TABLE stores (
  store_id    INT UNSIGNED   NOT NULL AUTO_INCREMENT,
  store_name  VARCHAR(100)   NOT NULL,
  channel     VARCHAR(20)    NOT NULL DEFAULT 'RETAIL',
  region      VARCHAR(40)    NOT NULL,
  city        VARCHAR(80)            ,
  state       VARCHAR(40)            ,
  country     VARCHAR(40)    NOT NULL DEFAULT 'USA',
  created_at  TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (store_id),
  UNIQUE KEY uq_stores_name (store_name),
  CONSTRAINT chk_stores_channel CHECK (channel IN ('RETAIL','ONLINE','PARTNER'))
) ENGINE=InnoDB;

-- ---------- transactional ----------
CREATE TABLE orders (
  order_id     BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  customer_id  BIGINT UNSIGNED  NOT NULL,
  store_id     INT UNSIGNED     NOT NULL,
  order_ts     DATETIME         NOT NULL,
  order_date   DATE             NOT NULL,
  status       VARCHAR(20)      NOT NULL DEFAULT 'PLACED',
  order_total  DECIMAL(12,2)    NOT NULL DEFAULT 0.00,
  created_at   TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (order_id),
  KEY ix_orders_customer (customer_id),
  KEY ix_orders_store (store_id),
  KEY ix_orders_order_date (order_date),
  KEY ix_orders_updated_at (updated_at),
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT fk_orders_store    FOREIGN KEY (store_id)    REFERENCES stores(store_id),
  CONSTRAINT chk_orders_status
    CHECK (status IN ('PLACED','PAID','SHIPPED','DELIVERED','CANCELLED','RETURNED'))
) ENGINE=InnoDB;

CREATE TABLE order_items (
  order_id      BIGINT UNSIGNED   NOT NULL,
  line_no       SMALLINT UNSIGNED NOT NULL,
  product_id    BIGINT UNSIGNED   NOT NULL,
  quantity      INT UNSIGNED      NOT NULL,
  unit_price    DECIMAL(10,2)     NOT NULL,
  discount_pct  DECIMAL(5,4)      NOT NULL DEFAULT 0.0000,
  line_amount   DECIMAL(12,2) AS (ROUND(quantity * unit_price * (1 - discount_pct), 2)) STORED,
  created_at    TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (order_id, line_no),
  KEY ix_order_items_product (product_id),
  KEY ix_order_items_updated_at (updated_at),
  CONSTRAINT fk_order_items_order
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
  CONSTRAINT fk_order_items_product
    FOREIGN KEY (product_id) REFERENCES products(product_id),
  CONSTRAINT chk_order_items_qty  CHECK (quantity > 0),
  CONSTRAINT chk_order_items_disc CHECK (discount_pct >= 0 AND discount_pct < 1)
) ENGINE=InnoDB;
