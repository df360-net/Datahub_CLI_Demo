-- Sales OLTP — static reference seed. Run as root (or sales_app) in sales_oltp.
-- Only the truly static reference lives here: product_categories + stores.
-- Products, customers, and order history are produced by the generator
-- (tools/generate_daily_activity.py) so volume and drift stay parameterized.
USE sales_oltp;

INSERT INTO product_categories (category_name) VALUES
  ('Electronics'),
  ('Home & Kitchen'),
  ('Apparel'),
  ('Sports & Outdoors'),
  ('Books'),
  ('Toys & Games');

INSERT INTO stores (store_name, channel, region, city, state, country) VALUES
  ('Web Store',        'ONLINE',  'National',  NULL,          NULL, 'USA'),
  ('NYC Flagship',     'RETAIL',  'Northeast', 'New York',    'NY', 'USA'),
  ('Boston Back Bay',  'RETAIL',  'Northeast', 'Boston',      'MA', 'USA'),
  ('Atlanta Midtown',  'RETAIL',  'Southeast', 'Atlanta',     'GA', 'USA'),
  ('Miami Brickell',   'RETAIL',  'Southeast', 'Miami',       'FL', 'USA'),
  ('Chicago Loop',     'RETAIL',  'Midwest',   'Chicago',     'IL', 'USA'),
  ('Dallas Uptown',    'RETAIL',  'Southwest', 'Dallas',      'TX', 'USA'),
  ('Phoenix Camelback','RETAIL',  'Southwest', 'Phoenix',     'AZ', 'USA'),
  ('LA Westfield',     'RETAIL',  'West',      'Los Angeles', 'CA', 'USA'),
  ('Seattle Partner',  'PARTNER', 'West',      'Seattle',     'WA', 'USA');
