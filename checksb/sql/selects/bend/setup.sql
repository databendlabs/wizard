-- row_per_block is set to 513, 5113, 51113 to ensure that the data is distributed across multiple blocks.
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id INT UNSIGNED NOT NULL,
    customer_name VARCHAR NOT NULL,
    segment VARCHAR NOT NULL,
    create_timestamp DATE NOT NULL,
    active BOOLEAN NOT NULL
) row_per_block=51113;

DROP TABLE IF EXISTS date_dim;
CREATE TABLE date_dim (
    date_key DATE NOT NULL,
    day_of_week TINYINT UNSIGNED NOT NULL,
    month TINYINT UNSIGNED NOT NULL,
    year SMALLINT UNSIGNED NOT NULL
) row_per_block=53;

DROP TABLE IF EXISTS products;
CREATE TABLE products
(
    product_id   INT UNSIGNED NOT NULL,
    product_name VARCHAR        NOT NULL,
    price        DECIMAL(10, 2) NOT NULL,
    category     VARCHAR        NOT NULL
) row_per_block=5113;

DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    sale_id INT UNSIGNED NOT NULL,
    product_id INT UNSIGNED NOT NULL,
    customer_id INT UNSIGNED NOT NULL,
    sale_date DATE NOT NULL,
    quantity INT NOT NULL,
    net_paid DECIMAL(10, 2) NOT NULL
) row_per_block=51113;

CREATE STAGE IF NOT EXISTS wizardbend
    URL = 's3://wizardbend/';

COPY INTO customers
    FROM @wizardbend/selects/customers.parquet
    FILE_FORMAT = (TYPE = parquet);

COPY INTO date_dim
    FROM @wizardbend/selects/date_dim.parquet
    FILE_FORMAT = (TYPE = parquet);

COPY INTO products
    FROM @wizardbend/selects/products.parquet
    FILE_FORMAT = (TYPE = parquet);


COPY INTO sales
    FROM @wizardbend/selects/sales.parquet
    FILE_FORMAT = (TYPE = parquet);



