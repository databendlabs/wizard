DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id INT UNSIGNED NOT NULL,
    customer_name VARCHAR NOT NULL,
    segment VARCHAR NOT NULL,
    create_timestamp DATE NOT NULL,
    active BOOLEAN NOT NULL
);

DROP TABLE IF EXISTS date_dim;
CREATE TABLE date_dim (
    date_key DATE NOT NULL,
    day_of_week TINYINT UNSIGNED NOT NULL,
    month TINYINT UNSIGNED NOT NULL,
    year SMALLINT UNSIGNED NOT NULL
);

DROP TABLE IF EXISTS products;
CREATE TABLE products
(
    product_id   INT UNSIGNED NOT NULL,
    product_name VARCHAR        NOT NULL,
    price        DECIMAL(10, 2) NOT NULL,
    category     VARCHAR        NOT NULL
);

DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    sale_id INT UNSIGNED NOT NULL,
    product_id INT UNSIGNED NOT NULL,
    customer_id INT UNSIGNED NOT NULL,
    sale_date DATE NOT NULL,
    quantity INT NOT NULL,
    net_paid DECIMAL(10, 2) NOT NULL
);

CREATE STAGE IF NOT EXISTS wizardbend
    URL = 's3://wizardbend/'
    CONNECTION = (ALLOW_ANONYMOUS = 'true');

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



