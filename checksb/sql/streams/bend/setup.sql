-- row_per_block is set to 513, 5113, 51113 to ensure that the data is distributed across multiple blocks.
CREATE OR REPLACE TABLE customers (
    customer_id INT UNSIGNED NOT NULL,
    customer_name VARCHAR NOT NULL,
    segment VARCHAR NOT NULL,
    create_timestamp DATE NOT NULL,
    active BOOLEAN NOT NULL
) row_per_block=51113;

CREATE OR REPLACE TABLE date_dim (
    date_key DATE NOT NULL,
    day_of_week TINYINT UNSIGNED NOT NULL,
    month TINYINT UNSIGNED NOT NULL,
    year SMALLINT UNSIGNED NOT NULL
) row_per_block=53;

CREATE OR REPLACE TABLE products
(
    product_id   INT UNSIGNED NOT NULL,
    product_name VARCHAR        NOT NULL,
    price        DECIMAL(10, 2) NOT NULL,
    category     VARCHAR        NOT NULL
) row_per_block=5113;

CREATE OR REPLACE TABLE sales (
    sale_id INT UNSIGNED NOT NULL,
    product_id INT UNSIGNED NOT NULL,
    customer_id INT UNSIGNED NOT NULL,
    sale_date DATE NOT NULL,
    quantity INT NOT NULL,
    net_paid DECIMAL(10, 2) NOT NULL
) row_per_block=51113;

CREATE OR REPLACE STAGE wizardbend
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

-- Creating stream for 'customers' table
CREATE OR REPLACE STREAM customers_stream ON TABLE customers;

-- Creating stream for 'date_dim' table
CREATE OR REPLACE STREAM date_dim_stream ON TABLE date_dim;

-- Creating stream for 'products' table
CREATE OR REPLACE STREAM products_stream ON TABLE products;

-- Creating stream for 'sales' table
CREATE OR REPLACE STREAM sales_stream ON TABLE sales;