CREATE OR REPLACE TABLE customers (
                           customer_id INT  NOT NULL,
                           customer_name VARCHAR NOT NULL,
                           segment VARCHAR NOT NULL,
                           create_timestamp DATE NOT NULL,
                           active BOOLEAN NOT NULL
);

CREATE OR REPLACE TABLE date_dim (
                          date_key DATE NOT NULL,
                          day_of_week TINYINT  NOT NULL,
                          month TINYINT  NOT NULL,
                          year SMALLINT  NOT NULL
);

CREATE OR REPLACE TABLE products
(
    product_id   INT  NOT NULL,
    product_name VARCHAR        NOT NULL,
    price        DECIMAL(10, 2) NOT NULL,
    category     VARCHAR        NOT NULL
);

CREATE OR REPLACE TABLE sales (
                       sale_id INT  NOT NULL,
                       product_id INT  NOT NULL,
                       customer_id INT  NOT NULL,
                       sale_date DATE NOT NULL,
                       quantity INT NOT NULL,
                       net_paid DECIMAL(10, 2) NOT NULL
);


CREATE OR REPLACE STAGE wizardbend URL='s3://wizardbend/';
CREATE OR REPLACE FILE FORMAT parquet_format TYPE = 'parquet';

COPY INTO customers
    FROM (SELECT
    $1:customer_id::INT,
    $1:customer_name::VARCHAR,
    $1:segment::VARCHAR,
    $1:create_timestamp::DATE,
    $1:active::BOOLEAN
    FROM @wizardbend/selects/customers.parquet (file_format => 'parquet_format'));

COPY INTO date_dim
    FROM (SELECT
    $1:date_key::DATE,
    $1:day_of_week::TINYINT,
    $1:month::TINYINT,
    $1:year::SMALLINT
    FROM @wizardbend/selects/date_dim.parquet (file_format => 'parquet_format'));

COPY INTO products
    FROM (SELECT
    $1:product_id::INT,
    $1:product_name::VARCHAR,
    $1:price::DECIMAL(10, 2),
    $1:category::VARCHAR
    FROM @wizardbend/selects/products.parquet (file_format => 'parquet_format'));

COPY INTO sales
    FROM (SELECT
    $1:sale_id::INT,
    $1:product_id::INT,
    $1:customer_id::INT,
    $1:sale_date::DATE,
    $1:quantity::INT,
    $1:net_paid::DECIMAL(10, 2)
    FROM @wizardbend/selects/sales.parquet (file_format => 'parquet_format'));

-- Creating stream for 'customers' table
CREATE OR REPLACE STREAM checksb_db.public.customers_stream ON TABLE customers APPEND_ONLY = TRUE;

-- Creating stream for 'date_dim' table
CREATE OR REPLACE STREAM checksb_db.public.date_dim_stream ON TABLE date_dim APPEND_ONLY = TRUE;

-- Creating stream for 'products' table
CREATE OR REPLACE STREAM checksb_db.public.products_stream ON TABLE products APPEND_ONLY = TRUE;

-- Creating stream for 'sales' table
CREATE OR REPLACE STREAM checksb_db.public.sales_stream ON TABLE sales APPEND_ONLY = TRUE;
