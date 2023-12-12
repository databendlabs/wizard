DROP TABLE IF EXISTS assets;
CREATE TABLE assets (
                        user_id INT NOT NULL,
                        asset_type VARCHAR NOT NULL,
                        quantity DECIMAL(18, 8) NOT NULL,
                        value DECIMAL(18, 8) NOT NULL,
                        last_updated DATE NOT NULL
);

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
                        order_id INT NOT NULL,
                        user_id INT NOT NULL,
                        order_type VARCHAR NOT NULL,
                        asset_type VARCHAR NOT NULL,
                        quantity DECIMAL(18, 8) NOT NULL,
                        price DECIMAL(18, 8) NOT NULL,
                        status VARCHAR NOT NULL,
                        created_at DATE NOT NULL,
                        updated_at DATE NOT NULL
);

DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
                              transaction_id INT NOT NULL,
                              user_id INT NOT NULL,
                              transaction_type VARCHAR NOT NULL,
                              asset_type VARCHAR NOT NULL,
                              quantity DECIMAL(18, 8) NOT NULL,
                              transaction_time DATE NOT NULL
);

CREATE OR REPLACE STAGE wizardbend URL='s3://wizardbend/';
CREATE OR REPLACE FILE FORMAT parquet_format TYPE = 'parquet';

COPY INTO assets
    FROM (SELECT
    $1:user_id::INT,
    $1:asset_type::VARCHAR,
    $1:quantity::DECIMAL(18, 8),
    $1:value::DECIMAL(18, 8),
    $1:last_updated::DATE
    FROM @wizardbend/mergeinto/assets.parquet (file_format => 'parquet_format'));

COPY INTO orders
    FROM (SELECT
    $1:order_id::INT,
    $1:user_id::INT,
    $1:order_type::VARCHAR,
    $1:asset_type::VARCHAR,
    $1:quantity::DECIMAL(18, 8),
    $1:price::DECIMAL(18, 8),
    $1:status::VARCHAR,
    $1:created_at::DATE,
    $1:updated_at::DATE
    FROM @wizardbend/mergeinto/orders.parquet (file_format => 'parquet_format'));

COPY INTO transactions
    FROM (SELECT
    $1:transaction_id::INT,
    $1:user_id::INT,
    $1:transaction_type::VARCHAR,
    $1:asset_type::VARCHAR,
    $1:quantity::DECIMAL(18, 8),
    $1:transaction_time::DATE
    FROM @wizardbend/mergeinto/transactions.parquet (file_format => 'parquet_format'));
