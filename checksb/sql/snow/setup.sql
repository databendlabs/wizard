CREATE TABLE assets (
                        user_id INT NOT NULL,
                        asset_type VARCHAR NOT NULL,
                        quantity DECIMAL(18, 8) NOT NULL,
                        value DECIMAL(18, 8) NOT NULL,
                        last_updated DATE NOT NULL
);

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

CREATE TABLE transactions (
                              transaction_id INT NOT NULL,
                              user_id INT NOT NULL,
                              transaction_type VARCHAR NOT NULL,
                              asset_type VARCHAR NOT NULL,
                              quantity DECIMAL(18, 8) NOT NULL,
                              transaction_time DATE NOT NULL
);

CREATE OR REPLACE FILE FORMAT parquet_format TYPE = 'parquet';

COPY INTO assets
    FROM (SELECT
    $1:user_id::INT,
    $2:asset_type::VARCHAR,
    $3:quantity::DECIMAL(18, 8),
    $4:value::DECIMAL(18, 8),
    $5:last_updated::DATE
    FROM @snow/assets (file_format => 'parquet_format'));

COPY INTO orders
    FROM (SELECT
    $1:order_id::INT,
    $2:user_id::INT,
    $3:order_type::VARCHAR,
    $4:asset_type::VARCHAR,
    $5:quantity::DECIMAL(18, 8),
    $6:price::DECIMAL(18, 8),
    $7:status::VARCHAR,
    $8:created_at::DATE,
    $9:updated_at::DATE
    FROM @snow/orders (file_format => 'parquet_format'));

COPY INTO transactions
    FROM (SELECT
    $1:transaction_id::INT,
    $2:user_id::INT,
    $3:transaction_type::VARCHAR,
    $4:asset_type::VARCHAR,
    $5:quantity::DECIMAL(18, 8),
    $6:transaction_time::DATE
    FROM @snow/transactions (file_format => 'parquet_format'));
