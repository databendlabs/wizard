-- row_per_block is set to 513, 5113, 51113 to ensure that the data is distributed across multiple blocks.
DROP TABLE IF EXISTS assets;
CREATE TABLE assets (
                        user_id       INT          NOT NULL,
                        asset_type    VARCHAR      NOT NULL,
                        quantity      DECIMAL(18,8) NOT NULL,
                        value         DECIMAL(18,8) NOT NULL,
                        last_updated  DATE         NOT NULL
) row_per_block=513;

DROP TABLE IF EXISTS assets_10w;
CREATE TABLE assets_10w (
                        user_id       INT          NOT NULL,
                        asset_type    VARCHAR      NOT NULL,
                        quantity      DECIMAL(18,8) NOT NULL,
                        value         DECIMAL(18,8) NOT NULL,
                        last_updated  DATE         NOT NULL
) row_per_block=513;

DROP TABLE IF EXISTS assets_10;
CREATE TABLE assets_10 (
                        user_id       INT          NOT NULL,
                        asset_type    VARCHAR      NOT NULL,
                        quantity      DECIMAL(18,8) NOT NULL,
                        value         DECIMAL(18,8) NOT NULL,
                        last_updated  DATE         NOT NULL
) row_per_block=513;

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
                        order_id      INT          NOT NULL,
                        user_id       INT          NOT NULL,
                        order_type    VARCHAR      NOT NULL,
                        asset_type    VARCHAR      NOT NULL,
                        quantity      DECIMAL(18,8) NOT NULL,
                        price         DECIMAL(18,8) NOT NULL,
                        status        VARCHAR      NOT NULL,
                        created_at    DATE         NOT NULL,
                        updated_at    DATE         NOT NULL
) row_per_block=5113;

DROP TABLE IF EXISTS orders_25w;
CREATE TABLE orders_25w (
                        order_id      INT          NOT NULL,
                        user_id       INT          NOT NULL,
                        order_type    VARCHAR      NOT NULL,
                        asset_type    VARCHAR      NOT NULL,
                        quantity      DECIMAL(18,8) NOT NULL,
                        price         DECIMAL(18,8) NOT NULL,
                        status        VARCHAR      NOT NULL,
                        created_at    DATE         NOT NULL,
                        updated_at    DATE         NOT NULL
) row_per_block=5113;

DROP TABLE IF EXISTS orders_25;
CREATE TABLE orders_25 (
                        order_id      INT          NOT NULL,
                        user_id       INT          NOT NULL,
                        order_type    VARCHAR      NOT NULL,
                        asset_type    VARCHAR      NOT NULL,
                        quantity      DECIMAL(18,8) NOT NULL,
                        price         DECIMAL(18,8) NOT NULL,
                        status        VARCHAR      NOT NULL,
                        created_at    DATE         NOT NULL,
                        updated_at    DATE         NOT NULL
) row_per_block=5113;

DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
                              transaction_id    INT          NOT NULL,
                              user_id           INT          NOT NULL,
                              transaction_type  VARCHAR      NOT NULL,
                              asset_type        VARCHAR      NOT NULL,
                              quantity          DECIMAL(18,8) NOT NULL,
                              transaction_time  DATE         NOT NULL
) row_per_block=51113;

DROP TABLE IF EXISTS transactions_50w;
CREATE TABLE transactions_50w (
                              transaction_id    INT          NOT NULL,
                              user_id           INT          NOT NULL,
                              transaction_type  VARCHAR      NOT NULL,
                              asset_type        VARCHAR      NOT NULL,
                              quantity          DECIMAL(18,8) NOT NULL,
                              transaction_time  DATE         NOT NULL
) row_per_block=51113;

DROP TABLE IF EXISTS transactions_50;
CREATE TABLE transactions_50 (
                              transaction_id    INT          NOT NULL,
                              user_id           INT          NOT NULL,
                              transaction_type  VARCHAR      NOT NULL,
                              asset_type        VARCHAR      NOT NULL,
                              quantity          DECIMAL(18,8) NOT NULL,
                              transaction_time  DATE         NOT NULL
) row_per_block=51113;

CREATE STAGE IF NOT EXISTS wizardbend
    URL = 's3://wizardbend/'
    CONNECTION = (ALLOW_ANONYMOUS = 'true');

CREATE STAGE IF NOT EXISTS tanboyu
    URL = 's3://tanboyu/'
    CONNECTION = (ALLOW_ANONYMOUS = 'true');

COPY INTO assets
    FROM @wizardbend/mergeinto/assets.parquet
    FILE_FORMAT = (TYPE = parquet);

COPY INTO assets_10w
    FROM @tanboyu/mergeinto/assets_10w.parquet
    FILE_FORMAT = (TYPE = parquet);

COPY INTO assets_10
    FROM @tanboyu/mergeinto/assets_10.parquet
    FILE_FORMAT = (TYPE = parquet);

COPY INTO orders
    FROM @wizardbend/mergeinto/orders.parquet
    FILE_FORMAT = (type = parquet);

COPY INTO orders_25w
    FROM @tanboyu/mergeinto/orders_25w.parquet
    FILE_FORMAT = (type = parquet);

COPY INTO orders_25
    FROM @tanboyu/mergeinto/orders_25.parquet
    FILE_FORMAT = (type = parquet);

COPY INTO transactions
    FROM @wizardbend/mergeinto/transactions.parquet
    FILE_FORMAT = (type = parquet);

COPY INTO transactions_50w
    FROM @tanboyu/mergeinto/transactions_50w.parquet
    FILE_FORMAT = (type = parquet);

COPY INTO transactions_50
    FROM @tanboyu/mergeinto/transactions_50.parquet
    FILE_FORMAT = (type = parquet);