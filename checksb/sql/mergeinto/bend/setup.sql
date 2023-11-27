DROP TABLE IF EXISTS assets;
CREATE TABLE assets (
                        user_id       INT          NOT NULL,
                        asset_type    VARCHAR      NOT NULL,
                        quantity      DECIMAL(18,8) NOT NULL,
                        value         DECIMAL(18,8) NOT NULL,
                        last_updated  DATE         NOT NULL
);

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
);

DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
                              transaction_id    INT          NOT NULL,
                              user_id           INT          NOT NULL,
                              transaction_type  VARCHAR      NOT NULL,
                              asset_type        VARCHAR      NOT NULL,
                              quantity          DECIMAL(18,8) NOT NULL,
                              transaction_time  DATE         NOT NULL
);

CREATE STAGE IF NOT EXISTS wizardbend
    URL = 's3://wizardbend/'
    CONNECTION = (ALLOW_ANONYMOUS = 'true');

COPY INTO assets
    FROM @wizardbend/mergeinto/assets.parquet
    FILE_FORMAT = (TYPE = parquet);


COPY INTO orders
    FROM @wizardbend/mergeinto/orders.parquet
    FILE_FORMAT = (type = parquet);

COPY INTO transactions
    FROM @wizardbend/mergeinto/transactions.parquet
    FILE_FORMAT = (type = parquet);
