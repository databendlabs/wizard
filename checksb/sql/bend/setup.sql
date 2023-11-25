CREATE TABLE assets (
                        user_id       INT          NOT NULL,
                        asset_type    VARCHAR      NOT NULL,
                        quantity      DECIMAL(18,8) NOT NULL,
                        value         DECIMAL(18,8) NOT NULL,
                        last_updated  DATE         NOT NULL
);

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

CREATE TABLE transactions (
                              transaction_id    INT          NOT NULL,
                              user_id           INT          NOT NULL,
                              transaction_type  VARCHAR      NOT NULL,
                              asset_type        VARCHAR      NOT NULL,
                              quantity          DECIMAL(18,8) NOT NULL,
                              transaction_time  DATE         NOT NULL
);

COPY INTO assets
    FROM (
    SELECT * FROM @bohu_unload/assets/
    )
    FILE_FORMAT = (type = parquet);

COPY INTO orders
    FROM (
    SELECT * FROM @bohu_unload/orders/
    )
    FILE_FORMAT = (type = parquet);

COPY INTO transactions
    FROM (
    SELECT * FROM @bohu_unload/transactions/
    )
    FILE_FORMAT = (type = parquet);
