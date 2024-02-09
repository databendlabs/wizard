-- row_per_block is set to 513, 5113, 51113 to ensure that the data is distributed across multiple blocks.
CREATE OR REPLACE TABLE assets (
                        user_id       INT          NOT NULL,
                        asset_type    VARCHAR      NOT NULL,
                        quantity      DECIMAL(18,8) NOT NULL,
                        value         DECIMAL(18,8) NOT NULL,
                        last_updated  DATE         NOT NULL
) row_per_block=513;

CREATE OR REPLACE TABLE orders (
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

CREATE OR REPLACE TABLE transactions (
                              transaction_id    INT          NOT NULL,
                              user_id           INT          NOT NULL,
                              transaction_type  VARCHAR      NOT NULL,
                              asset_type        VARCHAR      NOT NULL,
                              quantity          DECIMAL(18,8) NOT NULL,
                              transaction_time  DATE         NOT NULL
) row_per_block=51113;

CREATE OR REPLACE STAGE wizardbend
    URL = 's3://wizardbend/';

COPY INTO assets
    FROM @wizardbend/mergeinto/assets.parquet
    FILE_FORMAT = (TYPE = parquet);


COPY INTO orders
    FROM @wizardbend/mergeinto/orders.parquet
    FILE_FORMAT = (type = parquet);

COPY INTO transactions
    FROM @wizardbend/mergeinto/transactions.parquet
    FILE_FORMAT = (type = parquet);
