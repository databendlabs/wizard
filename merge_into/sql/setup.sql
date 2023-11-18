CREATE TABLE assets CLUSTER BY (user_id) AS SELECT
    number % 100000 AS user_id,
    CASE WHEN (rand() * 10)::int % 3 = 0 THEN 'BTC'
    WHEN (rand() * 10)::int % 3 = 1 THEN 'ETH'
    ELSE 'XRP'
END AS asset_type,
    (rand() * 1000)::decimal(18, 8) AS quantity,
    (rand() * 10000)::decimal(18, 8) AS value,
    date_add('year', floor(rand() * 10 % 5)::int, '2021-01-01') AS last_updated
FROM numbers(100000);


CREATE TABLE orders CLUSTER BY (to_yyyymmddhh(created_at), user_id) AS SELECT
    number % 500000 AS order_id,
    number % 100000 AS user_id,
    CASE WHEN (rand() * 10)::int % 2 = 0 THEN 'buy'
    ELSE 'sell'
END AS order_type,
    CASE WHEN (rand() * 10)::int % 3 = 0 THEN 'BTC'
         WHEN (rand() * 10)::int % 3 = 1 THEN 'ETH'
         ELSE 'XRP'
END AS asset_type,
    (rand() * 100)::decimal(18, 8) AS quantity,
    (rand() * 1000)::decimal(18, 8) AS price,
    CASE WHEN (rand() * 10)::int % 3 = 0 THEN 'completed'
         WHEN (rand() * 10)::int % 3 = 1 THEN 'pending'
         ELSE 'cancelled'
END AS status,
    date_add('day', floor(rand() * 10 % 365)::int, '2021-01-01') AS created_at,
    date_add('day', floor(rand() * 10 % 365)::int, '2021-01-01') AS updated_at
FROM numbers(500000);

CREATE TABLE transactions CLUSTER BY (to_yyyymmddhh(transaction_time), user_id) AS SELECT
    number % 1000000 AS transaction_id,
    number % 100000 AS user_id,
    CASE WHEN (rand() * 10)::int % 3 = 0 THEN 'deposit'
    WHEN (rand() * 10)::int % 3 = 1 THEN 'withdrawal'
    ELSE 'trade'
END AS transaction_type,
    CASE WHEN (rand() * 10)::int % 3 = 0 THEN 'BTC'
         WHEN (rand() * 10)::int % 3 = 1 THEN 'ETH'
         ELSE 'XRP'
END AS asset_type,
    (rand() * 100)::decimal(18, 8) AS quantity,
    date_add('day', floor(rand() * 10 % 365)::int, '2021-01-01') AS transaction_time
FROM numbers(1000000);

