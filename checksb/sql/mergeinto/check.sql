-- MERGE-INTO-C1: Asset Types Distribution
SELECT asset_type, COUNT(*) AS count
FROM assets
GROUP BY asset_type
ORDER BY count DESC, asset_type
    LIMIT 13;

-- MERGE-INTO-C2: Aggregated Quantity and Value Statistics
SELECT SUM(quantity) AS total_quantity,
       AVG(quantity) AS average_quantity,
       SUM(value) AS total_value,
       AVG(value) AS average_value
FROM assets;

-- MERGE-INTO-C3: Assets Counts by User
SELECT user_id, COUNT(*) AS count
FROM assets
GROUP BY user_id
ORDER BY count DESC, user_id
    LIMIT 13;

-- MERGE-INTO-C4: Date Range Analysis of Last Update
SELECT CASE
           WHEN last_updated < '2022-01-01' THEN 'Before 2022'
           ELSE 'After 2021-12-31'
           END AS date_range,
       COUNT(*) AS count
FROM assets
GROUP BY date_range
ORDER BY date_range
    LIMIT 13;

-- MERGE-INTO-C5: General Status Distribution
SELECT status, COUNT(*) AS count
FROM orders
GROUP BY status
ORDER BY count DESC, status
    LIMIT 13;

-- MERGE-INTO-C6: General Quantity Statistics
SELECT SUM(quantity) AS total_quantity,
       AVG(quantity) AS average_quantity,
       MIN(quantity) AS min_quantity,
       MAX(quantity) AS max_quantity
FROM orders;

-- MERGE-INTO-C7: New Orders vs Existing Orders Count
SELECT CASE
           WHEN order_id > 500000 THEN 'New Order'
           ELSE 'Existing Order'
           END AS order_category,
       COUNT(*) AS count
FROM orders
GROUP BY order_category
ORDER BY count DESC
    LIMIT 13;

-- MERGE-INTO-C8: Order Type Distribution
SELECT order_type, COUNT(*) AS count
FROM orders
GROUP BY order_type
ORDER BY count DESC, order_type
    LIMIT 13;

-- MERGE-INTO-C9: Date Range Analysis
SELECT CASE
           WHEN created_at < '2022-01-01' THEN 'Before 2022'
           WHEN created_at BETWEEN '2021-01-01' AND '2021-06-30' THEN 'First Half 2021'
           ELSE 'After 2021-06-30'
           END AS date_range,
       COUNT(*) AS count
FROM orders
GROUP BY date_range
ORDER BY date_range
    LIMIT 13;

-- MERGE-INTO-C10: Price Analysis After Adjustments
SELECT SUM(price) AS total_price,
       AVG(price) AS average_price,
       MIN(price) AS min_price,
       MAX(price) AS max_price
FROM orders;

-- MERGE-INTO-C11: Transaction Types Distribution
SELECT transaction_type, COUNT(*) AS count
FROM transactions
GROUP BY transaction_type
ORDER BY count DESC, transaction_type
    LIMIT 13;

-- MERGE-INTO-C12: Aggregated Quantity Statistics
SELECT SUM(quantity) AS total_quantity,
       AVG(quantity) AS average_quantity,
       MIN(quantity) AS min_quantity,
       MAX(quantity) AS max_quantity
FROM transactions;

-- MERGE-INTO-C13: Transaction Counts by User and Asset Type
SELECT user_id, asset_type, COUNT(*) AS count
FROM transactions
GROUP BY user_id, asset_type
ORDER BY count DESC, user_id, asset_type
    LIMIT 13;

-- MERGE-INTO-C14: Date Range Analysis of Transactions
SELECT CASE
           WHEN transaction_time < '2022-01-01' THEN 'Before 2022'
           ELSE 'After 2021-12-31'
           END AS date_range,
       COUNT(*) AS count
FROM transactions
GROUP BY date_range
ORDER BY date_range
    LIMIT 13;

-- MERGE-INTO-C15: asserts
SELECT asset_type, SUM(quantity) AS total_quantity, SUM(value) AS total_value
FROM assets
GROUP BY asset_type;

-- MERGE-INTO-C16: orders
SELECT asset_type, SUM(quantity) AS total_quantity, AVG(price) AS average_price
FROM orders
GROUP BY asset_type;

-- MERGE-INTO-C17: transactions
SELECT transaction_type, SUM(quantity) AS total_quantity
FROM transactions
GROUP BY transaction_type;
