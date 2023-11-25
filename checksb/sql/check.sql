-- 1. Asset Types Distribution
SELECT asset_type, COUNT(*) AS count
FROM assets
GROUP BY asset_type;

-- 2. Aggregated Quantity and Value Statistics
SELECT
    SUM(quantity) AS total_quantity,
    AVG(quantity) AS average_quantity,
    SUM(value) AS total_value,
    AVG(value) AS average_value
FROM assets;

-- 3. Assets Counts by User
SELECT user_id, COUNT(*) AS count
FROM assets
GROUP BY user_id;

-- 4. Date Range Analysis of Last Update
SELECT
    CASE
        WHEN last_updated < '2022-01-01' THEN 'Before 2022'
        ELSE 'After 2021-12-31'
        END AS date_range,
    COUNT(*) AS count
FROM assets
GROUP BY date_range;

-- 5. General Status Distribution
SELECT status, COUNT(*) AS count
FROM orders
GROUP BY status;

-- 6. General Quantity Statistics
SELECT
    SUM(quantity) AS total_quantity,
    AVG(quantity) AS average_quantity,
    MIN(quantity) AS min_quantity,
    MAX(quantity) AS max_quantity
FROM orders;

-- 7. New Orders vs Existing Orders Count
SELECT
    CASE
        WHEN order_id > 500000 THEN 'New Order'
        ELSE 'Existing Order'
        END AS order_category,
    COUNT(*) AS count
FROM orders
GROUP BY order_category;

-- 8. Order Type Distribution
SELECT order_type, COUNT(*) AS count
FROM orders
GROUP BY order_type;

-- 9. Date Range Analysis
SELECT
    CASE
        WHEN created_at < '2022-01-01' THEN 'Before 2022'
        WHEN created_at BETWEEN '2021-01-01' AND '2021-06-30' THEN 'First Half 2021'
        ELSE 'After 2021-06-30'
        END AS date_range,
    COUNT(*) AS count
FROM orders
GROUP BY date_range;

-- 10. Price Analysis After Adjustments
SELECT
    SUM(price) AS total_price,
    AVG(price) AS average_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM orders;

-- 11. Transaction Types Distribution
SELECT transaction_type, COUNT(*) AS count
FROM transactions
GROUP BY transaction_type;

-- 12. Aggregated Quantity Statistics
SELECT
    SUM(quantity) AS total_quantity,
    AVG(quantity) AS average_quantity,
    MIN(quantity) AS min_quantity,
    MAX(quantity) AS max_quantity
FROM transactions;

-- 13. Transaction Counts by User and Asset Type
SELECT user_id, asset_type, COUNT(*) AS count
FROM transactions
GROUP BY user_id, asset_type;

-- 14. Date Range Analysis of Transactions
SELECT
    CASE
        WHEN transaction_time < '2022-01-01' THEN 'Before 2022'
        ELSE 'After 2021-12-31'
        END AS date_range,
    COUNT(*) AS count
FROM transactions
GROUP BY date_range;
