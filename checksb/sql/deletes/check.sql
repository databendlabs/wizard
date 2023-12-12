-- CHECK-C1: Count of remaining assets
SELECT COUNT(*) AS remaining_assets FROM assets;

-- CHECK-C2: Average price of remaining orders
SELECT AVG(price) AS avg_price_orders FROM orders;

-- CHECK-C3 (After DELETE): Sum or average of the quantity column in remaining transactions
SELECT
    SUM(quantity) AS total_quantity_transactions,
    AVG(quantity) AS average_quantity_transactions
FROM transactions;


-- CHECK-C4: Average quantity of remaining orders
SELECT AVG(quantity) AS avg_quantity_orders FROM orders;

-- CHECK-C5: Count of transactions from user_ids greater than or equal to 50000
SELECT COUNT(*) AS transactions_high_user_id FROM transactions WHERE user_id >= 50000;
