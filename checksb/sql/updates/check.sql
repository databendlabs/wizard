-- UPDATE-C1: Count the number of BTC assets
SELECT COUNT(*) FROM assets WHERE asset_type = 'BTC';

-- UPDATE-C2: Count the number of orders that were pending and are now cancelled, with a creation date before 2021-06-01
SELECT COUNT(*)
FROM orders
WHERE status = 'cancelled' AND created_at < '2021-06-01';

-- UPDATE-C3: Calculate the average value of ETH assets
SELECT AVG(value) FROM assets WHERE asset_type = 'ETH';

-- UPDATE-C4: Count the number of buy orders for XRP in January 2021
SELECT COUNT(*) FROM orders WHERE order_type = 'buy' AND asset_type = 'XRP' AND created_at BETWEEN '2021-01-01' AND '2021-01-31';

-- UPDATE-C5: Calculate the total quantity of withdrawal transactions
SELECT SUM(quantity) FROM transactions WHERE transaction_type = 'withdrawal';

-- UPDATE-C6: Calculate the average price of completed orders
SELECT AVG(price) FROM orders WHERE status = 'completed';

-- UPDATE-C7: Count the assets with a last_updated date after 2022-01-01
SELECT COUNT(*) FROM assets WHERE last_updated > '2022-01-01';

-- UPDATE-C8: Count the number of deposit transactions in 2021
SELECT COUNT(*) FROM transactions WHERE transaction_type = 'deposit' AND transaction_time BETWEEN '2021-01-01' AND '2021-12-31';

-- UPDATE-C9: Count the number of completed orders in the first quarter of 2021
SELECT COUNT(*) FROM orders WHERE status = 'completed' AND created_at BETWEEN '2021-01-01' AND '2021-03-31';

-- UPDATE-C10: Calculate the total quantity of BTC transactions
SELECT SUM(quantity) FROM transactions WHERE asset_type = 'BTC';
