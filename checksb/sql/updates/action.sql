-- UPDATE-A1: Increase quantity by 10% for all BTC assets
UPDATE assets
SET quantity = quantity * 1.1
WHERE asset_type = 'BTC';

-- UPDATE-A2: Mark all pending orders older than 2021-06-01 as cancelled
UPDATE orders
SET status = 'cancelled'
WHERE status = 'pending' AND created_at < '2021-06-01';

-- UPDATE-A3: Double the value of ETH assets
UPDATE assets
SET value = value * 2
WHERE asset_type = 'ETH';

-- UPDATE-A4: Change order type from 'sell' to 'buy' for all XRP orders made in January 2021
UPDATE orders
SET order_type = 'buy'
WHERE asset_type = 'XRP' AND created_at BETWEEN '2021-01-01' AND '2021-01-31';

-- UPDATE-A5: Reduce the quantity of assets in transactions labeled as 'withdrawal' by 20%
UPDATE transactions
SET quantity = quantity * 0.8
WHERE transaction_type = 'withdrawal';

-- UPDATE-A6: Increase the price of all completed orders by 15%
UPDATE orders
SET price = price * 1.15
WHERE status = 'completed';

-- UPDATE-A8: Convert all 'trade' transactions in 2021 to 'deposit'
UPDATE transactions
SET transaction_type = 'deposit'
WHERE transaction_type = 'trade' AND transaction_time BETWEEN '2021-01-01' AND '2021-12-31';

-- UPDATE-A9: Update the status of all orders created in the first quarter of 2021 to 'completed'
UPDATE orders
SET status = 'completed'
WHERE created_at BETWEEN '2021-01-01' AND '2021-03-31';

-- UPDATE-A10: Increase the quantity of all transactions involving BTC by 30%
UPDATE transactions
SET quantity = quantity * 1.3
WHERE asset_type = 'BTC';
