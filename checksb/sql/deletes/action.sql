-- DELETE-A1: Delete assets with quantity over 200
DELETE FROM assets WHERE quantity > 200;

-- DELETE-A2: Delete orders with price less than 100
DELETE FROM orders WHERE price < 100;

-- DELETE-A3: Delete transactions of type 'deposit' involving 'BTC'
DELETE FROM transactions WHERE transaction_type = 'deposit' AND asset_type = 'BTC';

-- DELETE-A4: Delete transactions from user_id less than 50000
DELETE FROM transactions WHERE user_id < 50000;

-- DELETE-B1: Deleting assets that have no corresponding orders (ensuring some remain)
DELETE FROM assets 
WHERE user_id NOT IN (
    SELECT DISTINCT user_id 
    FROM orders 
) AND asset_type <> 'ETH';

-- DELETE-B2: Deleting orders that are not associated with an asset (ensuring some remain)
DELETE FROM orders
WHERE asset_type NOT IN (
    SELECT asset_type 
    FROM assets
) AND order_type <> 'buy';

-- DELETE-B3: Deleting transactions where the user has no orders (ensuring some remain)
DELETE FROM transactions 
WHERE user_id NOT IN (
    SELECT DISTINCT user_id 
    FROM orders 
) AND asset_type <> 'ETH';

-- DELETE-B4: Deleting assets that are not in the orders table (ensuring some remain)
DELETE FROM assets 
WHERE asset_type NOT IN (
    SELECT DISTINCT asset_type 
    FROM orders 
) AND asset_type <> 'BTC';

-- DELETE-B5: Deleting orders where the total quantity is less than a specific threshold (ensuring some remain)
DELETE FROM orders 
WHERE user_id IN (
    SELECT user_id 
    FROM (
        SELECT user_id, SUM(quantity) as total_quantity 
        FROM orders 
        GROUP BY user_id
    ) sub 
    WHERE sub.total_quantity < 1000
) AND order_type <> 'sell';

-- DELETE-B6: Deleting transactions for users who have made orders of a specific type (ensuring some remain)
DELETE FROM transactions 
WHERE user_id IN (
    SELECT DISTINCT user_id 
    FROM orders 
    WHERE order_type = 'sell'
) AND asset_type <> 'BTC';

-- DELETE-B7: Deleting transactions based on multiple criteria (ensuring some remain)
DELETE FROM transactions 
WHERE transaction_id IN (
    SELECT transaction_id 
    FROM transactions 
    WHERE quantity > 50 
    AND transaction_type = 'withdrawal'
    AND user_id IN (
        SELECT DISTINCT user_id 
        FROM assets 
        WHERE asset_type = 'ETH'
    )
) AND asset_type <> 'BTC';

-- DELETE-B8: Deleting orders where the total quantity is less than a specific threshold (ensuring some remain)
DELETE FROM orders 
WHERE asset_type IN (
    SELECT asset_type 
    FROM assets
    WHERE quantity < 100
) AND order_type <> 'buy';

-- DELETE-B9: Deleting transactions of users who have placed orders (ensuring some remain)
DELETE FROM transactions 
WHERE EXISTS (
    SELECT 1 
    FROM orders 
    WHERE orders.user_id = transactions.user_id
) AND asset_type <> 'ETH';
