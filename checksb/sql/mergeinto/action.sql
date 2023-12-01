-- MERGE-INTO-A1: Conditional Merge Based on Order Status
-- This query updates orders with a status of 'pending' to 'completed'.
MERGE INTO orders USING (
    SELECT * FROM orders WHERE status = 'pending'
) AS pending_orders ON orders.order_id = pending_orders.order_id
    WHEN MATCHED THEN
        UPDATE SET orders.status = 'completed';

-- MERGE-INTO-A2: Merging with Aggregated Data
-- This query updates the quantity of orders based on the sum of quantities for each user and asset type.
MERGE INTO orders USING (
    SELECT user_id, asset_type, SUM(quantity) AS total_quantity
    FROM orders
    GROUP BY user_id, asset_type
) AS agg_orders ON orders.user_id = agg_orders.user_id AND orders.asset_type = agg_orders.asset_type
    WHEN MATCHED THEN
        UPDATE SET orders.quantity = agg_orders.total_quantity;

-- MERGE-INTO-A3: Merge with Date-Based Condition
-- This query archives orders created before 2022-01-01.
MERGE INTO orders USING (
    SELECT * FROM orders WHERE created_at < '2022-01-01'
) AS old_orders ON orders.order_id = old_orders.order_id
    WHEN MATCHED THEN
        UPDATE SET orders.status = 'archived';

-- MERGE-INTO-A4: Random Swap of Buy and Sell Orders
-- This query randomly swaps 'buy' and 'sell' order types.
MERGE INTO orders USING (
    SELECT order_id,
           user_id,
           CASE WHEN order_type = 'buy' THEN 'sell' ELSE 'buy' END AS order_type,
           asset_type,
           quantity,
           price,
           status,
           created_at,
           updated_at
    FROM orders
) AS swapped_orders ON orders.order_id = swapped_orders.order_id
    WHEN MATCHED THEN
        UPDATE SET orders.order_type = swapped_orders.order_type;

-- MERGE-INTO-A5: Merging with a Fixed Increase in Quantity
-- This query increases the quantity of each order by 10.
MERGE INTO orders USING (
    SELECT * FROM orders
) AS all_orders ON orders.order_id = all_orders.order_id
    WHEN MATCHED THEN
        UPDATE SET orders.quantity = orders.quantity + 10;

-- MERGE-INTO-A6: Merge for Creating Duplicate Orders with New IDs
-- This query duplicates orders with new order IDs.
MERGE INTO orders USING (
    SELECT
            order_id + 500000 AS new_order_id, -- Creating new order_ids
            user_id,
            order_type,
            asset_type,
            quantity,
            price,
            status,
            created_at,
            updated_at
    FROM orders
) AS duplicate_orders ON orders.order_id = duplicate_orders.new_order_id
    WHEN NOT MATCHED THEN
        INSERT (order_id, user_id, order_type, asset_type, quantity, price, status, created_at, updated_at)
            VALUES (duplicate_orders.new_order_id, duplicate_orders.user_id, duplicate_orders.order_type, duplicate_orders.asset_type, duplicate_orders.quantity, duplicate_orders.price, duplicate_orders.status, duplicate_orders.created_at, duplicate_orders.updated_at);

-- MERGE-INTO-A7: Merge with Subquery Join
-- This query updates orders with a new quantity based on an average quantity calculation joined from a subquery.
MERGE INTO orders USING (
    SELECT o.order_id, o.user_id, o.order_type, o.asset_type, o.quantity + a.avg_quantity AS new_quantity, o.price, o.status, o.created_at, o.updated_at
    FROM orders o
             INNER JOIN (
        SELECT user_id, asset_type, AVG(quantity) AS avg_quantity
        FROM orders
        GROUP BY user_id, asset_type
    ) a ON o.user_id = a.user_id AND o.asset_type = a.asset_type
) AS joined_data ON orders.order_id = joined_data.order_id
    WHEN MATCHED THEN
        UPDATE SET orders.quantity = joined_data.new_quantity;

-- MERGE-INTO-A8: Merge with Date Ranges and Status Change
-- This query updates the status of orders created between 2021-01-01 and 2021-06-30.
MERGE INTO orders USING (
    SELECT * FROM orders
    WHERE created_at BETWEEN '2021-01-01' AND '2021-06-30'
) AS date_filtered ON orders.order_id = date_filtered.order_id
    WHEN MATCHED THEN
        UPDATE SET orders.status = CASE WHEN orders.status = 'pending' THEN 'expired' ELSE orders.status END;

-- MERGE-INTO-A9: Complex Merge with Nested Subqueries
-- This query updates the status of orders based on their quantity in relation to the average quantity.
MERGE INTO orders USING (
    SELECT o.order_id, o.user_id, o.order_type, o.asset_type, o.quantity, o.price, o.status, o.created_at, o.updated_at,
           CASE
               WHEN o.quantity < sub.avg_quantity THEN 'below_avg'
               WHEN o.quantity > sub.avg_quantity THEN 'above_avg'
               ELSE 'avg'
               END AS quantity_status
    FROM orders o
             INNER JOIN (
        SELECT user_id, asset_type, AVG(quantity) AS avg_quantity
        FROM orders
        GROUP BY user_id, asset_type
    ) sub ON o.user_id = sub.user_id AND o.asset_type = sub.asset_type
) AS complex_data ON orders.order_id = complex_data.order_id
    WHEN MATCHED THEN
        UPDATE SET orders.status = complex_data.quantity_status;

-- MERGE-INTO-A10: Merge Based on Average Asset Value
-- This query updates the price of orders based on the average value of assets for each user and asset type.
MERGE INTO orders USING (
    SELECT a.user_id, a.asset_type, AVG(a.value) as avg_value
    FROM assets a
    GROUP BY a.user_id, a.asset_type
) AS asset_data ON orders.user_id = asset_data.user_id AND orders.asset_type = asset_data.asset_type
    WHEN MATCHED THEN
        UPDATE SET orders.price = orders.price + asset_data.avg_value;