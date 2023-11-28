-- Query MA1: Update 'pending' orders to 'completed'
MERGE INTO orders USING (SELECT * FROM orders WHERE status = 'pending') AS pending_orders
    ON orders.order_id = pending_orders.order_id
    WHEN MATCHED THEN UPDATE SET orders.status = 'completed';

-- Query MA2: Update quantities based on user and asset type sums
MERGE INTO orders USING (
    SELECT user_id, asset_type, SUM(quantity) AS total_quantity FROM orders GROUP BY user_id, asset_type
) AS agg_orders
    ON orders.user_id = agg_orders.user_id AND orders.asset_type = agg_orders.asset_type
    WHEN MATCHED THEN UPDATE SET orders.quantity = agg_orders.total_quantity;

-- Query MA3: Archive orders created before 2022-01-01
MERGE INTO orders USING (SELECT * FROM orders WHERE created_at < '2022-01-01') AS old_orders
    ON orders.order_id = old_orders.order_id
    WHEN MATCHED THEN UPDATE SET orders.status = 'archived';

-- Query MA4: Randomly swap 'buy' and 'sell' order types
MERGE INTO orders USING (
    SELECT order_id, user_id, CASE WHEN order_type = 'buy' THEN 'sell' ELSE 'buy' END AS order_type, asset_type, quantity, price, status, created_at, updated_at FROM orders
) AS swapped_orders
    ON orders.order_id = swapped_orders.order_id
    WHEN MATCHED THEN UPDATE SET orders.order_type = swapped_orders.order_type;

-- Query MA5: Increase each order's quantity by 10
MERGE INTO orders USING (SELECT * FROM orders) AS all_orders
    ON orders.order_id = all_orders.order_id
    WHEN MATCHED THEN UPDATE SET orders.quantity = orders.quantity + 10;

-- Query MA6: Duplicate orders with new IDs
MERGE INTO orders USING (
    SELECT order_id + 500000 AS new_order_id, user_id, order_type, asset_type, quantity, price, status, created_at, updated_at FROM orders
) AS duplicate_orders
    ON orders.order_id = duplicate_orders.new_order_id
    WHEN NOT MATCHED THEN INSERT (order_id, user_id, order_type, asset_type, quantity, price, status, created_at, updated_at)
        VALUES (duplicate_orders.new_order_id, duplicate_orders.user_id, duplicate_orders.order_type, duplicate_orders.asset_type, duplicate_orders.quantity, duplicate_orders.price, duplicate_orders.status, duplicate_orders.created_at, duplicate_orders.updated_at);

-- Query MA7: Update orders with joined average quantities
MERGE INTO orders USING (
    SELECT o.order_id, o.user_id, o.order_type, o.asset_type, o.quantity + a.avg_quantity AS new_quantity, o.price, o.status, o.created_at, o.updated_at FROM orders o INNER JOIN (SELECT user_id, asset_type, AVG(quantity) AS avg_quantity FROM orders GROUP BY user_id, asset_type) a ON o.user_id = a.user_id AND o.asset_type = a.asset_type
) AS joined_data
    ON orders.order_id = joined_data.order_id
    WHEN MATCHED THEN UPDATE SET orders.quantity = joined_data.new_quantity;

-- Query MA8: Update status for orders between 2021-01-01 and 2021-06-30
MERGE INTO orders USING (SELECT * FROM orders WHERE created_at BETWEEN '2021-01-01' AND '2021-06-30') AS date_filtered
    ON orders.order_id = date_filtered.order_id
    WHEN MATCHED THEN UPDATE SET orders.status = CASE WHEN orders.status = 'pending' THEN 'expired' ELSE orders.status END;

-- Query MA9: Update status based on quantity vs average
MERGE INTO orders USING (
    SELECT o.order_id, o.user_id, o.order_type, o.asset_type, o.quantity, o.price, o.status, o.created_at, o.updated_at, CASE WHEN o.quantity < sub.avg_quantity THEN 'below_avg' WHEN o.quantity > sub.avg_quantity THEN 'above_avg' ELSE 'avg' END AS quantity_status FROM orders o INNER JOIN (SELECT user_id, asset_type, AVG(quantity) AS avg_quantity FROM orders GROUP BY user_id, asset_type) sub ON o.user_id = sub.user_id AND o.asset_type = sub.asset_type
) AS complex_data
    ON orders.order_id = complex_data.order_id
    WHEN MATCHED THEN UPDATE SET orders.status = complex_data.quantity_status;

-- Query MA10: Update price based on average asset value
MERGE INTO orders USING (
    SELECT a.user_id, a.asset_type, AVG(a.value) as avg_value FROM assets a GROUP BY a.user_id, a.asset_type
) AS asset_data
    ON orders.user_id = asset_data.user_id AND orders.asset_type = asset_data.asset_type
    WHEN MATCHED THEN UPDATE SET orders.price = orders.price + asset_data.avg_value;
