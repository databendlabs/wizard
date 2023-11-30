-- Query SC-1: Aggregate changes in 'customers_stream'
SELECT
    'customers' AS table_name,
    COUNT(*) AS total_changes,
    COUNT(DISTINCT CASE WHEN active IS FALSE THEN customer_id END) AS total_deactivations,
    COUNT(DISTINCT CASE WHEN active IS TRUE THEN customer_id END) AS total_activations
FROM customers_stream;

-- Query SC-2: Aggregate changes in 'products_stream' by category
SELECT
    'products' AS table_name,
    category,
    COUNT(*) AS changes_per_category,
    AVG(price) AS average_price_after_change
FROM products_stream
GROUP BY category;

-- Query SC-3: Summary of changes in 'sales_stream' by sale date
SELECT
    'sales' AS table_name,
    DATE_TRUNC('month', sale_date) AS month_of_sale,
    COUNT(*) AS number_of_sales_changes,
    SUM(quantity) AS total_quantity_changed
FROM sales_stream
GROUP BY DATE_TRUNC('month', sale_date);

-- Query SC-4: Check updates in 'date_dim_stream'
SELECT
    'date_dim' AS table_name,
    COUNT(*) AS total_changes,
    MAX(date_key) AS latest_date_updated
FROM date_dim_stream;
