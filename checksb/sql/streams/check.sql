-- STREAM-C01: Customer Analysis
SELECT
    'Customers' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT CASE WHEN customer_id > 1000000 AND customer_id < 2000000 THEN customer_id END) AS updated_customers,
    COUNT(DISTINCT CASE WHEN active = FALSE THEN customer_id END) AS inactivated_customers
FROM checksb_db.customers_stream;

-- STREAM-C02: Product Analysis
SELECT
    'Products' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT CASE WHEN product_id > 100000 AND product_id < 200000 THEN product_id END) AS new_editions,
    AVG(CASE WHEN product_id > 100000 AND product_id < 200000 THEN price END) AS avg_price_new_editions
FROM checksb_db.products_stream;

-- STREAM-C03: Sales Analysis
SELECT
    'Sales' AS table_name,
    COUNT(*) AS total_records,
    SUM(CASE WHEN sale_id > 5000000 AND sale_id < 6000000 THEN quantity END) AS total_quantity_new_sales,
    AVG(CASE WHEN sale_id > 5000000 AND sale_id < 6000000 THEN net_paid END) AS avg_net_paid_new_sales
FROM checksb_db.sales_stream;
