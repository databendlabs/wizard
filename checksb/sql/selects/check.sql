-- Query 1: Top 5 customers by total spending, including only active customers
SELECT c.customer_id, c.customer_name, SUM(s.net_paid) AS total_spent
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
WHERE c.active = TRUE
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spent DESC
    LIMIT 5;

-- Query 2: Total sales per category for the first quarter of 2021
SELECT p.category, SUM(s.net_paid) AS total_sales
FROM sales s
         JOIN products p ON s.product_id = p.product_id
WHERE s.sale_date BETWEEN '2021-01-01' AND '2021-03-31'
GROUP BY p.category
ORDER BY total_sales DESC;

-- Query 3: Average sale amount per customer segment
SELECT c.segment, AVG(s.net_paid) AS avg_sale_amount
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
GROUP BY c.segment
ORDER BY avg_sale_amount DESC;

-- Query 4: Top 3 most sold products in each category
SELECT p.category, p.product_name, COUNT(*) AS total_sales
FROM sales s
         JOIN products p ON s.product_id = p.product_id
GROUP BY p.category, p.product_name
ORDER BY p.category, p.product_name DESC, total_sales DESC
    LIMIT 3;

-- Query 5: Sales trend by month in 2021
SELECT dd.month, SUM(s.net_paid) AS monthly_sales
FROM sales s
         JOIN date_dim dd ON s.sale_date = dd.date_key
WHERE dd.year = 2021
GROUP BY dd.month
ORDER BY dd.month;

-- Query 6: Detailed view of customers with the highest number of transactions in 2021
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    MIN(s.sale_date) AS first_transaction_date,
    MAX(s.sale_date) AS last_transaction_date,
    SUM(s.net_paid) AS total_spent,
    COUNT(*) AS transaction_count
FROM
    sales s
        JOIN
    customers c ON s.customer_id = c.customer_id
WHERE
        s.sale_date >= '2021-01-01' AND s.sale_date < '2022-01-01'
GROUP BY
    c.customer_id, c.customer_name, c.segment
ORDER BY
    transaction_count DESC, total_spent DESC, c.customer_id
    LIMIT 5;

-- Query 7: Average product price by category
SELECT p.category, AVG(p.price) AS avg_price
FROM products p
GROUP BY p.category
ORDER BY avg_price DESC;

-- Query 8: Customer ranking by total spending using window function
SELECT
    c.customer_id,
    c.customer_name,
    SUM(s.net_paid) AS total_spent,
    RANK() OVER (ORDER BY SUM(s.net_paid) DESC) AS spending_rank
FROM
    sales s
        JOIN
    customers c ON s.customer_id = c.customer_id
GROUP BY
    c.customer_id, c.customer_name
ORDER BY
    spending_rank
    LIMIT 10;


-- Query 9: Count of active and inactive customers per segment
SELECT c.segment, SUM(CASE WHEN c.active = TRUE THEN 1 ELSE 0 END) AS active_customers,
       SUM(CASE WHEN c.active = FALSE THEN 1 ELSE 0 END) AS inactive_customers
FROM customers c
GROUP BY c.segment
ORDER BY c.segment;

-- Query 10: Total sales per day for the first week of 2021
SELECT dd.date_key, SUM(s.net_paid) AS daily_sales
FROM sales s
         JOIN date_dim dd ON s.sale_date = dd.date_key
WHERE dd.date_key BETWEEN '2021-01-01' AND '2021-01-07'
GROUP BY dd.date_key
ORDER BY dd.date_key;

-- Query 11: Top 5 customers with the least spending in Electronics
SELECT c.customer_id, c.customer_name, SUM(s.net_paid) AS total_spent
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
         JOIN products p ON s.product_id = p.product_id
WHERE p.category = 'Electronics'
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spent ASC
    LIMIT 5;

-- Query 12: Number of products sold per category
SELECT p.category, COUNT(*) AS products_sold
FROM sales s
         JOIN products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY products_sold DESC;

-- Query 13: Total sales and average quantity sold per product
SELECT p.product_id, p.product_name, SUM(s.net_paid) AS total_sales, AVG(s.quantity) AS avg_quantity_sold
FROM sales s
         JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_sales DESC
    LIMIT 10;

-- Query 14: Customers with the most transactions, top 10, with stable results
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(*) AS transaction_count
FROM
    sales s
        JOIN
    customers c ON s.customer_id = c.customer_id
GROUP BY
    c.customer_id, c.customer_name
ORDER BY
    transaction_count DESC, c.customer_id
    LIMIT 10;


-- Query 15: Sales comparison between Small and Large segment customers
SELECT c.segment, SUM(s.net_paid) AS total_sales
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
WHERE c.segment IN ('Small', 'Large')
GROUP BY c.segment
ORDER BY c.segment;


-- Query 16: Customers' last purchase date and the total number of purchases, with stable results
WITH CustomerPurchases AS (
    SELECT
        c.customer_id,
        c.customer_name,
        MAX(s.sale_date) AS last_purchase_date,
        COUNT(*) AS total_purchases
    FROM
        sales s
            JOIN
        customers c ON s.customer_id = c.customer_id
    GROUP BY
        c.customer_id, c.customer_name
)
SELECT *
FROM CustomerPurchases
ORDER BY
    last_purchase_date DESC, total_purchases DESC, customer_id
    LIMIT 10;


-- Query 18: Customers with average spending higher than the overall average
WITH AverageSpending AS (
    SELECT
        AVG(s.net_paid) AS avg_spending
    FROM
        sales s
)
SELECT
    c.customer_id,
    c.customer_name,
    AVG(s.net_paid) AS customer_avg_spending
FROM
    sales s
        JOIN
    customers c ON s.customer_id = c.customer_id
GROUP BY
    c.customer_id, c.customer_name
HAVING
        AVG(s.net_paid) > (SELECT avg_spending FROM AverageSpending)
ORDER BY
    customer_avg_spending DESC
    LIMIT 10;

-- Query 19: Total sales per category in each year
SELECT
    p.category,
    dd.year,
    SUM(s.net_paid) AS total_sales
FROM
    sales s
        JOIN
    products p ON s.product_id = p.product_id
        JOIN
    date_dim dd ON s.sale_date = dd.date_key
GROUP BY
    p.category, dd.year
ORDER BY
    p.category, dd.year;

-- Query 21: Products with sales above average in their category
WITH CategoryAverage AS (
    SELECT
        p.category,
        AVG(s.net_paid) AS avg_sales
    FROM
        sales s
            JOIN
        products p ON s.product_id = p.product_id
    GROUP BY
        p.category
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(s.net_paid) AS total_sales,
    ca.avg_sales
FROM
    sales s
        JOIN
    products p ON s.product_id = p.product_id
        JOIN
    CategoryAverage ca ON p.category = ca.category
GROUP BY
    p.product_id, p.product_name, p.category, ca.avg_sales
HAVING
        SUM(s.net_paid) > ca.avg_sales
ORDER BY
    p.product_id, total_sales DESC LIMIT 10;

