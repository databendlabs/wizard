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

-- Query J01: LEFT JOIN with COUNT - Find top 3 customers with the least purchases
SELECT c.customer_id, c.customer_name, COALESCE(COUNT(s.sale_id), 0) AS purchase_count
FROM customers c
         LEFT JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY purchase_count ASC, c.customer_id ASC
    LIMIT 3;


-- Query J02: INNER JOIN with SUM - Top 3 products by total sales value
SELECT p.product_id, p.product_name, SUM(s.net_paid) AS total_sales_value
FROM products p
         INNER JOIN sales s ON p.product_id = s.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_sales_value DESC, p.product_id ASC
    LIMIT 3;

-- Query J03: INNER JOIN with AVG - Top 3 product categories by average product price
SELECT p.category, AVG(p.price) AS avg_price
FROM products p
         INNER JOIN sales s ON p.product_id = s.product_id
GROUP BY p.category
ORDER BY avg_price DESC, p.category
    LIMIT 3;

-- Query J04: RIGHT JOIN with COUNT - Count of sales for products not sold to 'Large' segment customers
SELECT p.product_id, p.product_name, COUNT(s.sale_id) AS sales_count
FROM products p
         RIGHT JOIN sales s ON p.product_id = s.product_id
         LEFT JOIN customers c ON s.customer_id = c.customer_id AND c.segment != 'Large'
GROUP BY p.product_id, p.product_name
ORDER BY sales_count DESC, p.product_id ASC
    LIMIT 3;

-- Query W1: Rank customers by total spending within each segment and show their average purchase value, limited to top 10
SELECT
    sub.customer_id,
    sub.customer_name,
    sub.segment,
    sub.total_spending,
    RANK() OVER (PARTITION BY sub.segment ORDER BY sub.total_spending DESC) AS rank_in_segment
FROM (
         SELECT
             c.customer_id,
             c.customer_name,
             c.segment,
             SUM(s.net_paid) AS total_spending
         FROM
             customers c
                 JOIN
             sales s ON c.customer_id = s.customer_id
         GROUP BY
             c.customer_id, c.customer_name, c.segment
     ) AS sub
ORDER BY
    sub.segment, rank_in_segment
    LIMIT 10;


-- Query W2: Calculate the cumulative and average sales quantity per month for the first 6 months of 2021
SELECT sale_date, SUM(quantity) OVER (ORDER BY sale_date) AS cumulative_sales,
        AVG(quantity) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_monthly_sales
FROM sales
WHERE sale_date BETWEEN '2021-01-01' AND '2021-06-30'
ORDER BY sale_date
    LIMIT 10;

-- Query W3: Determine the growth in sales quantity for each product from the first sale to the latest sale
SELECT product_id,
       first_sale_quantity,
       last_sale_quantity,
       last_sale_quantity - first_sale_quantity AS growth
FROM (
         SELECT product_id,
                FIRST_VALUE(quantity) OVER (PARTITION BY product_id ORDER BY sale_date) AS first_sale_quantity,
                 LAST_VALUE(quantity) OVER (PARTITION BY product_id ORDER BY sale_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sale_quantity
         FROM sales
     ) AS sub
    LIMIT 10;


-- Query W5: Show the first 10 sales with a running total and running average of net_paid per customer
SELECT customer_id, sale_id, net_paid,
       SUM(net_paid) OVER (PARTITION BY customer_id ORDER BY sale_date) AS running_total,
        AVG(net_paid) OVER (PARTITION BY customer_id ORDER BY sale_date) AS running_avg
FROM sales
ORDER BY customer_id, sale_date
    LIMIT 10;

-- Query W6: Find the top 10 sales with the highest net_paid, including their percentage contribution to total sales, with secondary sorting for unique order
SELECT sale_id, product_id, customer_id, net_paid,
       net_paid / SUM(net_paid) OVER () AS percent_of_total_sales
FROM sales
ORDER BY net_paid DESC, sale_id ASC
    LIMIT 10;


-- Query W7: Display the top 10 products with the most fluctuation in sale quantities (measured by standard deviation), with secondary sorting for unique order
SELECT product_id,
       STDDEV(quantity) OVER (PARTITION BY product_id) AS quantity_stddev
FROM sales
GROUP BY product_id
ORDER BY quantity_stddev DESC, product_id ASC
    LIMIT 10;


-- Query W8: Calculate the average sale value for each customer, compared to the overall average, top 10 customers
SELECT
    customer_id,
    AVG(net_paid) OVER (PARTITION BY customer_id) AS customer_avg,
        AVG(net_paid) OVER () - AVG(net_paid) OVER (PARTITION BY customer_id) AS diff_from_overall_avg
FROM
    sales
ORDER BY
    diff_from_overall_avg DESC, customer_id ASC
    LIMIT 10;


-- Query W9: Top 10 sales with the most recent previous sale date for each product
SELECT sale_id, product_id, sale_date, LAG(sale_date, 1) OVER (PARTITION BY product_id ORDER BY sale_date) AS previous_sale_date
FROM sales
ORDER BY product_id, sale_date
    LIMIT 10;

-- Query W10: Display the top 10 customers by the number of distinct products they have purchased
SELECT customer_id, COUNT(DISTINCT product_id) OVER (PARTITION BY customer_id) AS distinct_product_count
FROM sales
ORDER BY distinct_product_count DESC, customer_id
    LIMIT 10;

