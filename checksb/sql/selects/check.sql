-- SELECT-GROUPBY1: Calculate total sales amount per customer segment, ordered by segment.
SELECT
    customer_id,
    SUM(net_paid) AS total_sales_amount
FROM
    sales
GROUP BY
    customer_id
ORDER BY
    customer_id
LIMIT 10;

-- SELECT-GROUPBY2: Count the number of sales per product category, ordered by category.
SELECT
    product_id,
    COUNT(*) AS sales_count
FROM
    sales
GROUP BY
    product_id
ORDER BY
    product_id
LIMIT 10;

-- SELECT-GROUPBY3: Calculate the average quantity sold per product category, ordered by category.
SELECT
    product_id,
    TRUNCATE(AVG(quantity), 1) AS avg_quantity_sold
FROM
    sales
GROUP BY
    product_id
ORDER BY
    product_id
LIMIT 3;
-- SELECT-GROUPBY4: Find the total net paid amount per customer, ordered by customer_id.
SELECT
    customer_id,
    SUM(net_paid) AS total_net_paid
FROM
    sales
GROUP BY
    customer_id
ORDER BY
    customer_id
LIMIT 10;

-- SELECT-GROUPBY5: Calculate the total sales amount per month, ordered by month.
SELECT
    sale_date,
    SUM(net_paid) AS total_sales_amount
FROM
    sales
GROUP BY
    sale_date
ORDER BY
    sale_date
LIMIT 10;

-- SELECT-GROUPBY6: Calculate the average net paid amount per year, ordered by year.
SELECT
    sale_date,
    TRUNCATE(AVG(net_paid), 1) AS avg_net_paid
FROM
    sales
GROUP BY
    sale_date
ORDER BY
    sale_date
LIMIT 10;

-- SELECT-GROUPBY7: Count the number of sales per day, ordered by sale_date.
SELECT
    sale_date,
    COUNT(*) AS sales_count
FROM
    sales
GROUP BY
    sale_date
ORDER BY
    sale_date
LIMIT 10;
-- SELECT-GROUPBY8: Calculate the total net paid amount per product, ordered by product_id.
SELECT
    product_id,
    SUM(net_paid) AS total_net_paid
FROM
    sales
GROUP BY
    product_id
ORDER BY
    product_id
LIMIT 10;

-- SELECT-GROUPBY9: Calculate the average quantity sold per customer, ordered by customer_id.
SELECT
    customer_id,
    TRUNCATE(AVG(quantity), 1) AS avg_quantity_sold
FROM
    sales
GROUP BY
    customer_id
ORDER BY
    customer_id
LIMIT 5;

-- SELECT-GROUPBY10: Calculate the total net paid amount per product category, ordered by category.
SELECT
    product_id,
    SUM(net_paid) AS total_net_paid
FROM
    sales
GROUP BY
    product_id
ORDER BY
    product_id
LIMIT 10;

-- SELECT-BASE-1: Top 5 customers by total spending, including only active customers
SELECT c.customer_id, c.customer_name, SUM(s.net_paid) AS total_spent
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
WHERE c.active = TRUE
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spent DESC
    LIMIT 5;

-- SELECT-BASE-2: Total sales per category for the first quarter of 2021
SELECT p.category, SUM(s.net_paid) AS total_sales
FROM sales s
         JOIN products p ON s.product_id = p.product_id
WHERE s.sale_date BETWEEN '2021-01-01' AND '2021-03-31'
GROUP BY p.category
ORDER BY total_sales DESC;

-- SELECT-BASE-3: Average sale amount per customer segment
SELECT c.segment, TRUNCATE(AVG(s.net_paid), 7) AS avg_sale_amount
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
GROUP BY c.segment
ORDER BY avg_sale_amount DESC;

-- SELECT-BASE-5: Sales trend by month in 2021
SELECT dd.month, SUM(s.net_paid) AS monthly_sales
FROM sales s
         JOIN date_dim dd ON s.sale_date = dd.date_key
WHERE dd.year = 2021
GROUP BY dd.month
ORDER BY dd.month;

-- SELECT-BASE-6: Detailed view of customers with the highest number of transactions in 2021
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

-- SELECT-BASE-7: Average product price by category
SELECT p.category, TRUNCATE(AVG(p.price), 7) AS avg_price
FROM products p
GROUP BY p.category
ORDER BY avg_price DESC;

-- SELECT-BASE-8: Customer ranking by total spending using window function
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

-- SELECT-BASE-9: Count of active and inactive customers per segment
SELECT c.segment, SUM(CASE WHEN c.active = TRUE THEN 1 ELSE 0 END) AS active_customers,
       SUM(CASE WHEN c.active = FALSE THEN 1 ELSE 0 END) AS inactive_customers
FROM customers c
GROUP BY c.segment
ORDER BY c.segment;

-- SELECT-BASE-10: Total sales per day for the first week of 2021
SELECT dd.date_key, SUM(s.net_paid) AS daily_sales
FROM sales s
         JOIN date_dim dd ON s.sale_date = dd.date_key
WHERE dd.date_key BETWEEN '2021-01-01' AND '2021-01-07'
GROUP BY dd.date_key
ORDER BY dd.date_key;

-- SELECT-BASE-11: Top 5 customers with the least spending in Electronics
SELECT c.customer_id, c.customer_name, SUM(s.net_paid) AS total_spent
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
         JOIN products p ON s.product_id = p.product_id
WHERE p.category = 'Electronics'
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spent ASC
    LIMIT 5;

-- SELECT-BASE-12: Number of products sold per category
SELECT p.category, COUNT(*) AS products_sold
FROM sales s
         JOIN products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY products_sold DESC;

-- SELECT-BASE-14: Customers with the most transactions, top 10, with stable results
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

-- SELECT-BASE-15: Sales comparison between Small and Large segment customers
SELECT c.segment, SUM(s.net_paid) AS total_sales
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
WHERE c.segment IN ('Small', 'Large')
GROUP BY c.segment
ORDER BY c.segment;

-- SELECT-BASE-16: Customers' last purchase date and the total number of purchases, with stable results
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

-- SELECT-BASE-18: Customers with average spending higher than the overall average
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

-- SELECT-BASE-19: Total sales per category in each year
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

-- SELECT-BASE-21: Products with sales above average in their category
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
    TRUNCATE(ca.avg_sales, 7)
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

-- SELECT-J01: LEFT JOIN with COUNT - Find top 3 customers with the least purchases
SELECT c.customer_id, c.customer_name, COALESCE(COUNT(s.sale_id), 0) AS purchase_count
FROM customers c
         LEFT JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY purchase_count ASC, c.customer_id ASC
    LIMIT 3;


-- SELECT-J02: INNER JOIN with SUM - Top 3 products by total sales value
SELECT p.product_id, p.product_name, SUM(s.net_paid) AS total_sales_value
FROM products p
         INNER JOIN sales s ON p.product_id = s.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_sales_value DESC, p.product_id ASC
    LIMIT 3;

-- SELECT-J03: INNER JOIN with AVG - Top 3 product categories by average product price
SELECT p.category, TRUNCATE(AVG(p.price), 7) AS avg_price
FROM products p
         INNER JOIN sales s ON p.product_id = s.product_id
GROUP BY p.category
ORDER BY avg_price DESC, p.category
    LIMIT 3;

-- SELECT-J04: RIGHT JOIN with COUNT - Count of sales for products not sold to 'Large' segment customers
SELECT p.product_id, p.product_name, COUNT(s.sale_id) AS sales_count
FROM products p
         RIGHT JOIN sales s ON p.product_id = s.product_id
         LEFT JOIN customers c ON s.customer_id = c.customer_id AND c.segment != 'Large'
GROUP BY p.product_id, p.product_name
ORDER BY sales_count DESC, p.product_id ASC
    LIMIT 3;

-- SELECT-J05: Join all tables, aggregate data, and use window functions to rank products within each customer segment based on their net paid amount
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    p.product_name,
    p.category,
    s.sale_date,
    SUM(s.net_paid) as total_net_paid,
    RANK() OVER (PARTITION BY c.segment ORDER BY SUM(s.net_paid) DESC) as rank_in_segment
FROM customers c
         JOIN sales s ON c.customer_id = s.customer_id
         JOIN products p ON s.product_id = p.product_id
         JOIN date_dim d ON s.sale_date = d.date_key
GROUP BY c.customer_id, c.customer_name, c.segment, p.product_name, p.category, s.sale_date
ORDER BY c.segment, rank_in_segment, c.customer_id, s.sale_date
    LIMIT 10;

-- SELECT-J06: Aggregate sales data by product category and month, and find top selling categories each month
SELECT
    p.category, d.month, d.year,
    SUM(s.quantity) as total_quantity_sold,
    ROW_NUMBER() OVER (PARTITION BY d.month, d.year ORDER BY SUM(s.quantity) DESC) as rank
FROM sales s
         JOIN products p ON s.product_id = p.product_id
         JOIN date_dim d ON s.sale_date = d.date_key
GROUP BY p.category, d.month, d.year
ORDER BY d.year, d.month, rank
    LIMIT 10;


-- SELECT-J07: Check the distribution of product categories purchased per customer
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(DISTINCT p.category) as categories_purchased,
    SUM(s.net_paid) as total_spent
FROM customers c
         JOIN sales s ON c.customer_id = s.customer_id
         JOIN products p ON s.product_id = p.product_id
GROUP BY c.customer_id, c.customer_name
ORDER BY categories_purchased DESC, total_spent DESC
    LIMIT 10;

-- SELECT-J08: List sales where customers bought more than one item, ranked by the number of items bought and the total net paid in each sale, with sale_id ensuring stable order
SELECT
    s.sale_id,
    s.customer_id,
    c.customer_name,
    s.product_id,
    p.product_name,
    s.quantity,
    DENSE_RANK() OVER (ORDER BY s.quantity DESC, s.net_paid DESC, s.sale_id) as quantity_rank
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
         JOIN products p ON s.product_id = p.product_id
WHERE s.quantity > 1
ORDER BY quantity_rank, s.sale_id
    LIMIT 10;


-- SELECT-J09: Aggregate sales and customer data to find the average sale amount per customer segment, ranked by average sale amount
SELECT
    c.segment,
    TRUNCATE(AVG(s.net_paid), 7) as avg_sale_amount,
    COUNT(s.sale_id) as number_of_sales,
    RANK() OVER (ORDER BY AVG(s.net_paid) DESC) as avg_sale_rank
FROM customers c
         JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.segment
ORDER BY avg_sale_rank
    LIMIT 10;

-- SELECT-W1: Rank customers by total spending within each segment and show their average purchase value, limited to top 10
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


-- SELECT-W3: Determine the growth in sales quantity for each product from the first sale to the latest sale
SELECT product_id,
       first_sale_quantity,
       last_sale_quantity,
       last_sale_quantity - first_sale_quantity AS growth
FROM (
         SELECT product_id,
                FIRST_VALUE(quantity) OVER (PARTITION BY product_id ORDER BY sale_date ASC, sale_id ASC) AS first_sale_quantity,
                 LAST_VALUE(quantity) OVER (PARTITION BY product_id ORDER BY sale_date ASC, sale_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sale_quantity
         FROM sales
     ) AS sub
ORDER BY growth DESC, product_id ASC
    LIMIT 10;


-- SELECT-W5: Show the first 10 sales with a running total and running average of net_paid per customer
SELECT
    customer_id,
    sale_id,
    net_paid,
    ROUND(SUM(net_paid) OVER (PARTITION BY customer_id ORDER BY sale_date), 4) AS running_total,
    ROUND(AVG(net_paid) OVER (PARTITION BY customer_id ORDER BY sale_date), 4) AS running_avg
FROM
    sales
ORDER BY
    customer_id, sale_date
LIMIT 10;

-- SELECT-W6: Find the top 10 sales with the highest net_paid, including their percentage contribution to total sales, with secondary sorting for unique order
SELECT sale_id, product_id, customer_id, net_paid,
       net_paid / SUM(net_paid) OVER () AS percent_of_total_sales
FROM sales
ORDER BY net_paid DESC, sale_id ASC
    LIMIT 10;

-- SELECT-W8: Calculate the average sale value for each customer, compared to the overall average, top 10 customers 
SELECT customer_id,
    ROUND(AVG(net_paid) OVER (PARTITION BY customer_id), 3) AS customer_avg,
    ROUND(AVG(net_paid) OVER () - AVG(net_paid) OVER (PARTITION BY customer_id), 3) AS diff_from_overall_avg
FROM
    sales
ORDER BY
    diff_from_overall_avg DESC, customer_id ASC
LIMIT 10;

-- SELECT-W9: Top 10 sales with the most recent previous sale date for each product
SELECT sale_id, product_id, sale_date,
       COALESCE(CAST(LAG(sale_date, 1) OVER (PARTITION BY product_id ORDER BY sale_date) AS VARCHAR), 'None') AS previous_sale_date
FROM sales
ORDER BY product_id, sale_date
LIMIT 10;

-- SELECT-W10: Display the top 10 customers by the number of distinct products they have purchased
SELECT customer_id, COUNT(DISTINCT product_id) OVER (PARTITION BY customer_id) AS distinct_product_count
FROM sales
ORDER BY distinct_product_count DESC, customer_id
    LIMIT 10;

-- SELECT-W11: Calculate each customer's average sale value and rank these averages within each customer segment
WITH CustomerAverage AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.segment,
        AVG(s.net_paid) AS avg_sale_value
    FROM
        customers c
            JOIN
        sales s ON c.customer_id = s.customer_id
    GROUP BY
        c.customer_id, c.customer_name, c.segment
)
SELECT
    customer_id,
    customer_name,
    segment,
    avg_sale_value,
    RANK() OVER (PARTITION BY segment ORDER BY avg_sale_value DESC) AS rank_in_segment
FROM
    CustomerAverage
ORDER BY
    segment, rank_in_segment
    LIMIT 10;

-- SELECT-W12: Display the top 5 products with the highest average sales quantity, along with their rank across all categories
WITH ProductAverage AS (
    SELECT
        p.product_id,
        p.product_name,
        AVG(s.quantity) AS avg_quantity
    FROM
        products p
            JOIN
        sales s ON p.product_id = s.product_id
    GROUP BY
        p.product_id, p.product_name
)
SELECT
    product_id,
    product_name,
    TRUNCATE(avg_quantity, 2) AS avg_quantity,
    RANK() OVER (ORDER BY avg_quantity DESC, product_id) AS overall_rank
FROM
    ProductAverage
ORDER BY
    overall_rank
LIMIT 5;
-- SELECT-W13: Calculate a cumulative total of sales and a running three-month average, then rank these by customer
WITH SalesData AS (
    SELECT
        customer_id,
        sale_date,
        net_paid,
        sale_id, -- assuming sale_id is a unique identifier for each sale
        SUM(net_paid) OVER (PARTITION BY customer_id ORDER BY sale_date, sale_id) AS cumulative_sales,
            AVG(net_paid) OVER (PARTITION BY customer_id ORDER BY sale_date, sale_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_3m_avg
    FROM
        sales
)
SELECT
    customer_id,
    sale_date,
    cumulative_sales,
    TRUNCATE(running_3m_avg, 4),
    RANK() OVER (ORDER BY running_3m_avg DESC, cumulative_sales DESC, customer_id, sale_date, sale_id) AS sales_rank
FROM
    SalesData
ORDER BY
    customer_id, sale_date
    LIMIT 10;

-- SELECT-W14: Find the top 5 days with the highest sales, along with a row number indicating their rank ordered by date
SELECT
    sale_date,
    daily_total,
    ROW_NUMBER() OVER (ORDER BY sale_date) AS date_rank
FROM (
         SELECT
             sale_date,
             SUM(net_paid) AS daily_total
         FROM
             sales
         GROUP BY
             sale_date
     ) AS DailySales
ORDER BY
    daily_total DESC
    LIMIT 5;

-- SELECT-W16: Compare each sale's net_paid to the average of the previous 5 sales of the same customer
SELECT
    customer_id,
    sale_id,
    net_paid,
    ROUND(COALESCE(AVG(net_paid) OVER (PARTITION BY customer_id ORDER BY sale_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), 0), 4) AS prev_5_avg
FROM
    sales
ORDER BY
    customer_id, sale_id
LIMIT 10;

-- SELECT-W17: Calculate the cumulative sum of net_paid for each customer, partitioned by customer and ordered by sale_date.
SELECT
    customer_id,
    sale_id,
    net_paid,
    SUM(net_paid) OVER (PARTITION BY customer_id ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum
FROM
    sales
ORDER BY
    customer_id, sale_date
LIMIT 10;


-- SELECT-W18: For each customer, calculate the rank of their sales based on net_paid within their own sales.
WITH CustomerSales AS (
    SELECT
        customer_id,
        sale_id,
        net_paid,
        RANK() OVER (PARTITION BY customer_id ORDER BY net_paid DESC) AS sale_rank
    FROM
        sales
)
SELECT
    customer_id,
    sale_id,
    net_paid,
    sale_rank
FROM
    CustomerSales
ORDER BY
    customer_id, sale_rank
LIMIT 10;

-- SELECT-W19: Calculate the total and average net_paid for each product category, joined with product details.
SELECT
    p.product_id,
    p.product_name,
    SUM(s.net_paid) AS total_net_paid,
    AVG(s.net_paid) AS average_net_paid
FROM
    products p
    JOIN sales s ON p.product_id = s.product_id
GROUP BY
    p.product_id, p.product_name
ORDER BY
    p.product_id
LIMIT 10;

-- SELECT-W20: Find the top 5 customers by their total net_paid, along with their cumulative sales and running average.
WITH CustomerTotals AS (
    SELECT
        customer_id,
        SUM(net_paid) AS total_net_paid,
        AVG(net_paid) AS average_net_paid
    FROM
        sales
    GROUP BY
        customer_id
)
SELECT
    customer_id,
    total_net_paid,
    average_net_paid,
    RANK() OVER (ORDER BY total_net_paid DESC) AS customer_rank
FROM
    CustomerTotals
ORDER BY
    customer_rank
LIMIT 5;

-- SELECT-W21: Calculate the month-over-month growth rate of total sales.
WITH MonthlySales AS (
    SELECT
        DATE_TRUNC('month', sale_date) AS sale_month,
        SUM(net_paid) AS monthly_total
    FROM
        sales
    GROUP BY
        sale_month
)
SELECT
    sale_month,
    monthly_total,
    CASE 
        WHEN COALESCE(LAG(monthly_total, 1) OVER (ORDER BY sale_month), 0) = 0 THEN 'None'
        ELSE CAST(COALESCE(LAG(monthly_total, 1) OVER (ORDER BY sale_month), 0) AS VARCHAR)
    END AS previous_month_total,
    CASE 
        WHEN COALESCE(LAG(monthly_total, 1) OVER (ORDER BY sale_month), 0) = 0 THEN 'None'
        ELSE CAST(ROUND(
            (monthly_total - COALESCE(LAG(monthly_total, 1) OVER (ORDER BY sale_month), 0)) /
            NULLIF(COALESCE(LAG(monthly_total, 1) OVER (ORDER BY sale_month), 0), 0) * 100,
            8
        ) AS VARCHAR)
    END AS month_over_month_growth
FROM
    MonthlySales
ORDER BY
    sale_month
LIMIT 10;

-- SELECT-W22: Calculate the top 5 products by sales quantity, along with their rank within their product category.
WITH ProductQuantities AS (
    SELECT
        p.product_id,
        p.category AS product_category,  -- replace 'category' with the actual column name if different
        SUM(s.quantity) AS total_quantity
    FROM
        sales s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY
        p.product_id, p.category
)
SELECT
    product_id,
    product_category,
    total_quantity,
    RANK() OVER (PARTITION BY product_category ORDER BY total_quantity DESC) AS category_rank
FROM
    ProductQuantities
ORDER BY
    product_id,
    category_rank
LIMIT 5;


-- SELECT-ADVANCED-1: Use window functions to rank customers by total spending, limited to the top 5 in each segment.
WITH CustomerSpending AS (
    SELECT
        c.customer_id AS customer_id_alias,
        segment,
        SUM(net_paid) AS total_spent,
        RANK() OVER (PARTITION BY segment ORDER BY SUM(net_paid) DESC) AS spending_rank
    FROM
        sales s
    JOIN customers c ON s.customer_id = c.customer_id
    GROUP BY
        c.customer_id, c.segment 
)
SELECT
    customer_id_alias,
    segment,
    total_spent
FROM
    CustomerSpending
WHERE
    spending_rank <= 5
ORDER BY
    segment, spending_rank;

-- SELECT-ADVANCED-2: Find the top 3 products with the highest average price in each category, using a strict order.
WITH CategoryAverage AS (
    SELECT
        p.category,
        p.product_id,
        AVG(p.price) AS avg_price,
        COUNT(*) OVER (PARTITION BY p.category) AS category_product_count, 
        RANK() OVER (
            PARTITION BY p.category
            ORDER BY AVG(p.price) DESC, p.product_id ASC 
        ) AS price_rank
    FROM
        products p
    GROUP BY
        p.category, p.product_id
)
SELECT
    product_id,
    category,
    avg_price
FROM
    CategoryAverage
WHERE
    price_rank <= 3
ORDER BY
    category, price_rank;
