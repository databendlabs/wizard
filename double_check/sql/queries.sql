-- Q1
SELECT c.segment, SUM(s.net_paid) AS total_sales
FROM customers c
         JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.segment
ORDER BY total_sales DESC;

-- Q2
SELECT p.category, AVG(s.quantity) AS avg_quantity
FROM products p
         JOIN sales s ON p.product_id = s.product_id
GROUP BY p.category
ORDER BY avg_quantity DESC;

-- Q3
SELECT p.product_name, SUM(s.quantity) AS total_quantity
FROM products p
         JOIN sales s ON p.product_id = s.product_id
WHERE p.category = 'Electronics'
GROUP BY p.product_name
ORDER BY total_quantity DESC
    LIMIT 5;

-- Q4
SELECT dd.year, dd.month, COUNT(*) AS total_sales
FROM date_dim dd
         JOIN sales s ON dd.date_key = s.sale_date
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month
    LIMIT 10;

-- Q5
SELECT dd.year, SUM(s.net_paid) AS total_sales, AVG(p.price) AS avg_price
FROM sales s
         JOIN date_dim dd ON s.sale_date = dd.date_key
         JOIN products p ON s.product_id = p.product_id
GROUP BY dd.year
ORDER BY dd.year;

-- Q6
SELECT segment, COUNT(*) AS active_customers
FROM customers
WHERE active
GROUP BY segment
ORDER BY active_customers DESC;

-- Q7
SELECT dd.day_of_week, AVG(s.net_paid) AS avg_sale_amount
FROM sales s
         JOIN date_dim dd ON s.sale_date = dd.date_key
GROUP BY dd.day_of_week
ORDER BY avg_sale_amount DESC;

-- Q8
SELECT dd.date_key, SUM(s.net_paid) AS total_sales
FROM sales s
         JOIN products p ON s.product_id = p.product_id
         JOIN date_dim dd ON s.sale_date = dd.date_key
WHERE p.category = 'Furniture'
GROUP BY dd.date_key
ORDER BY total_sales DESC
    LIMIT 3;

-- Q9
SELECT SUM(s.net_paid) / (SELECT SUM(net_paid) FROM sales JOIN products ON sales.product_id = products.product_id WHERE products.category = 'Clothing') AS contribution_percentage
FROM sales s
         JOIN products p ON s.product_id = p.product_id
         JOIN customers c ON s.customer_id = c.customer_id
WHERE c.segment = 'Large' AND p.category = 'Clothing';

-- Q10
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    total_sales,
    avg_product_price,
    max_sale_date,
    rank() OVER (PARTITION BY c.segment ORDER BY total_sales DESC) as sales_rank_in_segment,
        CASE
            WHEN total_sales > 10000 THEN 'High'
            WHEN total_sales BETWEEN 5000 AND 10000 THEN 'Medium'
            ELSE 'Low'
            END as sales_volume_category
FROM
    (SELECT
         s.customer_id,
         SUM(s.net_paid) as total_sales,
         AVG(s.net_paid / s.quantity) as avg_product_price,
         MAX(s.sale_date) as max_sale_date
     FROM
         sales s
     GROUP BY
         s.customer_id) as sales_summary
        JOIN
    customers c ON sales_summary.customer_id = c.customer_id
WHERE
        c.active = TRUE AND
        sales_summary.total_sales > (SELECT AVG(total_sales) FROM (SELECT customer_id, SUM(net_paid) as total_sales FROM sales GROUP BY customer_id) as average_sales)
ORDER BY
    c.segment, sales_rank_in_segment
    LIMIT 10;

-- Q11
SELECT
    dd.year,
    p.product_name,
    p.category,
    SUM(s.quantity) as total_sold,
    RANK() OVER (PARTITION BY dd.year, p.category ORDER BY SUM(s.quantity) DESC, p.product_name) as rank_in_category
FROM sales s
         JOIN date_dim dd ON s.sale_date = dd.date_key
         JOIN products p ON s.product_id = p.product_id
GROUP BY dd.year, p.product_name, p.category
HAVING dd.year = 2021
ORDER BY dd.year, p.category, total_sold DESC, p.product_name
    LIMIT 10;


-- Q12
SELECT
    c.customer_name,
    p.category,
    AVG(sales.net_paid) OVER (PARTITION BY p.category) as avg_category_sales,
        s.total_customer_sales,
    RANK() OVER (ORDER BY s.total_customer_sales DESC) as sales_rank,
        CASE
            WHEN s.total_customer_sales > 5000 THEN 'VIP'
            ELSE 'Regular'
            END as customer_status
FROM customers c
         JOIN (
    SELECT customer_id, SUM(net_paid) as total_customer_sales
    FROM sales
    GROUP BY customer_id
) s ON c.customer_id = s.customer_id
         JOIN sales ON sales.customer_id = c.customer_id
         JOIN products p ON sales.product_id = p.product_id
WHERE c.active = TRUE
ORDER BY p.category, sales_rank;

-- Q13
WITH monthly_sales AS (
    SELECT
        c.segment,
        EXTRACT(MONTH FROM s.sale_date) as sale_month,
        SUM(s.net_paid) as monthly_sales
    FROM sales s
             JOIN customers c ON s.customer_id = c.customer_id
    GROUP BY c.segment, sale_month
)
SELECT
    segment,
    sale_month,
    monthly_sales,
    LAG(monthly_sales, 1) OVER (PARTITION BY segment ORDER BY sale_month) as previous_month_sales,
            monthly_sales - LAG(monthly_sales, 1) OVER (PARTITION BY segment ORDER BY sale_month) as sales_difference
FROM monthly_sales
ORDER BY segment, sale_month
    LIMIT 10;

-- Inner Join
SELECT c.segment, SUM(s.net_paid) AS total_sales
FROM customers c
         JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.segment
ORDER BY total_sales DESC;

-- Inner Join
SELECT p.category, AVG(s.quantity) AS avg_quantity
FROM products p
         JOIN sales s ON p.product_id = s.product_id
GROUP BY p.category
ORDER BY avg_quantity DESC;


-- Inner Join with Filtering
SELECT p.product_name, SUM(s.quantity) AS total_quantity
FROM products p
         JOIN sales s ON p.product_id = s.product_id
WHERE p.category = 'Electronics'
GROUP BY p.product_name
ORDER BY total_quantity DESC
    LIMIT 5;

--left join
SELECT c.customer_name, c.segment, COALESCE(SUM(s.net_paid), 0) AS total_spent
FROM customers c
         LEFT JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_name, c.segment
ORDER BY total_spent DESC
    LIMIT 10;

--right join
SELECT p.product_name, p.category, COALESCE(COUNT(s.sale_id), 0) AS sale_count
FROM products p
         RIGHT JOIN sales s ON p.product_id = s.product_id
GROUP BY p.product_name, p.category
ORDER BY sale_count DESC
    LIMIT 10;

-- full outer join
SELECT IFNULL(c.customer_name, 'No Customer') AS customer, IFNULL(p.product_name, 'No Product') AS product, s.sale_date
FROM sales s
         FULL OUTER JOIN customers c ON s.customer_id = c.customer_id
         FULL OUTER JOIN products p ON s.product_id = p.product_id
WHERE s.sale_date BETWEEN '2021-01-01' AND '2021-12-31'
ORDER BY s.sale_date
    LIMIT 10;




