SELECT c.segment, COUNT(*) AS total_customers, AVG(s.net_paid) AS average_spent
FROM customers c
         JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.segment;

SELECT p.product_name,
       (SELECT SUM(net_paid) FROM sales WHERE product_id = p.product_id) AS total_revenue
FROM products p
WHERE p.price > 5
ORDER BY total_revenue DESC
    LIMIT 10;


SELECT customer_id, sale_date, net_paid,
       RANK() OVER (PARTITION BY customer_id ORDER BY net_paid DESC) AS rank
FROM sales
WHERE sale_date BETWEEN '2021-01-01' AND '2021-12-31'
    LIMIT 10;


SELECT c.customer_name, p.category, SUM(s.quantity) AS total_quantity, AVG(s.net_paid) AS average_price
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
         JOIN products p ON s.product_id = p.product_id
GROUP BY c.customer_name, p.category
HAVING SUM(s.quantity) > 5
ORDER BY total_quantity DESC
    LIMIT 10;


SELECT d.year, d.month, COUNT(*) AS total_sales, SUM(s.net_paid) AS total_revenue
FROM sales s
         JOIN date_dim d ON s.sale_date = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


SELECT category, COUNT(*) AS product_count, MAX(price) AS highest_price
FROM products
GROUP BY category
ORDER BY product_count DESC, highest_price DESC;



SELECT c.customer_name,
       SUM(CASE WHEN p.category = 'Electronics' THEN s.quantity ELSE 0 END) AS electronics_quantity,
       SUM(CASE WHEN p.category = 'Clothing' THEN s.quantity ELSE 0 END) AS clothing_quantity
FROM sales s
         JOIN customers c ON s.customer_id = c.customer_id
         JOIN products p ON s.product_id = p.product_id
GROUP BY c.customer_name
HAVING SUM(s.quantity) > 10
ORDER BY electronics_quantity DESC, clothing_quantity DESC
    LIMIT 10;


SELECT customer_name, segment,
       (SELECT SUM(net_paid)
        FROM sales
        WHERE customer_id IN
              (SELECT customer_id
               FROM customers
               WHERE segment = c.segment AND active = true))
FROM customers c
WHERE c.customer_id IN (SELECT customer_id FROM sales WHERE net_paid > 100)
    LIMIT 10;
