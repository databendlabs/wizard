CREATE OR REPLACE TABLE customers AS SELECT
                              number % 1000000 AS customer_id,
  concat('Customer ', to_string(number % 1000000)) AS customer_name,
  CASE WHEN (rand() * 10000)::int % 3 = 0 THEN 'Small'
       WHEN (rand() * 10000 % 3)::int = 1 THEN 'Medium'
       ELSE 'Large'
END AS segment,
  date_add('year', floor(rand() * 10000 % 5)::int, '2021-01-01') AS create_timestamp,
  (rand() * 10000)::int % 2 = 0 AS active
FROM numbers(1000000);


CREATE OR REPLACE TABLE products AS SELECT
                             number % 100000 AS product_id,
  concat('Product ', to_string(number % 100000)) AS product_name,
  (rand() * 10000 % 2000 * 0.01)::decimal(10, 2) AS price,
  CASE WHEN (rand() * 10000)::int % 4 = 0 THEN 'Electronics'
       WHEN (rand() * 10000 % 4)::int = 1 THEN 'Clothing'
       WHEN (rand() * 10000 % 4)::int = 2 THEN 'Grocery'
       ELSE 'Furniture'
END AS category
FROM numbers(100000);


CREATE OR REPLACE TABLE sales AS SELECT
                          number % 5000000 AS sale_id,
  number % 100000 AS product_id,
  number % 1000000 AS customer_id,
  date_add('day', floor(rand() * 10000 % 365)::int, '2021-01-01') AS sale_date,
  (rand() * 10000 % 20 + 1)::int AS quantity,
  (rand() * 10000 % 2000 * 0.01)::decimal(10, 2) AS net_paid
FROM numbers(5000000);


CREATE OR REPLACE TABLE date_dim AS SELECT
                             date_add('day', number % 365, '2021-01-01') AS date_key,
                             to_day_of_week(date_add('day', number % 365, '2021-01-01')) AS day_of_week,
                             to_month(date_add('day', number % 365, '2021-01-01')) AS month,
  to_year(date_add('day', number % 365, '2021-01-01')) AS year
FROM numbers(365);
