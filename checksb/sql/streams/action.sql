-- STREAM-A01: Simulated Update as Insert for 'customers'
INSERT INTO customers (customer_id, customer_name, segment, create_timestamp, active)
SELECT
        customer_id + 1000000, -- Ensuring a unique customer_id
        CONCAT(customer_name, ' - Updated'),
        CASE WHEN segment = 'Small' THEN 'Medium' ELSE 'Large' END as segment, -- Changing segment
        '2022-01-01', -- Fixed date for simulation
        NOT active -- Inverting active status
FROM customers;

-- STREAM-A02: Simulated Update as Insert for 'products'
INSERT INTO products (product_id, product_name, price, category)
SELECT
        product_id + 100000, -- Ensuring a unique product_id
        CONCAT(product_name, ' - New Edition'),
        price * 1.1, -- Increasing price
        category
FROM products;

-- STREAM-A03: Simulated Update as Insert for 'sales'
INSERT INTO sales (sale_id, product_id, customer_id, sale_date, quantity, net_paid)
SELECT
        sale_id + 5000000, -- Ensuring a unique sale_id
        product_id,
        customer_id,
        '2022-02-01', -- Fixed date for simulation
        quantity + 1, -- Incremented quantity
        net_paid * 1.05 -- Adjusted net_paid
FROM sales;
