-- Query SA-1: Set 'active' status to FALSE for all customers with odd-numbered IDs
UPDATE customers
SET active = FALSE
WHERE MOD(customer_id, 2) = 1;

-- Query SA-2: Decrease the price of all 'Grocery' products by 10%
UPDATE products
SET price = price * 0.90
WHERE category = 'Grocery';

-- Query SA-3: Delete sales records with quantities greater than 15
DELETE FROM sales
WHERE quantity > 15;

-- Query SA-4: Update the sale date to '2023-01-01' for all sales of 'Furniture' category products
UPDATE sales
SET sale_date = '2023-01-01'
WHERE product_id IN (
    SELECT product_id
    FROM products
    WHERE category = 'Furniture'
);
