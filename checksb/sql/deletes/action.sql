-- DELETE-A1: Delete assets with quantity over 200
DELETE FROM assets WHERE quantity > 200;

-- DELETE-A2: Delete orders with price less than 100
DELETE FROM orders WHERE price < 100;

-- DELETE-A3: Delete transactions of type 'deposit' involving 'BTC'
DELETE FROM transactions WHERE transaction_type = 'deposit' AND asset_type = 'BTC';

-- DELETE-A4: Delete transactions from user_id less than 50000
DELETE FROM transactions WHERE user_id < 50000;
