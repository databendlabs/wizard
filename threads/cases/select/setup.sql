-- Setup script for select benchmark case
DROP DATABASE IF EXISTS benchmark_db;
CREATE DATABASE benchmark_db;
CREATE TABLE benchmark_db.test_table (id INT, name VARCHAR(255));

-- Insert some sample data
INSERT INTO benchmark_db.test_table (id, name) VALUES (0, 'Name 0');
INSERT INTO benchmark_db.test_table (id, name) VALUES (1, 'Name 1');
INSERT INTO benchmark_db.test_table (id, name) VALUES (2, 'Name 2');
INSERT INTO benchmark_db.test_table (id, name) VALUES (3, 'Name 3');
INSERT INTO benchmark_db.test_table (id, name) VALUES (4, 'Name 4');
