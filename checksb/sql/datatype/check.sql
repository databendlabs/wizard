-- Boundary value extraction queries for all data types
-- Outputs raw values for checksb framework to compare between Databend and Snowflake

-- Test 1: Extract maximum numeric boundary values
SELECT 'Test 1: Maximum numeric boundaries' as test_name,
       id,
       int_max,
       bigint_max,
       smallint_max,
       float_val,
       double_val,
       decimal_val
FROM test_numbers WHERE id = 1
ORDER BY id;

-- Test 2: Extract minimum numeric boundary values
SELECT 'Test 2: Minimum numeric boundaries' as test_name,
       id,
       int_min,
       bigint_min,
       smallint_min,
       float_val,
       double_val,
       decimal_val
FROM test_numbers WHERE id = 2
ORDER BY id;

-- Test 3: Extract subnormal and edge case numeric values
SELECT 'Test 3: Subnormal and edge values' as test_name,
       id,
       int_min,
       int_max,
       bigint_min,
       bigint_max,
       smallint_min,
       smallint_max,
       float_val,
       double_val,
       decimal_val
FROM test_numbers WHERE id = 3
ORDER BY id;

-- Test 4: Extract zero values across all numeric types
SELECT 'Test 4: Zero value validation' as test_name,
       id,
       int_min,
       int_max,
       bigint_min,
       bigint_max,
       smallint_min,
       smallint_max,
       float_val,
       double_val,
       decimal_val
FROM test_numbers WHERE id = 4
ORDER BY id;

-- Test 5: Extract NULL values across all numeric types
SELECT 'Test 5: NULL value validation' as test_name,
       id,
       int_min,
       int_max,
       bigint_min,
       bigint_max,
       smallint_min,
       smallint_max,
       float_val,
       double_val,
       decimal_val
FROM test_numbers WHERE id = 5
ORDER BY id;

-- Test 6: Extract arithmetic operation results with boundary values
SELECT 'Test 6: Arithmetic boundary operations' as test_name,
       id,
       int_max,
       int_max + 1 as int_max_plus_one,
       int_min,
       int_min - 1 as int_min_minus_one,
       bigint_max,
       bigint_max * 2 as bigint_max_times_two,
       float_val,
       float_val * 2 as float_val_times_two
FROM test_numbers WHERE id IN (1, 2)
ORDER BY id;

-- Test 7: Extract comparison operation results with extreme values
SELECT 'Test 7: Extreme value comparisons' as test_name,
       id,
       int_max,
       int_min,
       CASE WHEN int_max > int_min THEN 'greater' ELSE 'not_greater' END as int_comparison,
       bigint_max,
       bigint_min,
       CASE WHEN bigint_max > bigint_min THEN 'greater' ELSE 'not_greater' END as bigint_comparison,
       float_val,
       -float_val as negative_float_val,
       CASE WHEN float_val > -float_val THEN 'positive' ELSE 'not_positive' END as float_sign
FROM test_numbers WHERE id IN (1, 2, 3)
ORDER BY id;

-- Test 8: Extract aggregation results with boundary values
SELECT 'Test 8: Aggregation with boundaries' as test_name,
       COUNT(*) as total_rows,
       MAX(int_max) as max_int_found,
       MIN(int_min) as min_int_found,
       MAX(bigint_max) as max_bigint_found,
       MIN(bigint_min) as min_bigint_found,
       AVG(decimal_val) as avg_decimal,
       COUNT(float_val) as non_null_floats
FROM test_numbers;

-- Test 9: Extract type casting results with extreme values
SELECT 'Test 9: Type casting boundaries' as test_name,
       id,
       int_max,
       CAST(int_max AS BIGINT) as int_max_as_bigint,
       smallint_max,
       CAST(smallint_max AS INT) as smallint_max_as_int,
       decimal_val,
       CAST(decimal_val AS FLOAT) as decimal_as_float,
       float_val,
       CAST(float_val AS DOUBLE) as float_as_double
FROM test_numbers WHERE id IN (1, 2, 3, 4) AND (int_max IS NOT NULL OR smallint_max IS NOT NULL OR decimal_val IS NOT NULL OR float_val IS NOT NULL)
ORDER BY id;

-- Test 10: Extract boolean values and logic results
SELECT 'Test 10: Boolean extreme validation' as test_name,
       id,
       bool_true,
       bool_false,
       bool_null,
       CASE WHEN bool_true AND NOT bool_false THEN 'logic_correct' ELSE 'logic_incorrect' END as boolean_logic_test
FROM test_booleans
ORDER BY id;

-- Test 11: Extract date/time boundary values
SELECT 'Test 11: Date/Time boundaries' as test_name,
       id,
       date_val,
       timestamp_val,
       EXTRACT(YEAR FROM date_val) as year_extracted,
       EXTRACT(MONTH FROM date_val) as month_extracted,
       EXTRACT(DAY FROM date_val) as day_extracted
FROM test_dates
ORDER BY id;

-- Test 12: Extract date arithmetic results with extreme values
SELECT 'Test 12: Date arithmetic extremes' as test_name,
       id,
       date_val,
       timestamp_val,
       CASE WHEN timestamp_val > date_val THEN 'timestamp_greater' ELSE 'timestamp_not_greater' END as timestamp_date_comparison,
       EXTRACT(YEAR FROM date_val) as extracted_year,
       CASE WHEN EXTRACT(MONTH FROM date_val) = 2 AND EXTRACT(DAY FROM date_val) = 29 THEN 'leap_day' ELSE 'not_leap_day' END as leap_day_check
FROM test_dates WHERE date_val IS NOT NULL
ORDER BY id;

-- Test 13: Extract VARIANT complex structure data
SELECT 'Test 13: VARIANT complex structures' as test_name,
       id,
       variant_null,
       variant_string,
       variant_number,
       variant_boolean,
       variant_array,
       variant_object,
       variant_nested
FROM test_variants
ORDER BY id;

-- Test 14: Extract VARIANT structure metadata
SELECT 'Test 14: VARIANT structure metadata' as test_name,
       id,
       CASE 
         WHEN id = 1 THEN 'Unicode and escaping test'
         WHEN id = 2 THEN 'Special characters and edge cases'
         WHEN id = 3 THEN 'Large data structures'
       END as test_category,
       CASE WHEN variant_nested IS NOT NULL THEN 'has_nested' ELSE 'no_nested' END as nested_status,
       CASE WHEN variant_object IS NOT NULL THEN 'has_object' ELSE 'no_object' END as object_status,
       CASE WHEN variant_array IS NOT NULL THEN 'has_array' ELSE 'no_array' END as array_status,
       CASE WHEN variant_string IS NOT NULL THEN 'has_string' ELSE 'no_string' END as string_status,
       CASE WHEN variant_number IS NOT NULL THEN 'has_number' ELSE 'no_number' END as number_status,
       CASE WHEN variant_boolean IS NOT NULL THEN 'has_boolean' ELSE 'no_boolean' END as boolean_status
FROM test_variants WHERE id IN (1, 2, 3)
ORDER BY id;

-- Test 15: Extract VARIANT type information
SELECT 'Test 15: VARIANT type extraction' as test_name,
       id,
       variant_string,
       variant_number,
       variant_boolean,
       variant_array,
       variant_object
FROM test_variants
ORDER BY id;

-- Test 16: Extract cross-table data counts
SELECT 'Test 16: Cross-table consistency' as test_name,
       'table_counts' as data_type,
       (SELECT COUNT(*) FROM test_numbers) as numbers_count,
       (SELECT COUNT(*) FROM test_variants) as variants_count,
       (SELECT COUNT(*) FROM test_booleans) as booleans_count,
       (SELECT COUNT(*) FROM test_dates) as dates_count;

-- Test 17: Extract extreme value detection results
SELECT 'Test 17: Edge case detection' as test_name,
       id,
       int_max,
       int_min,
       bigint_max,
       bigint_min,
       smallint_max,
       smallint_min,
       ABS(float_val) as abs_float_val,
       ABS(double_val) as abs_double_val,
       CASE WHEN int_max = 2147483647 THEN 'int_max_boundary' ELSE 'int_max_other' END as int_max_type,
       CASE WHEN bigint_min = -9223372036854775808 THEN 'bigint_min_boundary' ELSE 'bigint_min_other' END as bigint_min_type
FROM test_numbers WHERE id IN (1, 2)
ORDER BY id;

-- Test 18: Extract NULL counts across all data types
SELECT 'Test 18: NULL count extraction' as test_name,
       'Numbers' as table_name,
       COUNT(*) as total_rows,
       COUNT(int_min) as non_null_int_min,
       COUNT(bigint_max) as non_null_bigint_max,
       COUNT(float_val) as non_null_float,
       COUNT(double_val) as non_null_double,
       COUNT(decimal_val) as non_null_decimal
FROM test_numbers
UNION ALL
SELECT 'Test 18: NULL count extraction' as test_name,
       'Booleans' as table_name,
       COUNT(*) as total_rows,
       COUNT(bool_true) as non_null_bool_true,
       COUNT(bool_false) as non_null_bool_false,
       COUNT(bool_null) as non_null_bool_null,
       0 as unused1,
       0 as unused2
FROM test_booleans
UNION ALL
SELECT 'Test 18: NULL count extraction' as test_name,
       'Dates' as table_name,
       COUNT(*) as total_rows,
       COUNT(date_val) as non_null_date,
       COUNT(timestamp_val) as non_null_timestamp,
       0 as unused1,
       0 as unused2,
       0 as unused3
FROM test_dates;

-- Test 19: Extract performance indicators
SELECT 'Test 19: Performance indicators' as test_name,
       id,
       CASE WHEN variant_nested IS NOT NULL THEN 'has_complex_nested' ELSE 'no_complex_nested' END as nested_complexity,
       CASE WHEN variant_string IS NOT NULL THEN 'has_string_data' ELSE 'no_string_data' END as string_complexity,
       CASE WHEN variant_array IS NOT NULL THEN 'has_array_data' ELSE 'no_array_data' END as array_complexity,
       CASE WHEN variant_object IS NOT NULL THEN 'has_object_data' ELSE 'no_object_data' END as object_complexity
FROM test_variants
ORDER BY id;

-- Test 20: Extract comprehensive data summary
SELECT 'Test 20: Comprehensive data summary' as test_name,
       'SUMMARY' as category,
       (SELECT COUNT(*) FROM test_numbers WHERE int_max IS NOT NULL OR int_min IS NOT NULL) as numeric_data_rows,
       (SELECT COUNT(*) FROM test_variants WHERE variant_nested IS NOT NULL) as variant_data_rows,
       (SELECT COUNT(*) FROM test_booleans WHERE bool_true IS NOT NULL OR bool_false IS NOT NULL) as boolean_data_rows,
       (SELECT COUNT(*) FROM test_dates WHERE date_val IS NOT NULL OR timestamp_val IS NOT NULL) as datetime_data_rows,
       (SELECT COUNT(*) FROM test_numbers) as total_numbers_rows,
       (SELECT COUNT(*) FROM test_variants) as total_variants_rows,
       (SELECT COUNT(*) FROM test_booleans) as total_booleans_rows,
       (SELECT COUNT(*) FROM test_dates) as total_dates_rows;
