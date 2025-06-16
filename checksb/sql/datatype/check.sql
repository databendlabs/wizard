-- Boundary value validation queries for all data types
-- Outputs raw values for checksb framework to compare between Databend and Snowflake

-- Test 1: Numeric boundary values (max, min, zero, subnormal)
SELECT 'Test 1: Numeric boundaries' as test_name,
       id,
       int_max,
       int_min,
       bigint_max,
       bigint_min,
       smallint_max,
       smallint_min,
       float_val,
       double_val,
       decimal_val
FROM test_numbers
ORDER BY id;

-- Test 2: Boolean values (true, false, null)
SELECT 'Test 2: Boolean values' as test_name,
       id,
       bool_true,
       bool_false,
       bool_null
FROM test_booleans
ORDER BY id;

-- Test 3: Date/Time boundary values
SELECT 'Test 3: Date/Time boundaries' as test_name,
       id,
       date_val,
       timestamp_val
FROM test_dates
ORDER BY id;

-- Test 4: VARIANT boundary value extraction
SELECT 'Test 4: VARIANT boundaries' as test_name,
       id,
       variant_nested:unicode::VARCHAR as unicode_text,
       variant_nested:escape::VARCHAR as escape_chars,
       variant_number:max_int::BIGINT as max_int_from_variant,
       variant_number:min_int::BIGINT as min_int_from_variant,
       variant_number:float_max::DOUBLE as float_max_from_variant,
       variant_number:scientific::DOUBLE as scientific_notation,
       variant_boolean:true::BOOLEAN as true_from_variant,
       variant_boolean:false::BOOLEAN as false_from_variant,
       variant_array[0]::INT as first_array_element,
       variant_nested:level1.level2.level3.level4.level5.data::VARCHAR as deep_nested_value
FROM test_variants
ORDER BY id;

-- Test 5: Arithmetic operations with boundary values
SELECT 'Test 5: Arithmetic boundary operations' as test_name,
       id,
       int_max,
       int_max + 1 as int_overflow,
       int_min,
       int_min - 1 as int_underflow,
       bigint_max + bigint_max as bigint_double,
       smallint_max * smallint_max as smallint_square,
       float_val * 2 as float_double,
       double_val / 2 as double_half
FROM test_numbers WHERE id <= 2
ORDER BY id;

-- Test 6: Type casting with boundary values
SELECT 'Test 6: Type cast boundaries' as test_name,
       id,
       CAST(int_max AS BIGINT) as int_to_bigint,
       CAST(bigint_max AS FLOAT) as bigint_to_float,
       CAST(float_val AS DOUBLE) as float_to_double,
       CAST(decimal_val AS VARCHAR) as decimal_to_string
FROM test_numbers WHERE id <= 2
ORDER BY id;

-- Test 7: NULL handling across all types
SELECT 'Test 7: NULL handling' as test_name,
       'Numbers' as table_type,
       COUNT(*) as total_rows,
       COUNT(int_max) as non_null_count
FROM test_numbers
UNION ALL
SELECT 'Test 7: NULL handling' as test_name,
       'Booleans' as table_type,
       COUNT(*) as total_rows,
       COUNT(bool_true) as non_null_count
FROM test_booleans
UNION ALL
SELECT 'Test 7: NULL handling' as test_name,
       'Dates' as table_type,
       COUNT(*) as total_rows,
       COUNT(date_val) as non_null_count
FROM test_dates;

-- Test 8: Edge case detection
SELECT 'Test 8: Edge cases' as test_name,
       id,
       CASE WHEN int_max = 2147483647 THEN 'INT_MAX' ELSE 'OTHER' END as int_max_check,
       CASE WHEN bigint_min = -9223372036854775808 THEN 'BIGINT_MIN' ELSE 'OTHER' END as bigint_min_check,
       CASE WHEN float_val > 1e38 THEN 'FLOAT_EXTREME' ELSE 'NORMAL' END as float_check,
       CASE WHEN double_val > 1e308 THEN 'DOUBLE_EXTREME' ELSE 'NORMAL' END as double_check
FROM test_numbers WHERE id <= 3
ORDER BY id;

-- Test 9: Data consistency validation
SELECT 'Test 9: Data consistency' as test_name,
       (SELECT COUNT(*) FROM test_numbers) as numbers_count,
       (SELECT COUNT(*) FROM test_booleans) as booleans_count,
       (SELECT COUNT(*) FROM test_dates) as dates_count,
       (SELECT COUNT(*) FROM test_variants) as variants_count;
