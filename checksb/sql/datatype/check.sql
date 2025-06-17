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
       variant_string:simple_string::VARCHAR as simple_string,
       variant_number:max_int::BIGINT as max_int_from_variant,
       variant_number:min_int::BIGINT as min_int_from_variant,
       variant_number:float_max::DOUBLE as float_max_from_variant,
       variant_number:scientific::DOUBLE as scientific_notation,
       variant_boolean:`true`::BOOLEAN as true_from_variant,
       variant_boolean:`false`::BOOLEAN as false_from_variant,
       variant_array[0]:id::INT as first_array_element,
       variant_object:level1.level2.level3.level4.level5.data::VARCHAR as deep_nested_value
FROM test_variants
ORDER BY id;


-- Test 8: Edge case detection
SELECT 'Test 8: Edge cases' as test_name,
       id,
       CASE WHEN int_max = 2147483647 THEN 'INT_MAX' ELSE 'OTHER' END as int_max_check,
       CASE WHEN bigint_min = -9223372036854775808 THEN 'BIGINT_MIN' ELSE 'OTHER' END as bigint_min_check,
       CASE WHEN float_val > 1e38 THEN 'FLOAT_EXTREME' ELSE 'NORMAL' END as float_check,
       CASE WHEN double_val > 1e308 THEN 'DOUBLE_EXTREME' ELSE 'NORMAL' END as double_check
FROM test_numbers WHERE id <= 3
ORDER BY id;
