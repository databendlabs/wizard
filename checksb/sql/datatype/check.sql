-- Extreme boundary validation queries for all data types
-- Tests robustness and edge case handling across Databend and Snowflake

-- Test 1: Numeric boundary value validation - Maximum values
SELECT 'Test 1: Maximum numeric boundaries' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN int_max = 2147483647 THEN 1 ELSE 0 END) as int_max_correct,
       SUM(CASE WHEN bigint_max = 9223372036854775807 THEN 1 ELSE 0 END) as bigint_max_correct,
       SUM(CASE WHEN smallint_max = 32767 THEN 1 ELSE 0 END) as smallint_max_correct,
       SUM(CASE WHEN ABS(float_val - 3.4028235e+38) < 1e+30 THEN 1 ELSE 0 END) as float_max_correct,
       SUM(CASE WHEN ABS(double_val - 1.7976931348623157e+308) < 1e+300 THEN 1 ELSE 0 END) as double_max_correct
FROM test_numbers WHERE id = 1;

-- Test 2: Numeric boundary value validation - Minimum values
SELECT 'Test 2: Minimum numeric boundaries' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN int_min = -2147483648 THEN 1 ELSE 0 END) as int_min_correct,
       SUM(CASE WHEN bigint_min = -9223372036854775808 THEN 1 ELSE 0 END) as bigint_min_correct,
       SUM(CASE WHEN smallint_min = -32768 THEN 1 ELSE 0 END) as smallint_min_correct,
       SUM(CASE WHEN ABS(float_val + 3.4028235e+38) < 1e+30 THEN 1 ELSE 0 END) as float_min_correct,
       SUM(CASE WHEN ABS(double_val + 1.7976931348623157e+308) < 1e+300 THEN 1 ELSE 0 END) as double_min_correct
FROM test_numbers WHERE id = 2;

-- Test 3: Subnormal and edge case numeric values
SELECT 'Test 3: Subnormal and edge values' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN int_min = 1 AND int_max = -1 THEN 1 ELSE 0 END) as edge_integers_correct,
       SUM(CASE WHEN float_val > 0 AND float_val < 1e-30 THEN 1 ELSE 0 END) as subnormal_float_detected,
       SUM(CASE WHEN double_val > 0 AND double_val < 1e-300 THEN 1 ELSE 0 END) as subnormal_double_detected,
       SUM(CASE WHEN decimal_val = 0.01 THEN 1 ELSE 0 END) as decimal_precision_correct
FROM test_numbers WHERE id = 3;

-- Test 4: Zero value validation across all numeric types
SELECT 'Test 4: Zero value validation' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN int_min = 0 AND int_max = 0 THEN 1 ELSE 0 END) as zero_integers_correct,
       SUM(CASE WHEN bigint_min = 0 AND bigint_max = 0 THEN 1 ELSE 0 END) as zero_bigints_correct,
       SUM(CASE WHEN smallint_min = 0 AND smallint_max = 0 THEN 1 ELSE 0 END) as zero_smallints_correct,
       SUM(CASE WHEN float_val = 0.0 THEN 1 ELSE 0 END) as zero_float_correct,
       SUM(CASE WHEN double_val = 0.0 THEN 1 ELSE 0 END) as zero_double_correct,
       SUM(CASE WHEN decimal_val = 0.00 THEN 1 ELSE 0 END) as zero_decimal_correct
FROM test_numbers WHERE id = 4;

-- Test 5: NULL value validation across all numeric types
SELECT 'Test 5: NULL value validation' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN int_min IS NULL THEN 1 ELSE 0 END) as null_int_min,
       SUM(CASE WHEN bigint_max IS NULL THEN 1 ELSE 0 END) as null_bigint_max,
       SUM(CASE WHEN float_val IS NULL THEN 1 ELSE 0 END) as null_float,
       SUM(CASE WHEN double_val IS NULL THEN 1 ELSE 0 END) as null_double,
       SUM(CASE WHEN decimal_val IS NULL THEN 1 ELSE 0 END) as null_decimal
FROM test_numbers WHERE id = 5;

-- Test 6: Arithmetic operations with boundary values
SELECT 'Test 6: Arithmetic boundary operations' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN int_max + 1 < int_max THEN 1 ELSE 0 END) as int_overflow_detected,
       SUM(CASE WHEN int_min - 1 > int_min THEN 1 ELSE 0 END) as int_underflow_detected,
       SUM(CASE WHEN bigint_max * 2 < bigint_max THEN 1 ELSE 0 END) as bigint_overflow_detected,
       SUM(CASE WHEN float_val * 2 > float_val * 10 THEN 1 ELSE 0 END) as float_arithmetic_valid
FROM test_numbers WHERE id IN (1, 2);

-- Test 7: Comparison operations with extreme values
SELECT 'Test 7: Extreme value comparisons' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN int_max > int_min THEN 1 ELSE 0 END) as int_comparison_correct,
       SUM(CASE WHEN bigint_max > bigint_min THEN 1 ELSE 0 END) as bigint_comparison_correct,
       SUM(CASE WHEN float_val > -float_val THEN 1 ELSE 0 END) as float_sign_comparison,
       SUM(CASE WHEN double_val <> 0 THEN 1 ELSE 0 END) as double_nonzero_detection
FROM test_numbers WHERE id IN (1, 2, 3);

-- Test 8: Aggregation functions with boundary values
SELECT 'Test 8: Aggregation with boundaries' as test_name,
       COUNT(*) as total_rows,
       MAX(int_max) as max_int_found,
       MIN(int_min) as min_int_found,
       MAX(bigint_max) as max_bigint_found,
       MIN(bigint_min) as min_bigint_found,
       AVG(CASE WHEN decimal_val IS NOT NULL THEN decimal_val END) as avg_decimal,
       SUM(CASE WHEN float_val IS NOT NULL THEN 1 ELSE 0 END) as non_null_floats
FROM test_numbers;

-- Test 9: Type casting with extreme values
SELECT 'Test 9: Type casting boundaries' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN CAST(int_max AS BIGINT) = bigint_max THEN 1 ELSE 0 END) as int_to_bigint_cast,
       SUM(CASE WHEN CAST(smallint_max AS INT) = 32767 THEN 1 ELSE 0 END) as smallint_to_int_cast,
       SUM(CASE WHEN CAST(decimal_val AS FLOAT) IS NOT NULL THEN 1 ELSE 0 END) as decimal_to_float_cast,
       SUM(CASE WHEN CAST(float_val AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) as float_to_double_cast
FROM test_numbers WHERE id IN (1, 2, 3, 4);

-- Test 10: Boolean extreme validation
SELECT 'Test 10: Boolean extreme validation' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN bool_true = TRUE THEN 1 ELSE 0 END) as true_values_correct,
       SUM(CASE WHEN bool_false = FALSE THEN 1 ELSE 0 END) as false_values_correct,
       SUM(CASE WHEN bool_null IS NULL THEN 1 ELSE 0 END) as null_values_correct,
       SUM(CASE WHEN bool_true AND NOT bool_false THEN 1 ELSE 0 END) as boolean_logic_correct
FROM test_booleans;

-- Test 11: Date/Time boundary validation
SELECT 'Test 11: Date/Time boundaries' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN date_val = '1900-01-01' THEN 1 ELSE 0 END) as early_date_correct,
       SUM(CASE WHEN date_val = '2099-12-31' THEN 1 ELSE 0 END) as future_date_correct,
       SUM(CASE WHEN date_val = '2000-02-29' THEN 1 ELSE 0 END) as leap_year_correct,
       SUM(CASE WHEN date_val = '1970-01-01' THEN 1 ELSE 0 END) as epoch_date_correct,
       SUM(CASE WHEN date_val IS NULL THEN 1 ELSE 0 END) as null_date_correct
FROM test_dates;

-- Test 12: Date arithmetic with extreme values
SELECT 'Test 12: Date arithmetic extremes' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN timestamp_val > date_val THEN 1 ELSE 0 END) as timestamp_date_comparison,
       SUM(CASE WHEN EXTRACT(YEAR FROM date_val) BETWEEN 1900 AND 2099 THEN 1 ELSE 0 END) as year_range_valid,
       SUM(CASE WHEN EXTRACT(MONTH FROM date_val) = 2 AND EXTRACT(DAY FROM date_val) = 29 THEN 1 ELSE 0 END) as leap_day_detected
FROM test_dates WHERE date_val IS NOT NULL;

-- Test 13: VARIANT complex structure validation
SELECT 'Test 13: VARIANT complex structures' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN variant_null IS NULL THEN 1 ELSE 0 END) as variant_nulls_correct,
       SUM(CASE WHEN variant_string IS NOT NULL THEN 1 ELSE 0 END) as variant_strings_exist,
       SUM(CASE WHEN variant_number IS NOT NULL THEN 1 ELSE 0 END) as variant_numbers_exist,
       SUM(CASE WHEN variant_boolean IS NOT NULL THEN 1 ELSE 0 END) as variant_booleans_exist,
       SUM(CASE WHEN variant_array IS NOT NULL THEN 1 ELSE 0 END) as variant_arrays_exist,
       SUM(CASE WHEN variant_object IS NOT NULL THEN 1 ELSE 0 END) as variant_objects_exist,
       SUM(CASE WHEN variant_nested IS NOT NULL THEN 1 ELSE 0 END) as variant_nested_exist
FROM test_variants;

-- Test 14: VARIANT deep nesting and complexity
SELECT 'Test 14: VARIANT deep nesting' as test_name,
       id,
       CASE 
         WHEN id = 1 THEN 'Unicode and escaping test'
         WHEN id = 2 THEN 'Special characters and edge cases'
         WHEN id = 3 THEN 'Large data structures'
       END as test_category,
       CASE WHEN variant_nested IS NOT NULL THEN 'PASS' ELSE 'FAIL' END as nested_structure_status,
       CASE WHEN variant_object IS NOT NULL THEN 'PASS' ELSE 'FAIL' END as object_structure_status,
       CASE WHEN variant_array IS NOT NULL THEN 'PASS' ELSE 'FAIL' END as array_structure_status
FROM test_variants WHERE id IN (1, 2, 3);

-- Test 15: VARIANT data type extraction and validation
SELECT 'Test 15: VARIANT type extraction' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN variant_string IS NOT NULL THEN 1 ELSE 0 END) as string_variants,
       SUM(CASE WHEN variant_number IS NOT NULL THEN 1 ELSE 0 END) as number_variants,
       SUM(CASE WHEN variant_boolean IS NOT NULL THEN 1 ELSE 0 END) as boolean_variants,
       SUM(CASE WHEN variant_array IS NOT NULL THEN 1 ELSE 0 END) as array_variants,
       SUM(CASE WHEN variant_object IS NOT NULL THEN 1 ELSE 0 END) as object_variants
FROM test_variants;

-- Test 16: Cross-table data consistency validation
SELECT 'Test 16: Cross-table consistency' as test_name,
       (SELECT COUNT(*) FROM test_numbers) as numbers_count,
       (SELECT COUNT(*) FROM test_variants) as variants_count,
       (SELECT COUNT(*) FROM test_booleans) as booleans_count,
       (SELECT COUNT(*) FROM test_dates) as dates_count,
       CASE 
         WHEN (SELECT COUNT(*) FROM test_numbers) = 5 
          AND (SELECT COUNT(*) FROM test_variants) = 3
          AND (SELECT COUNT(*) FROM test_booleans) = 3
          AND (SELECT COUNT(*) FROM test_dates) = 5
         THEN 'PASS' 
         ELSE 'FAIL' 
       END as consistency_check;

-- Test 17: Extreme value edge case detection
SELECT 'Test 17: Edge case detection' as test_name,
       COUNT(*) as total_rows,
       SUM(CASE WHEN int_max = 2147483647 AND int_min = -2147483648 THEN 1 ELSE 0 END) as int_boundaries_detected,
       SUM(CASE WHEN bigint_max = 9223372036854775807 AND bigint_min = -9223372036854775808 THEN 1 ELSE 0 END) as bigint_boundaries_detected,
       SUM(CASE WHEN smallint_max = 32767 AND smallint_min = -32768 THEN 1 ELSE 0 END) as smallint_boundaries_detected,
       SUM(CASE WHEN ABS(float_val) > 1e+30 THEN 1 ELSE 0 END) as extreme_float_values,
       SUM(CASE WHEN ABS(double_val) > 1e+300 THEN 1 ELSE 0 END) as extreme_double_values
FROM test_numbers WHERE id IN (1, 2);

-- Test 18: NULL handling across all data types
SELECT 'Test 18: Comprehensive NULL handling' as test_name,
       'Numbers' as table_name,
       SUM(CASE WHEN int_min IS NULL THEN 1 ELSE 0 END) as null_int_count,
       SUM(CASE WHEN bigint_max IS NULL THEN 1 ELSE 0 END) as null_bigint_count,
       SUM(CASE WHEN float_val IS NULL THEN 1 ELSE 0 END) as null_float_count,
       SUM(CASE WHEN double_val IS NULL THEN 1 ELSE 0 END) as null_double_count,
       SUM(CASE WHEN decimal_val IS NULL THEN 1 ELSE 0 END) as null_decimal_count
FROM test_numbers
UNION ALL
SELECT 'Test 18: Comprehensive NULL handling' as test_name,
       'Booleans' as table_name,
       SUM(CASE WHEN bool_true IS NULL THEN 1 ELSE 0 END) as null_bool_true_count,
       SUM(CASE WHEN bool_false IS NULL THEN 1 ELSE 0 END) as null_bool_false_count,
       SUM(CASE WHEN bool_null IS NULL THEN 1 ELSE 0 END) as null_bool_null_count,
       0 as null_unused1,
       0 as null_unused2
FROM test_booleans
UNION ALL
SELECT 'Test 18: Comprehensive NULL handling' as test_name,
       'Dates' as table_name,
       SUM(CASE WHEN date_val IS NULL THEN 1 ELSE 0 END) as null_date_count,
       SUM(CASE WHEN timestamp_val IS NULL THEN 1 ELSE 0 END) as null_timestamp_count,
       0 as null_unused1,
       0 as null_unused2,
       0 as null_unused3
FROM test_dates;

-- Test 19: Performance and stress test indicators
SELECT 'Test 19: Performance indicators' as test_name,
       COUNT(*) as total_variant_rows,
       SUM(CASE WHEN variant_nested IS NOT NULL THEN 1 ELSE 0 END) as complex_nested_structures,
       SUM(CASE WHEN variant_string IS NOT NULL THEN 1 ELSE 0 END) as string_structures,
       SUM(CASE WHEN variant_array IS NOT NULL THEN 1 ELSE 0 END) as array_structures,
       AVG(CASE WHEN variant_object IS NOT NULL THEN 1.0 ELSE 0.0 END) as object_structure_ratio
FROM test_variants;

-- Test 20: Final comprehensive validation summary
SELECT 'Test 20: Comprehensive validation summary' as test_name,
       'SUMMARY' as category,
       (SELECT COUNT(*) FROM test_numbers WHERE int_max IS NOT NULL OR int_min IS NOT NULL) as numeric_test_rows,
       (SELECT COUNT(*) FROM test_variants WHERE variant_nested IS NOT NULL) as variant_test_rows,
       (SELECT COUNT(*) FROM test_booleans WHERE bool_true IS NOT NULL OR bool_false IS NOT NULL) as boolean_test_rows,
       (SELECT COUNT(*) FROM test_dates WHERE date_val IS NOT NULL OR timestamp_val IS NOT NULL) as datetime_test_rows,
       CASE 
         WHEN (SELECT COUNT(*) FROM test_numbers) >= 5 
          AND (SELECT COUNT(*) FROM test_variants) >= 3
          AND (SELECT COUNT(*) FROM test_booleans) >= 3
          AND (SELECT COUNT(*) FROM test_dates) >= 5
         THEN 'ALL_TESTS_POPULATED' 
         ELSE 'INCOMPLETE_TEST_DATA' 
       END as overall_status;
