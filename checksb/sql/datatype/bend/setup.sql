-- Databend-specific setup for extreme boundary testing
-- Focus on testing robustness with edge cases and extreme values

-- Create test tables for extreme boundary testing
CREATE OR REPLACE TABLE test_numbers (
    id INT,
    int_min INT,
    int_max INT,
    bigint_min BIGINT,
    bigint_max BIGINT,
    smallint_min SMALLINT,
    smallint_max SMALLINT,
    float_val FLOAT,
    double_val DOUBLE,
    decimal_val DECIMAL(10,2)
);

CREATE OR REPLACE TABLE test_variants (
    id INT,
    variant_null VARIANT,
    variant_string VARIANT,
    variant_number VARIANT,
    variant_boolean VARIANT,
    variant_array VARIANT,
    variant_object VARIANT,
    variant_nested VARIANT
);

CREATE OR REPLACE TABLE test_booleans (
    id INT,
    bool_true BOOLEAN,
    bool_false BOOLEAN,
    bool_null BOOLEAN
);

CREATE OR REPLACE TABLE test_dates (
    id INT,
    date_val DATE,
    timestamp_val TIMESTAMP
);

-- Test 1: Absolute maximum boundary values
INSERT INTO test_numbers VALUES (
    1,
    2147483647,              -- INT MAX
    2147483647,              -- INT MAX
    9223372036854775807,     -- BIGINT MAX  
    9223372036854775807,     -- BIGINT MAX
    32767,                   -- SMALLINT MAX
    32767,                   -- SMALLINT MAX
    3.4028235e+38,           -- FLOAT MAX
    1.7976931348623157e+308, -- DOUBLE MAX
    99999999.99              -- DECIMAL MAX
);

-- Test 2: Absolute minimum boundary values
INSERT INTO test_numbers VALUES (
    2,
    -2147483648,             -- INT MIN
    -2147483648,             -- INT MIN
    -9223372036854775808,    -- BIGINT MIN
    -9223372036854775808,    -- BIGINT MIN
    -32768,                  -- SMALLINT MIN
    -32768,                  -- SMALLINT MIN
    -3.4028235e+38,          -- FLOAT MIN
    -1.7976931348623157e+308, -- DOUBLE MIN
    -99999999.99             -- DECIMAL MIN
);

-- Test 3: Special floating point values and edge cases
INSERT INTO test_numbers VALUES (
    3,
    1,                       -- Edge case: 1
    -1,                      -- Edge case: -1
    1,                       -- Edge case: 1
    -1,                      -- Edge case: -1
    1,                       -- Edge case: 1
    -1,                      -- Edge case: -1
    1.175494e-38,            -- FLOAT MIN POSITIVE (subnormal)
    2.2250738585072014e-308, -- DOUBLE MIN POSITIVE (subnormal)
    0.01                     -- DECIMAL MIN PRECISION
);

-- Test 4: Zero and near-zero values
INSERT INTO test_numbers VALUES (
    4,
    0,                       -- Zero
    0,                       -- Zero
    0,                       -- Zero
    0,                       -- Zero
    0,                       -- Zero
    0,                       -- Zero
    0.0,                     -- Float zero
    0.0,                     -- Double zero
    0.00                     -- Decimal zero
);

-- Test 5: NULL values
INSERT INTO test_numbers VALUES (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


-- VARIANT Test 1: Extremely complex nested structures
INSERT INTO test_variants
SELECT 
    1,
    NULL,
    PARSE_JSON('{"unicode": "测试🌍🚀💻", "escape": "\\n\\t\\r\\\"\\\\", "long": "xxxxxxxxxxxxxxxxxxxx"}'),
    PARSE_JSON('{"max_int": 9223372036854775807, "min_int": -9223372036854775808, "float_max": 3.4028235e+38, "scientific": 1.23e-45}'),
    PARSE_JSON('{"true": true, "false": false, "null": null, "mixed": [true, false, null]}'),
    PARSE_JSON('[1, 2, 3, [4, 5, [6, 7, [8, 9, [10]]]], {"nested": [{"deep": [{"deeper": true}]}]}]'),
    PARSE_JSON('{"level1": {"level2": {"level3": {"level4": {"level5": {"data": "deep_nesting_test"}}}}}}'),
    PARSE_JSON('{"users": [{"id": 1, "profile": {"name": "Alice", "meta": {"tags": ["admin", "user"], "settings": {"theme": "dark", "notifications": {"email": true, "sms": false}}}}}, {"id": 2, "profile": {"name": "Bob", "meta": {"tags": [], "settings": null}}}], "config": {"version": "1.0", "features": {"feature1": true, "feature2": false}, "limits": {"max_users": 1000, "max_storage": 1073741824}}}');

-- VARIANT Test 2: Edge cases and special characters
INSERT INTO test_variants
SELECT
    2,
    NULL,
    PARSE_JSON('{"empty": "", "whitespace": "   ", "newlines": "line1\\nline2\\nline3", "tabs": "col1\\tcol2\\tcol3"}'),
    PARSE_JSON('{"zero": 0, "negative_zero": -0, "infinity": "Infinity", "negative_infinity": "-Infinity", "very_large": 1e308}'),
    PARSE_JSON('{"boolean_strings": ["true", "false", "TRUE", "FALSE", "True", "False"], "boolean_numbers": [1, 0, -1]}'),
    PARSE_JSON('[[], [[]], [[[]]], [null, null, null], ["", "", ""], [0, 0, 0]]'),
    PARSE_JSON('{"empty_object": {}, "null_values": {"a": null, "b": null}, "mixed_nulls": {"c": 1, "d": null, "e": "text"}}'),
    PARSE_JSON('{"special_chars": {"quotes": "\\\"double\\\" and \'single\'", "backslashes": "\\\\path\\\\to\\\\file", "unicode": "🔥💯⚡️🌟", "control": "\\u0000\\u0001\\u0002"}, "arrays_of_objects": [{"type": "A", "values": [1, 2, 3]}, {"type": "B", "values": [4, 5, 6]}, {"type": "C", "values": []}], "deeply_nested_arrays": [[[[[[["deep"]]]]]]] }');

-- VARIANT Test 3: Large data structures
INSERT INTO test_variants
SELECT
    3,
    NULL,
    PARSE_JSON('{"large_string": "AAAAAAAAAAAAAAAAAAAA", "repeated_pattern": "abc123abc123abc123"}'),
    PARSE_JSON('{"large_array_numbers": [999999999, 999999999, 999999999, 999999999, 999999999]}'),
    PARSE_JSON('{"boolean_array": [true, false, true, false, true]}'),
    PARSE_JSON('[{"id": 1, "data": "test"}, {"id": 2, "data": "test"}, {"id": 3, "data": "test"}, {"id": 21, "data": "final"}]'),
    PARSE_JSON('{"matrix": [[1, 2, 3, 4, 5], [1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]}'),
    PARSE_JSON('{"performance_test": {"large_object": {"key_1": "value_1", "key_2": "value_2", "final_key": "final_value"}, "large_nested": {"level1": {"level2": {"level3": {"data": [{"item": "test"}, {"item": "test"}, {"item": "last"}]}}}}}}');

-- Boolean extreme tests
INSERT INTO test_booleans VALUES (1, TRUE, FALSE, NULL);
INSERT INTO test_booleans VALUES (2, FALSE, TRUE, NULL);
INSERT INTO test_booleans VALUES (3, NULL, NULL, NULL);

-- Date/Time extreme boundary tests
INSERT INTO test_dates VALUES (
    1,
    '1900-01-01',            -- Early supported date
    '1900-01-01 00:00:00'    -- Early timestamp
);

INSERT INTO test_dates VALUES (
    2,
    '2099-12-31',            -- Far future date
    '2099-12-31 23:59:59'    -- Far future timestamp
);

INSERT INTO test_dates VALUES (
    3,
    '2000-02-29',            -- Leap year edge case
    '2000-02-29 23:59:59'    -- Leap year timestamp
);

INSERT INTO test_dates VALUES (
    4,
    '1970-01-01',            -- Unix epoch
    '1970-01-01 00:00:00'    -- Unix epoch timestamp
);

INSERT INTO test_dates VALUES (5, NULL, NULL);
