query:
    full_join | semi_join ;

semi_join:
    SELECT L.field_name FROM _table AS L WHERE semi_condition ;

semi_condition:
    L . integer_field_name semi_type ( SELECT R . integer_field_name FROM _table AS R )
    ;

semi_type:
    IN | NOT IN ;

full_join:
    SELECT L.field_name FROM _table AS L join_type _table AS R ON ( join_condition ) ;

field_name:
    integer_field_name | varchar_field_name | decimal_field_name | datetime_field_name ;

decimal_field_name:
    `col_decimal` | `col_decimal_not_null` | `col_decimal_not_null_key` | `col_decimal_key` 
    | `col_decimal_35` | `col_decimal_35_not_null` | `col_decimal_35_not_null_key` | `col_decimal_35_key`
    | `col_decimal_5_2` | `col_decimal_5_2_not_null` | `col_decimal_5_2_not_null_key` | `col_decimal_5_2_key`;

datetime_field_name:
    `col_datetime` | `col_datetime_key` | `col_datetime_not_null` | `col_datetime_not_null_key` ;

integer_field_name:
    `col_int` | `col_int_not_null` | `col_int_not_null_key` | `col_int_key` 
    | `col_tinyint` | `col_tinyint_not_null` | `col_tinyint_not_null_key` | `col_tinyint_key` ;

varchar_field_name:
    `col_varchar_binary_not_null` | `col_varchar_binary_not_null_key` | `col_varchar_binary` | `col_varchar_binary_key`
    | `col_varchar_8_binary_not_null` | `col_varchar_8_binary_not_null_key` | `col_varchar_8_binary` | `col_varchar_8_binary_key` ;

join_type:
    INNER JOIN | LEFT JOIN | RIGHT JOIN ;

join_condition:
    equal_condition | not_equal_condition ;

not_equal_condition:
    integer_not_equal_condition;

equal_condition:
    integer_equal_condition;

integer_not_equal_condition:
    L . integer_field_name > R . integer_field_name
    | L . integer_field_name < R . integer_field_name
    | L . integer_field_name <> R . integer_field_name ;

varchar_not_equal_condition:
    L . varchar_field_name > R . varchar_field_name
    | L . varchar_field_name < R . varchar_field_name
    | L . varchar_field_name <> R . varchar_field_name ;

decimal_not_equal_condition:
    L . decimal_field_name > R . decimal_field_name
    | L . decimal_field_name < R . decimal_field_name
    | L . decimal_field_name <> R . decimal_field_name ;

datetime_not_equal_condition:
    L . datetime_field_name > R . datetime_field_name
    | L . datetime_field_name < R . datetime_field_name
    | L . datetime_field_name <> R . datetime_field_name ;

integer_equal_condition:
    L . integer_field_name = R . integer_field_name ;

varchar_equal_condition:
    L . varchar_field_name = R . varchar_field_name ;

decimal_equal_condition:
    L . decimal_field_name = R . decimal_field_name ;

datetime_equal_condition:
    L . datetime_field_name = R . datetime_field_name ;

mixed_equal_condition:
    integer_equal_condition AND integer_equal_condition
    | varchar_equal_condition AND varchar_equal_condition
    | decimal_equal_condition AND decimal_equal_condition
    | datetime_equal_condition AND datetime_equal_condition
    | integer_equal_condition AND varchar_equal_condition
    | integer_equal_condition AND decimal_equal_condition
    | integer_equal_condition AND datetime_equal_condition
    | varchar_equal_condition AND decimal_equal_condition
    | varchar_equal_condition AND datetime_equal_condition
    | decimal_equal_condition AND datetime_equal_condition
    ;
