## add_data_types.zz
query:
	{ @nonaggregates = () ; $tables = 0 ; $fields = 0 ; "" } query_type ;


query_type:
        aggregate_select ;


aggregate_select:
        SELECT distinct  aggregate_select_list
        FROM new_table_item
        group_by_clause;

distinct: | | | DISTINCT;


aggregate_select_list:
        aggregate_select_item
        | aggregate_select_item
        | aggregate_select_item, aggregate_select_list
        | new_select_item, aggregate_select_item;


where_clause:
        | WHERE where_list ;


where_list:
	generic_where_list |
        range_predicate1_list | range_predicate2_list |
        range_predicate1_list and_or generic_where_list |
        range_predicate2_list and_or generic_where_list ;


generic_where_list:
        where_item |
        ( where_list and_or where_item ) ;

not:
	| | | NOT;

################################################################################
# The IS not NULL values in where_item are to hit the ref_or_null and          #
# the not_exists optimizations.  The LIKE '%a%' rule is to try to hit the      #
# rnd_pos optimization                                                         #
################################################################################
where_item:
        table1 .`pk` arithmetic_operator existing_table_item . _field  |
        table1 .`pk` arithmetic_operator existing_table_item . _field  |
	existing_table_item . _field arithmetic_operator value  |
        existing_table_item . _field arithmetic_operator existing_table_item . _field |
        existing_table_item . _field arithmetic_operator value  |
        existing_table_item . _field arithmetic_operator existing_table_item . _field |
        table1 .`pk` IS not NULL |
        table1 . _field IS not NULL |
        table1 . _field_indexed arithmetic_operator value AND ( table1 . char_field_name LIKE '%a%' OR table1.char_field_name LIKE '%b%') ;

################################################################################
# The range_predicate_1* rules below are in place to ensure we hit the         #
# index_merge/sort_union optimization.                                         #
# NOTE: combinations of the predicate_1 and predicate_2 rules tend to hit the  #
# index_merge/intersect optimization                                           #
################################################################################

range_predicate1_list:
      range_predicate1_item | 
      ( range_predicate1_item OR range_predicate1_list ) ;

range_predicate1_item:
         table1 . int_indexed not BETWEEN _tinyint_unsigned[invariant] AND ( _tinyint_unsigned[invariant] + _tinyint_unsigned ) |
         table1 . `col_varchar_key` arithmetic_operator _char[invariant]  |
         table1 . int_indexed not IN (number_list) |
         table1 . `col_varchar_key` not IN (char_list) |
         table1 . `pk` > _tinyint_unsigned[invariant] AND table1 . `pk` < ( _tinyint_unsigned[invariant] + _tinyint_unsigned ) |
         table1 . `col_int_key` > _tinyint_unsigned[invariant] AND table1 . `col_int_key` < ( _tinyint_unsigned[invariant] + _tinyint_unsigned ) ;

################################################################################
# The range_predicate_2* rules below are in place to ensure we hit the         #
# index_merge/union optimization.                                              #
# NOTE: combinations of the predicate_1 and predicate_2 rules tend to hit the  #
# index_merge/intersect optimization                                           #
################################################################################

range_predicate2_list:
      range_predicate2_item | 
      ( range_predicate2_item and_or range_predicate2_list ) ;

range_predicate2_item:
        table1 . `pk` = _tinyint_unsigned |
        table1 . `col_int_key` = _tinyint_unsigned |
        table1 . `col_varchar_key` = _char |
        table1 . int_indexed = _tinyint_unsigned |
        table1 . `col_varchar_key` = _char |
        table1 . int_indexed = existing_table_item . int_indexed |
        table1 . `col_varchar_key` = existing_table_item . `col_varchar_key` ;

################################################################################
# The number and char_list rules are for creating WHERE conditions that test   #
# 'field' IN (list_of_items)                                                   #
################################################################################
number_list:
        _tinyint_unsigned | number_list, _tinyint_unsigned ;

char_list: 
        _char | char_list, _char ;

################################################################################
# We ensure that a GROUP BY statement includes all nonaggregates.              #
# This helps to ensure the query is more useful in detecting real errors /     #
# that the query doesn't lend itself to variable result sets                   #
################################################################################
group_by_clause:
	{ scalar(@nonaggregates) > 0 ? " GROUP BY ".join (', ' , @nonaggregates ) : "" }  ;

optional_group_by:
        | | group_by_clause ;

having_clause:
	| HAVING having_list;

having_list:
        having_item |
        having_item |
	(having_list and_or having_item)  ;

having_item:
	existing_select_item arithmetic_operator value ;

################################################################################
# We mix digit and _digit here.  We want to alter the possible values of LIMIT #
# To ensure we hit varying EXPLAIN plans, but the OFFSET can be smaller        #
################################################################################

new_select_item:
	nonaggregate_select_item |
	nonaggregate_select_item |
        nonaggregate_select_item |        
        nonaggregate_select_item |
        nonaggregate_select_item |        
        nonaggregate_select_item |
        nonaggregate_select_item |        
        nonaggregate_select_item |
	aggregate_select_item  ;

################################################################################
# We have the perl code here to help us write more sensible queries            #
# It allows us to use field1...fieldn in the WHERE, ORDER BY, and GROUP BY     #
# clauses so that the queries will produce more stable and interesting results #
################################################################################

nonaggregate_select_item:
        table_one_two . _field_indexed AS { my $f = "field".++$fields ; push @nonaggregates , $f ; $f } |
        table_one_two . _field_indexed AS { my $f = "field".++$fields ; push @nonaggregates , $f ; $f } |
	table_one_two . _field AS { my $f = "field".++$fields ; push @nonaggregates , $f ; $f } ;

aggregate_select_item:
    group_concat_item
	| aggregate existing_table_item . _field ) AS { "field".++$fields }, group_concat_item
	| group_concat_item, num_aggregate existing_table_item . int_field_name)  AS { "field".++$fields };

group_concat_item:
	{ $concatNum = 0 ; "" } GROUP_CONCAT( distinct group_concat_concat_item group_concat_order_by_item  ) AS { "field".++$fields } ;

group_concat_concat_item:
    existing_table_item . _field { ++$concatNum;  "" } | existing_table_item . _field, existing_table_item . int_field_name { $concatNum = $concatNum + 2;  "" }| existing_table_item . _field, existing_table_item . int_field_name, existing_table_item . date_field_name  { $concatNum = $concatNum + 3;  "" };

group_concat_order_by_item:
  #  ORDER BY existing_table_item. _field , {$str=''; for( $a = 1; $a <$concatNum; $a = $a + 1 ){   $str = $str .$a .' , '; } $str = $str .$concatNum;}
  #  | ORDER BY existing_table_item. _field desc, {$str=''; for( $a = 1; $a <$concatNum; $a = $a + 1 ){   $str = $str .$a .' , '; } $str = $str .$concatNum;}
  #  |
    ORDER BY {$str=''; for( $a = 1; $a <$concatNum; $a = $a + 1 ){   $str = $str .$a .' , '; } $str = $str .$concatNum;};

################################################################################
# The combo_select_items are for 'spice' - we actually found                   #
################################################################################

combo_select_item:
    ( ( table_one_two . int_field_name ) math_operator ( table_one_two . int_field_name ) ) AS { my $f = "field".++$fields ; push @nonaggregates , $f ; $f } |
    CONCAT ( table_one_two . char_field_name , table_one_two . char_field_name ) AS { my $f = "field".++$fields ; push @nonaggregates , $f ; $f } ;

table_one_two:
	table1 ;

aggregate:
	COUNT( distinct |  MIN(  | MAX(  | COUNT(;

num_aggregate:
    	SUM(  | AVG(  | COUNT( distinct | COUNT(;

################################################################################
# The following rules are for writing more sensible queries - that we don't    #
# reference tables / fields that aren't present in the query and that we keep  #
# track of what we have added.  You shouldn't need to touch these ever         #
################################################################################
new_table_item:
	_table AS { "table".++$tables };

current_table_item:
	{ "table".$tables };

previous_table_item:
	{ "table".($tables - 1) };

existing_table_item:
	{ "table".$prng->int(1,$tables) };

existing_select_item:
	{ "field".$prng->int(1,$fields) };
################################################################################
# end of utility rules                                                         #
################################################################################

arithmetic_operator:
	= | > | < | != | <> | <= | >= ;

################################################################################
# We are trying to skew the ON condition for JOINs to be largely based on      #
# equalities, but to still allow other arithmetic operators                    #
################################################################################
join_condition_operator:
    arithmetic_operator | = | = | = ;

################################################################################
# Used for creating combo_items - ie (field1 + field2) AS fieldX               #
# We ignore division to prevent division by zero errors                        #
################################################################################
math_operator:
    + | - | * ;

################################################################################
# We stack AND to provide more interesting options for the optimizer           #
# Alter these percentages at your own risk / look for coverage regressions     #
# with --debug if you play with these.  Those optimizations that require an    #
# OR-only list in the WHERE clause are specifically stacked in another rule    #
################################################################################
and_or:
   AND | AND | OR ;

	
value:
	_digit | _digit | _digit | _digit | _tinyint_unsigned|
        _char(2) | _char(2) | _char(2) | _char(2) | _char(2) ;

_table:
     A | B | C | BB | CC | B | C | BB | CC | 
     C | C | C | C  | C  | C | C | C  | C  |
     CC | CC | CC | CC | CC | CC | CC | CC |
     D ;

################################################################################
# Add a possibility for 'view' to occur at the end of the previous '_table' rule
# to allow a chance to use views (when running the RQG with --views)
################################################################################

view:
    _A | _B | _C | _BB | _CC ;

#_field:
#    int_field_name | char_field_name ;

_digit:
    1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | _tinyint_unsigned ;

int_field_name:
        `col_tinyint` | `col_bigint`  | `col_decimal` | `col_decimal_30_10` | `col_decimal_30_10_not_null` | `col_decimal_not_null_key` | `col_decimal_key` | `col_decimal_40_not_null_key` | `col_decimal_40_key` | `col_decimal_40` | `pk` | `col_int_key` | `col_int` | `col_int_not_null` | `col_int_not_null_key` | `col_tinyint_not_null` | `col_bigint_not_null`;

num_field_name:
        int_field_name | `col_double` | `col_float` | `col_double_not_null` | `col_float_not_null`;

int_indexed:
   `pk` | `col_int_key` | `col_decimal_key` | `col_decimal_40_key` | `col_decimal_30_10_key`;

char_field_name:
  `col_varchar_64` | `col_char_64` | `col_varchar_64_not_null`| `col_char_64_not_null` ;

char_indexed:
  `col_varchar_10_key` | `col_varchar_64_key` ;

date_field_name:
        `col_date_key` | `col_date` | `col_datetime_key` | `col_datetime` | `col_time_key` | `col_time` | `col_year` | `col_year_key` | `col_date_not_null` | `col_datetime_not_null` | `col_time_not_null` | `col_year_not_null`;

################################################################################
# We define LIMIT_rows in this fashion as LIMIT values can differ depending on      #
# how large the LIMIT is - LIMIT 2 = LIMIT 9 != LIMIT 19                       #
################################################################################

limit_size:
    1 | 2 | 10 | 100 | 1000;
