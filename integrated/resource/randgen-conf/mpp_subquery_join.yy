# Copyright (C) 2009 Sun Microsystems, Inc. All rights reserved.
# Use is subject to license terms.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301
# USA

query: select;

select: 
	SELECT outer_select_item
	FROM outer_from_with_join
	WHERE subquery_expression 
	outer_group_by outer_having;

outer_select_item:
	OUTR . field_name AS X |
	aggregate_function OUTR . field_name ) AS X;

aggregate_function:
	AVG( |
	COUNT( |
	MIN( | 
	MAX( | 
	SUM( ; 

outer_from:
	outer_table_name AS OUTR ;

outer_from_with_join:
	outer_table_name AS OUTR2 left_right JOIN outer_table_name AS OUTR ON ( outer_join_eq_condition AND outer_join_condition );

outer_join_eq_condition:
	OUTR2 . int_field_name = OUTR . int_field_name |
	OUTR2 . date_field_name = OUTR . date_field_name |
	OUTR2 . decimal_field_name = OUTR . decimal_field_name |
	OUTR2 . char_field_name = OUTR . char_field_name ;

outer_join_condition:
	OUTR2 . int_field_name arithmetic_operator OUTR . int_field_name |
	OUTR2 . date_field_name arithmetic_operator OUTR . date_field_name |
	OUTR2 . decimal_field_name arithmetic_operator OUTR . decimal_field_name |
	OUTR2 . char_field_name arithmetic_operator OUTR . char_field_name ;

outer_order_by:
	ORDER BY OUTR . field_name , OUTR . `pk` ;

outer_group_by:
	| GROUP BY OUTR . field_name ;

outer_having:
	| HAVING X arithmetic_operator value ;

limit:
	| LIMIT digit ;

select_inner_body:
	FROM inner_from
	WHERE inner_condition_top
	inner_order_by;

select_inner:
	SELECT inner_select_item
	select_inner_body;

select_inner_one_row:
	SELECT inner_select_item
	select_inner_body LIMIT 1;

select_inner_two_cols:
	SELECT inner_select_item , INNR . field_name AS Z 
	select_inner_body;

inner_order_by:
	| ORDER BY INNR . field_name ;

inner_group_by:
	| | | ;
inner_group_by_disabled:
	| GROUP BY INNR . field_name |
	GROUP BY INNR . field_name WITH ROLLUP;

inner_having:
	| | | HAVING X arithmetic_operator value;

inner_select_item:
	INNR . field_name AS Y |
    INNR . int_field_name + 1 AS Y ;

inner_select_item_disabled_causes_semijoin_not_to_kick_in;
	aggregate_function INNR . field_name ) AS Y ;

inner_from:
	inner_table_name AS INNR ;

inner_from_disabled_unnecessary_complication:
	inner_table_name AS INNR2 left_right JOIN inner_table_name AS INNR ON ( inner_join_condition );

inner_join_condition:
	INNR2 . int_field_name arithmetic_operator INNR . int_field_name |
	INNR2 . date_field_name arithmetic_operator INNR . date_field_name |
	INNR2 . decimal_field_name arithmetic_operator INNR . decimal_field_name |
	INNR2 . char_field_name arithmetic_operator INNR . char_field_name ;

outer_condition_top:
	outer_condition_bottom |
	( outer_condition_bottom logical_operator outer_condition_bottom ) |
	outer_condition_bottom logical_operator outer_condition_bottom ;

outer_condition_bottom:
	OUTR . expression ;

expression:
	field_name null_operator |
	int_field_name int_expression |
	date_field_name date_expression |
	char_field_name char_expression ;

int_expression:
	arithmetic_operator digit ;

date_expression:
	arithmetic_operator date | BETWEEN date AND date;

char_expression:
	arithmetic_operator _varchar(1);

inner_condition_top:
	INNR . expression |
	inner_condition_bottom logical_operator inner_condition_bottom ;

inner_condition_bottom:
	INNR . expression |
	INNR . int_field_name arithmetic_operator INNR . int_field_name |
	INNR . date_field_name arithmetic_operator INNR . date_field_name |
	INNR . decimal_field_name arithmetic_operator INNR . decimal_field_name |
	INNR . char_field_name arithmetic_operator INNR . char_field_name;

null_operator: IS NULL | IS NOT NULL ;

logical_operator: AND | OR | OR NOT;

logical_operator_disabled_bug37899: XOR;

logical_operator_disabled_bug37896: AND NOT ;

arithmetic_operator: = | > | < | <> | >= | <= ;

subquery_expression:
	OUTR . field_name IN ( select_inner ) |
	OUTR . field_name NOT IN ( select_inner ) ;

subquery_expression_disabled_crash_in_item_subselect:
	value IN ( select_inner );

subquery_expression_disabled_bug37894:
	value NOT IN ( select_inner );

subquery_word: SOME | ANY | ALL ;

field_name:
	int_field_name | char_field_name | date_field_name | decimal_field_name;

int_field_name:
        `pk` | `col_int` | `col_int_not_null` ;

date_field_name:
        `col_datetime` | `col_datetime_not_null` ;

char_field_name:
        `col_varchar_10` | `col_varchar_10_not_null` ;

decimal_field_name:
        `col_decimal` | `col_decimal_not_null` ;

outer_table_name:
	A | B | C | D;

inner_table_name:
	AA | BB | CC | DD;

value: _digit | _date | _time | _datetime | _varchar(1) | NULL ;

left_right:
	LEFT | INNER | RIGHT ;

select_option_disabled_triggers_materialization_assert:
	| DISTINCT ;
