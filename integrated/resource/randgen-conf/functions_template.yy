
################################################################################
# functions_template.yy:  Random Query Generator grammar for testing functions #
#                         with simple form, i.e., those only use functions in  #
#                         select list and no 'where', 'group by', 'having',    #
#                         'join' or 'order by' clause, also there should be no #
#                         nested functions.                                    #
#                                                                              #
# NOTE:                                                                        #
#       Following grammar only serves as a template for writing function       #
#       tests. For more details, please refer to                               #
#       https://github.com/pingcap/tiflash-scripts/issues/1878.                #
#                                                                              #
################################################################################

query: select
;

# we can omit $i since output column is fixed
# but for better evolvability, we just leave it here.
select: { $i = 0; '' } SELECT select_list FROM _table ORDER BY { join(', ', map { "field".$_ } (1..$i)) }
;

# for simplicity, each test can only have one output column
select_list:
    select_item AS { $i++; 'field'.$i }
;

select_item:
    func
;

func:
    arith_func |
    str_func |
    date_func |
    cast_func
;

arith_func:
    arg >> arg |
    arg << arg
;

str_func:
   ELT( arg_list ) |
   HEX( _field ) |
   REPEAT( arg, tinyint_field_name ) |
   LENGTH( REPEAT( arg, int_field_name ) ) |
   REVERSE( _field )
;

date_func:
   GET_FORMAT( get_format_type, get_format_format ) |
   TIME_TO_SEC( _field )
;

cast_func:
   CAST( _field AS TIME )
;

arg_list:
    arg, arg |
    arg, arg |
    arg, arg, arg
;

arg:
    _field | _field | _field | _field | value
;

tinyint_field_name:
    COL_TINYINT | COL_TINYINT_NOT_NULL | COL_TINYINT_KEY | COL_TINYINT_NOT_NULL_KEY
;

int_field_name:
    COL_TINYINT | COL_TINYINT_NOT_NULL | COL_TINYINT_KEY | COL_TINYINT_NOT_NULL_KEY |
    COL_SMALLINT | COL_SMALLINT_NOT_NULL | COL_SMALLINT_KEY | COL_SMALLINT_NOT_NULL_KEY
;

value:
   _bigint | _smallint | _int_usigned |
   _char(1) | _char(10) | _datetime | _date | _time | NULL
;

get_format_type:
   DATE | TIME | DATETIME
;

get_format_format:
   'EUR' | 'USA' | 'JIS' | 'ISO' | 'INTERNAL' | arg
;
