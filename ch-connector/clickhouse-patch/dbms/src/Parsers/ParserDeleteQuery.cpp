#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserDeleteQuery.h>

#include <Common/typeid_cast.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserDeleteQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserWhitespaceOrComments ws;
    ParserKeyword s_delete_from("DELETE FROM");
    ParserKeyword s_dot(".");
    ParserKeyword s_where("WHERE");
    ParserIdentifier name_p;

    ASTPtr database;
    ASTPtr table;
    ASTPtr where;

    ParserExpressionWithOptionalAlias exp_elem(false);

    ws.ignore(pos, end);

    if (!s_delete_from.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (!name_p.parse(pos, end, table, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (s_dot.ignore(pos, end, max_parsed_pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, end, table, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    ws.ignore(pos, end);

    if (!s_where.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (!exp_elem.parse(pos, end, where, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    std::shared_ptr<ASTDeleteQuery> query = std::make_shared<ASTDeleteQuery>(StringRange(begin, pos));
    node = query;

    if (database)
        query->database = typeid_cast<ASTIdentifier &>(*database).name;

    query->table = typeid_cast<ASTIdentifier &>(*table).name;

    if (where)
    {
        query->where = where;
        query->children.push_back(where);

        std::shared_ptr<ASTSelectQuery> select_query = std::make_shared<ASTSelectQuery>();

        std::shared_ptr<ASTAsterisk> asterisk = std::make_shared<ASTAsterisk>();
        std::shared_ptr<ASTExpressionList> table_columns = std::make_shared<ASTExpressionList>();
        table_columns->children.push_back(asterisk);

        select_query->select_expression_list = table_columns;
        select_query->children.push_back(table_columns);

        auto table_expr = std::make_shared<ASTTableExpression>();
        table_expr->database_and_table_name =
            std::make_shared<ASTIdentifier>(StringRange(),
            query->database.size() ? query->database + '.' + query->table : query->table,
            ASTIdentifier::Table);

        auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
        table_element->table_expression = table_expr;
        table_element->children.emplace_back(table_expr);

        auto table_list = std::make_shared<ASTTablesInSelectQuery>();
        table_list->children.emplace_back(table_element);

        select_query->tables = table_list;
        select_query->children.push_back(table_list);

        select_query->where_expression = query->where;
        select_query->children.push_back(query->where);

        query->select = select_query;
        query->children.push_back(select_query);
    }

    return true;
}


}
