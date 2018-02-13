#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


/** Cases:
  *
  * DELETE FROM [db.]table WHERE ...
  */
class ParserDeleteQuery : public IParserBase
{
protected:
    const char * getName() const { return "DELETE query"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
