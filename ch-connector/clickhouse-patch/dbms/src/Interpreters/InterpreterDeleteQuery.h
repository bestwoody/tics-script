#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>


namespace DB
{


/** Interprets the DELETE query.
  */
class InterpreterDeleteQuery : public IInterpreter
{
public:
    InterpreterDeleteQuery(const ASTPtr & query_ptr_, const Context & context_, bool allow_materialized_ = false);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context context;
    bool allow_materialized;
};


}
