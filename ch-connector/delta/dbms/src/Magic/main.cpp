#include <iostream>

#include <Interpreters/Context.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>

void dumpTable(const char *name)
{
    // class BlockOutputStreamPrintRows : IBlockOutputStream {...};
    //
    // string query = "SELECT * FROM ";
    // query += name;
    // auto context = Context::createGlobal(...)
    //
    // auto in = executeQuery(query, context, false);
    // auto out = BlockOutputStreamPrintRows(...);
    // copyData(in, out)
}

int main(int argc, char ** argv)
{
    if (argc <= 1)
        return 0;

    // NOTE: for developing, fully scan specified table.
    dumpTable(argv[1]);
    return 0;
}
