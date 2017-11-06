#include <iostream>

void dumpTable(const char *name)
{
    // Ref: TCPHandler
    //   Query: state.io = executeQuery(state.query, query_context, false, state.stage);
    //   Output: processOrdinaryQuery
}

int main(int argc, char ** argv)
{
    if (args_ <= 1)
        return 0;

    // NOTE: for developing, fully scan specified table.
    dumpTable(argv[1]);
    return 0;
}
