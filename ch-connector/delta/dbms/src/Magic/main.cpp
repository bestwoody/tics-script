#include <iostream>

void dumpTable(const char *name)
{
    // string query = "SELECT * FROM ";
    // query += name;
    // auto context = Context::createGlobal(...)
    // auto result = executeQuery(query, context, false);
    // for:
    //   auto block = result.in.read();
    //   (output)
}

int main(int argc, char ** argv)
{
    if (argc <= 1)
        return 0;

    // NOTE: for developing, fully scan specified table.
    dumpTable(argv[1]);
    return 0;
}
