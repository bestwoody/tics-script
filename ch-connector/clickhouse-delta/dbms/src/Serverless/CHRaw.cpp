#include <iostream>

#include <DataStreams/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>

#include "Context.h"

namespace Magic
{

void queryDumpImpl(const char * config, char * query)
{
    DB::Application app(config);
    auto result = DB::executeQuery(query, app.context(), false);
    DB::BlockOutputStreamPrintRows out(std::cout);
    DB::copyData(*result.in, out);
}

int queryDump(const char * config, char * query)
{
    try
    {
        queryDumpImpl(config, query);
    }
    catch (DB::Exception e)
    {
        std::cerr << DB::getCurrentExceptionMessage(true, true) << std::endl;
        return -1;
    }
    return 0;
}

}

// TODO: Handle args manually, not by BaseDaemon
int main(int argc, char ** argv)
{
    if (argc != 3) {
        std::cerr << "usage: <bin> config-file query-string" << std::endl;
        return -1;
    }
    return Magic::queryDump(argv[1], argv[2]);
}
