#include <iostream>

#include <Common/typeid_cast.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>

#include "Context.h"


namespace Magic
{

void queryDumpImpl(int argc, char ** argv, char * query)
{
    DB::Application app(argc, argv);
    auto result = DB::executeQuery(query, app.context(), false);
    BlockOutputStreamPrintRows out(std::cout);
    DB::copyData(*result.in, out);
}


int queryDump(int argc, char ** argv, char * query)
{
    try
    {
        queryDumpImpl(argc, argv, query);
    }
    catch (DB::Exception e)
    {
        std::cerr << DB::getCurrentExceptionMessage(true, true) << std::endl;
        return -1;
    }
    return 0;
}

}


int main(int argc, char ** argv)
{
    // TODO: handle args manually, not by BaseDaemon

    if (argc != 5 || std::string(argv[1]) != "--config-file" || std::string(argv[3]) != "--query") {
        std::cerr << "usage: <bin> --config-file <config-file> --query <query>" << std::endl;
        return -1;
    }

    return Magic::queryDump(argc - 2, argv, argv[4]);
}
