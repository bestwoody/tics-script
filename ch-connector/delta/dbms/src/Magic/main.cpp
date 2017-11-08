#include <iostream>

#include <Common/typeid_cast.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>

#include "Context.h"

namespace Magic {

// Not a effective impl, for test/dev only.
class BlockOutputStreamPrintRows : public DB::IBlockOutputStream
{
    void write(const DB::Block & block) override
    {
       for (size_t i = 0; i < block.rows(); i++)
        {
            for (size_t j = 0; j < block.columns(); j++)
            {
                DB::ColumnWithTypeAndName data = block.getByPosition(i);

                // TODO: support more types
                if (data.type->getName() == "Int64") {
                    auto column = typeid_cast<DB::ColumnInt64 *>(data.column.get());
                    std::cout << column->getElement(j) << "\t";
                }
            }
            std::cout << std::endl;
        }
    }
};

void _queryDumpImpl(std::string path, std::string query)
{
    DB::Application app(path);

    auto result = DB::executeQuery(query, app.context(), false);
    BlockOutputStreamPrintRows out;
    DB::copyData(*result.in, out);
}

void queryDumpImpl(std::string path, std::string query)
{
    auto context = DB::createContext(path);

    auto result = DB::executeQuery(query, *context, false);
    BlockOutputStreamPrintRows out;
    DB::copyData(*result.in, out);
}

int queryDump(const char * path, const char * query)
{
    try
    {
        queryDumpImpl(path, query);
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
    if (argc <= 2) {
        std::cerr << "usage: <bin> db-path query" << std::endl;
        return -1;
    }

    // NOTE: for developing, fully scan specified table.
    return Magic::queryDump(argv[1], argv[2]);
}
