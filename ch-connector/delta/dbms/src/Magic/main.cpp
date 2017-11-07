#include <iostream>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>

namespace Magic {

class BlockOutputStreamPrintRows : public DB::IBlockOutputStream
{
    void write(const DB::Block & block) override
    {
    }
};

void dumpTable(const char *name)
{
    std::string query = "SELECT * FROM ";
    query += name;

    auto context = DB::Context::createGlobal();
    auto result = DB::executeQuery(query, context, false);
    BlockOutputStreamPrintRows out;
    DB::copyData(*result.in, out);
}

}

int main(int argc, char ** argv)
{
    if (argc <= 1)
        return 0;

    // NOTE: for developing, fully scan specified table.
    Magic::dumpTable(argv[1]);
    return 0;
}
