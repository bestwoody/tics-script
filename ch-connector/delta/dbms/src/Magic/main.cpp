#include <iostream>

#include <Common/typeid_cast.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>

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
