#pragma once

#include <iostream>

#include <Common/typeid_cast.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>

// Not a effective impl, for test/dev only.
class BlockOutputStreamPrintRows : public DB::IBlockOutputStream
{
public:
    BlockOutputStreamPrintRows(std::ostream & writer_) : writer(writer_), block_index(0) {}

    void write(const DB::Block & block) override
    {
        for (size_t i = 0; i < block.rows(); i++)
        {
            for (size_t j = 0; j < block.columns(); j++)
            {
                DB::ColumnWithTypeAndName data = block.getByPosition(j);

                // TODO: support more types
                if (data.type->getName() == "Int64")
                {
                    const auto & column = typeid_cast<DB::ColumnInt64 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (data.type->getName() == "String")
                {
                    const auto & column = typeid_cast<DB::ColumnString &>(*data.column);
                    print(i, j, column.getDataAt(i).toString());
                }
            }
            writer << std::endl;
        }

        block_index += 1;
    }

private:
    template <typename T>
    std::ostream & print(size_t row, size_t col, const T v)
    {
        //return writer << "(" << block_index << ", "<< row << ", " << col << ")" << v << "\t";
        return writer << v << "\t";
    }

private:
    std::ostream & writer;
    size_t block_index;
};
