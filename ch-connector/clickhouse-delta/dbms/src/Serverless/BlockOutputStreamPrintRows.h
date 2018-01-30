#pragma once

#include <iostream>

#include <Common/typeid_cast.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>

namespace Magic
{

// Not a effective implement, for test/dev only.
// TODO: Support all types
class BlockOutputStreamPrintRows : public DB::IBlockOutputStream
{
public:
    BlockOutputStreamPrintRows(std::ostream & writer_) : writer(writer_), block_index(0)
    {
    }

    void write(const DB::Block & block) override
    {
        for (size_t i = 0; i < block.rows(); i++)
        {
            for (size_t j = 0; j < block.columns(); j++)
            {
                DB::ColumnWithTypeAndName data = block.getByPosition(j);
                auto name = data.type->getName();

                if (name == "String")
                {
                    const auto column = typeid_cast<const DB::ColumnString *>(data.column.get());
                    print(i, j, column->getDataAt(i).toString());
                }
                else if (name == "DateTime")
                {
                    const auto column = typeid_cast<const DB::ColumnUInt32 *>(data.column.get());
                    print(i, j, (int64_t)column->getElement(i));
                }
                else if (name == "Int8")
                {
                    const auto column = typeid_cast<const DB::ColumnInt8 *>(data.column.get());
                    print(i, j, (int64_t)column->getElement(i));
                }
                else if (name == "Int16")
                {
                    const auto column = typeid_cast<const DB::ColumnInt16 *>(data.column.get());
                    print(i, j, (int64_t)column->getElement(i));
                }
                else if (name == "Int32")
                {
                    const auto column = typeid_cast<const DB::ColumnInt32 *>(data.column.get());
                    print(i, j, (int64_t)column->getElement(i));
                }
                else if (name == "Int64")
                {
                    const auto column = typeid_cast<const DB::ColumnInt64 *>(data.column.get());
                    print(i, j, (int64_t)column->getElement(i));
                }
                else if (name == "UInt8")
                {
                    const auto column = typeid_cast<const DB::ColumnUInt8 *>(data.column.get());
                    print(i, j, (int64_t)column->getElement(i));
                }
                else if (name == "UInt16")
                {
                    const auto column = typeid_cast<const DB::ColumnUInt16 *>(data.column.get());
                    print(i, j, (int64_t)column->getElement(i));
                }
                else if (name == "UInt32")
                {
                    const auto column = typeid_cast<const DB::ColumnUInt32 *>(data.column.get());
                    print(i, j, (int64_t)column->getElement(i));
                }
                else if (name == "UInt64")
                {
                    const auto column = typeid_cast<const DB::ColumnUInt64 *>(data.column.get());
                    print(i, j, (uint64_t)column->getElement(i));
                }
                else if (name == "Float32")
                {
                    const auto column = typeid_cast<const DB::ColumnFloat32 *>(data.column.get());
                    print(i, j, column->getElement(i));
                }
                else if (name == "Float64")
                {
                    const auto column = typeid_cast<const DB::ColumnFloat64 *>(data.column.get());
                    print(i, j, column->getElement(i));
                }
                else
                {
                    throw DB::Exception("Unknown type name: " + name);
                }
            }
            writer << std::endl;
        }

        block_index += 1;
    }

private:
    template <typename T>
    std::ostream & print(size_t row, size_t col, const T v, bool detail = false)
    {
        if (detail)
            return writer << "(" << block_index << ", "<< row << ", " << col << ")" << v << "\t";
        else
            return writer << v << "\t";
    }

private:
    std::ostream & writer;
    size_t block_index;
};

}
