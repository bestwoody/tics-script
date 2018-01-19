#pragma once

#include <iostream>

#include <Common/typeid_cast.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>

namespace DB
{

// Not a effective implement, for test/dev only.
// TODO: Support all types
class BlockOutputStreamPrintRows : public IBlockOutputStream
{
public:
    BlockOutputStreamPrintRows(std::ostream & writer_) : writer(writer_), block_index(0)
    {
    }

    void write(const Block & block) override
    {
        for (size_t i = 0; i < block.rows(); i++)
        {
            for (size_t j = 0; j < block.columns(); j++)
            {
                ColumnWithTypeAndName data = block.getByPosition(j);
                auto name = data.type->getName();

                if (name == "String")
                {
                    const auto & column = typeid_cast<ColumnString &>(*data.column);
                    print(i, j, column.getDataAt(i).toString());
                }
                else if (name == "DateTime")
                {
                    const auto & column = typeid_cast<ColumnUInt32 &>(*data.column);
                    print(i, j, (int64_t)column.getElement(i));
                }
                else if (name == "Int8")
                {
                    const auto & column = typeid_cast<ColumnInt8 &>(*data.column);
                    print(i, j, (int64_t)column.getElement(i));
                }
                else if (name == "Int16")
                {
                    const auto & column = typeid_cast<ColumnInt16 &>(*data.column);
                    print(i, j, (int64_t)column.getElement(i));
                }
                else if (name == "Int32")
                {
                    const auto & column = typeid_cast<ColumnInt32 &>(*data.column);
                    print(i, j, (int64_t)column.getElement(i));
                }
                else if (name == "Int64")
                {
                    const auto & column = typeid_cast<ColumnInt64 &>(*data.column);
                    print(i, j, (int64_t)column.getElement(i));
                }
                else if (name == "UInt8")
                {
                    const auto & column = typeid_cast<ColumnUInt8 &>(*data.column);
                    print(i, j, (int64_t)column.getElement(i));
                }
                else if (name == "UInt16")
                {
                    const auto & column = typeid_cast<ColumnUInt16 &>(*data.column);
                    print(i, j, (int64_t)column.getElement(i));
                }
                else if (name == "UInt32")
                {
                    const auto & column = typeid_cast<ColumnUInt32 &>(*data.column);
                    print(i, j, (int64_t)column.getElement(i));
                }
                else if (name == "UInt64")
                {
                    const auto & column = typeid_cast<ColumnUInt64 &>(*data.column);
                    print(i, j, (uint64_t)column.getElement(i));
                }
                else if (name == "Float32")
                {
                    const auto & column = typeid_cast<ColumnFloat32 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (name == "Float64")
                {
                    const auto & column = typeid_cast<ColumnFloat64 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else
                {
                    throw Exception(std::string("unknown type name: ") + name);
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
