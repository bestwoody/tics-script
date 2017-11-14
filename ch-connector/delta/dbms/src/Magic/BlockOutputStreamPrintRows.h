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
                auto name = data.type->getName();

                // TODO: support more types
                if (name == "String")
                {
                    const auto & column = typeid_cast<DB::ColumnString &>(*data.column);
                    print(i, j, column.getDataAt(i).toString());
                }
                else if (name == "Int8")
                {
                    const auto & column = typeid_cast<DB::ColumnInt8 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (name == "Int16")
                {
                    const auto & column = typeid_cast<DB::ColumnInt16 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (name == "Int32")
                {
                    const auto & column = typeid_cast<DB::ColumnInt32 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (name == "Int64")
                {
                    const auto & column = typeid_cast<DB::ColumnInt64 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (name == "UInt8")
                {
                    const auto & column = typeid_cast<DB::ColumnUInt8 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (name == "UInt16")
                {
                    const auto & column = typeid_cast<DB::ColumnUInt16 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (name == "UInt32")
                {
                    const auto & column = typeid_cast<DB::ColumnUInt32 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (name == "UInt64")
                {
                    const auto & column = typeid_cast<DB::ColumnUInt64 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (name == "Float32")
                {
                    const auto & column = typeid_cast<DB::ColumnFloat32 &>(*data.column);
                    print(i, j, column.getElement(i));
                }
                else if (name == "Float64")
                {
                    const auto & column = typeid_cast<DB::ColumnFloat64 &>(*data.column);
                    print(i, j, column.getElement(i));
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
