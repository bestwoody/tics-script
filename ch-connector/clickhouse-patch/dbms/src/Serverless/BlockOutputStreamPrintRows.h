#pragma once

#include <iostream>

#include <Common/typeid_cast.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <DataStreams/dedupUtils.h>

namespace Magic
{

class BlockOutputStreamPrintRows : public DB::IBlockOutputStream
{
public:
    BlockOutputStreamPrintRows(std::ostream & writer_) : writer(writer_), block_index(0)
    {
    }

    void write(const DB::Block & block) override
    {
        DB::BlockPrinter::print(writer, block);
        block_index += 1;
    }

private:
    std::ostream & writer;
    size_t block_index;
};

}
