#pragma once

#include <memory>

#include <unistd.h>

#include "fs/fs.h"
#include "io/persist.h"

namespace moonshine {

struct BlockPersistSync : public IBlockPersist {
    void WriteBlock(const ColumnsLayoutPtr &layout, const SortedSchema &schema, size_t pos_in_block_stream, const Block &block) override {
        for (size_t i = 0; i < schema.size(); ++i) {
            Buf buf = block[i]->Data(0, block.Rows());
            PersistLocationPtr loc = layout->PrepareColumnPartialWrite(*block[i], schema[i], pos_in_block_stream, block.Rows(), buf.size);
            if (::pwrite(loc->fd, buf.data, buf.size, loc->offset) < 0)
                throw ErrFileWriteFailed(loc->address);
            loc->Close();
        }
        ColumnWithTypeAndName encoded_pk = block.GetEncodedPKColumn();
        if (encoded_pk.column)
            layout->OnEncodedPKColumnFound(*encoded_pk.column, encoded_pk.info);
    }

    void ReadBlock(const ColumnsLayoutPtr &layout, const SortedSchema &schema, size_t pos_in_block_stream, Block &block) override {
        block.CreateColumnsBySchema(schema);
        for (size_t i = 0; i < schema.size(); ++i) {
            PersistLocationPtr loc = layout->GetReadLocation(schema[i], pos_in_block_stream);
            char *pdata = block[i]->AssignBegin(loc->size);
            if (::pread(loc->fd, pdata, loc->size, loc->offset) < 0)
                throw ErrFileReadFailed(loc->address);
            block[i]->AssignDone();
            loc->Close();
        }
        block.Resize(block[0]->Size());
    }
};

}
