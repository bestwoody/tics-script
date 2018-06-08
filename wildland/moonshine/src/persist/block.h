#pragma once

#include <memory>

#include "fs/fs.h"
#include "persist/persist.h"

namespace moonshine {

class BlockPersistSync : public IBlockPersist {
public:
    BlockPersistSync(const FSPtr &fs_) : fs(fs_) {}

    void WriteBlock(const ColumnsLayoutPtr &layout, const SortedSchema &schema, size_t pos_in_block_stream, const Block &block) override {
        for (size_t i = 0; i < schema.size(); ++i) {
            Buf buf = block[i]->Data(0, block.Rows());
            PersistLocationPtr loc = layout->PrepareColumnPartialWrite(*block[i], schema[i], pos_in_block_stream, block.Rows(), buf.size);
            fs->Write(loc->fd, buf.data, buf.size, loc->offset, loc->path);
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
            fs->Read(loc->fd, pdata, loc->size, loc->offset, loc->path);
            block[i]->AssignDone();
            loc->Close();
        }
        block.Resize(block[0]->Size());
    }

private:
    FSPtr fs;
};

}
