#pragma once

#include <string>
#include <memory>

#include "data/block.h"

namespace moonshine {

using std::string;
using std::shared_ptr;

struct PersistLocation {
    string address;
    int fd;
    size_t offset;
    size_t size;

    PersistLocation(const string &address_, int fd_, size_t offset_, size_t size_) : address(address_), fd(fd_), offset(offset_), size(size_) {}

    virtual void Close() {
    }

    friend ostream & operator <<(ostream &w, const PersistLocation &x) {
        return w << x.address << " (fd:" << x.fd << ") from " << x.offset << " size " << x.size;
    }
};

using PersistLocationPtr = shared_ptr<PersistLocation>;

struct IColumnsLayout {
    virtual PersistLocationPtr PrepareColumnPartialWrite(const IColumn &column, const TypeAndName &info, size_t pos_in_block_stream, size_t rows, size_t cb) {
        throw ErrNoImpl("IColumnsLayout.PrepareColumnPartialWrite");
    }

    virtual void OnWriteDone() {}

    virtual void OnEncodedPKColumnFound(const IColumn &column, const TypeAndName &info) {}

    virtual PersistLocationPtr GetReadLocation(const TypeAndName &info, size_t pos_in_block_stream) {
        throw ErrNoImpl("IColumnsPersist.GetReadLocation");
    }

    virtual size_t GetBlockCount(const SortedSchema &schema) {
        throw ErrNoImpl("IColumnsPersist.GetBlockCount");
    }
};

using ColumnsLayoutPtr = shared_ptr<IColumnsLayout>;

struct ITablesLayout {
    virtual ColumnsLayoutPtr GetColumnsLayout(const string &table) {
        throw ErrNoImpl("ITablesLayout.GetColumnsLayout");
    }
};

using TablesLayoutPtr = shared_ptr<ITablesLayout>;

struct IBlockPersist {
    virtual void WriteBlock(const ColumnsLayoutPtr &layout, const SortedSchema &schema, size_t pos_in_block_stream, const Block &block) {
        throw ErrNoImpl("IBlockPersist.WriteBlock");
    }

    virtual void ReadBlock(const ColumnsLayoutPtr &layout, const SortedSchema &schema, size_t pos_in_block_stream, Block &block) {
        throw ErrNoImpl("IBlockPersist.ReadBlock");
    }
};

using BlockPersistPtr = shared_ptr<IBlockPersist>;

}
