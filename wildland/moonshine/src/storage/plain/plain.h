#pragma once

#include <string>
#include <ostream>
#include <memory>

#include "data/stream.h"
#include "persist/block.h"
#include "persist/layout.h"
#include "storage/storage.h"

namespace moonshine {

using std::string;
using std::cout;
using std::make_shared;

class BlocksInputPlain : public IBlocksInput {
public:
    BlocksInputPlain(ColumnsLayoutPtr layout_, BlockPersistPtr block_persist_, const SortedSchema & schema_) :
        layout(layout_), block_persist(block_persist_), schema(schema_), current(0) {
        total = layout->GetBlockCount(schema);
    }

    const Block Read() override {
        Block block;
        block_persist->ReadBlock(layout, schema, current, block);
        current += 1;
        return block;
    }

    bool Done() override {
        return current >= total;
    }

private:
    ColumnsLayoutPtr layout;
    BlockPersistPtr block_persist;
    const SortedSchema & schema;
    size_t current;
    size_t total;
};

class TablePlain : public ITable {
public:
    TablePlain(IStorage *storage_, const ColumnsLayoutPtr &layout_,
        const BlockPersistPtr &block_persist_, const class SortedSchema &schema_, bool sort_, bool print_) :
        ITable(storage_, schema_), layout(layout_), block_persist(block_persist_), sort(sort_), print(print_) {}

    void Write(IBlocksInput &blocks) override {
        const SortedSchema &schema = GetSchema();
        size_t i = 0;
        while (!blocks.Done()) {
            Block block = blocks.Read();
            if (sort) {
                block.GenEncodedPKColumn(schema);
                block.SortRowsByPK();
            }
            if (print)
                block.DebugPrintByColumn(cout, schema);
            block_persist->WriteBlock(layout, schema, i++, block);
        }
        layout->OnWriteDone();
    }

    BlocksInputPtr Scan() override {
        return make_shared<BlocksInputPlain>(layout, block_persist, GetSchema());
    }

private:
    ColumnsLayoutPtr layout;
    BlockPersistPtr block_persist;
    const bool sort;
    const bool print;
};

struct StoragePlain : public IStorage {
    StoragePlain(const TablesLayoutPtr &layout_, const BlockPersistPtr &block_persist_) :
        layout(layout_), block_persist(block_persist_) {}

    string GetName() override {
        return "plain";
    }

    ExpectedArgs GetExpectedArgs() override {
        ExpectedArgs args;
        args.AddExpect(false, "sort", "bool", "sort block data or not");
        args.AddExpect(true, "print", "bool", "print block or not, for debug only");
        return args;
    }

    TablePtr CreateTable(
        const string &name,
        const SortedSchema &schema,
        const Args &args) override {

        ColumnsLayoutPtr columns_layout = layout->GetColumnsLayout(name);
        return make_shared<TablePlain>(this, columns_layout, block_persist, schema,
            args.Get<bool>("sort"), args.Get<bool>("print", false));
    }

private:
    TablesLayoutPtr layout;
    BlockPersistPtr block_persist;
};

}
