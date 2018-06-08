#pragma once

#include <string>
#include <ostream>

#include "storage/storage.h"

namespace moonshine {

using std::string;
using std::cout;

class TableNull : public ITable {
public:
    TableNull(IStorage *storage_, const SortedSchema &schema_, bool sort_, bool print_)
        : ITable(storage_, schema_), sort(sort_), print(print_) {}

    void Write(IBlocksInput &blocks) override {
        while (!blocks.Done()) {
            Block block = blocks.Read();
            if (sort) {
                block.GenEncodedPKColumn(GetSchema());
                block.SortRowsByPK();
            }
            if (print)
                block.DebugPrintByColumn(cout, GetSchema());
        }
    }

    BlocksInputPtr Scan() override {
        throw ErrNoImpl("TableNull.Scan");
    }

private:
    const bool sort;
    const bool print;
};

struct StorageNull : public IStorage {
    string GetName() override {
        return "null";
    }

    ExpectedArgs GetExpectedArgs() override {
        ExpectedArgs args;
        args.AddExpect(false, "sort", "bool", "sort block data or not");
        args.AddExpect(true, "print", "bool", "default: true. print data to stdout or not");
        return args;
    }

    TablePtr CreateTable(
        const string &name,
        const SortedSchema &schema,
        const Args &args) override {

        return make_shared<TableNull>(this, schema, args.Get<bool>("sort"), args.Get<bool>("print", true));
    }
};

}
