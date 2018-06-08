#pragma once

#include <string>
#include <memory>

#include "base/args.h"
#include "data/stream.h"

namespace moonshine {

using std::string;
using std::shared_ptr;

class IStorage;

class ITable {
public:
    ITable(IStorage *storage_, const SortedSchema &schema_) : storage(storage_), schema(schema_) {}

    IStorage * GetStorage() const {
        return storage;
    }

    const SortedSchema & GetSchema() const {
        return schema;
    }

    virtual void Write(IBlocksInput &blocks) {
        throw ErrNoImpl("ITable.Write");
    }

    virtual BlocksInputPtr Scan() {
        throw ErrNoImpl("ITable.Scan");
    }

private:
    IStorage *storage;
    const SortedSchema schema;
};

using TablePtr = shared_ptr<ITable>;

struct IStorage {
    virtual string GetName() {
        throw ErrNoImpl("IStorage.GetName");
    }

    virtual ExpectedArgs GetExpectedArgs() {
        throw ErrNoImpl("IStorage.GetExpectedArgs");
    }

    virtual TablePtr CreateTable(
        const string &name,
        const SortedSchema &schema,
        const Args &args) {

        throw ErrNoImpl("IStorage.CreateTable");
    }
};

using StoragePtr = shared_ptr<IStorage>;

}
