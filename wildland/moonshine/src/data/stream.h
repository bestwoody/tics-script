#pragma once

#include <memory>

#include "data/block.h"

namespace moonshine {

using std::shared_ptr;

struct IBlocksInput {
    virtual const Block Read() {
        throw ErrNoImpl("Blocks.Read");
    }

    virtual bool Done() {
        throw ErrNoImpl("Blocks.Done");
    }
};

using BlocksInputPtr = shared_ptr<IBlocksInput>;

}
