#pragma once

#include <string>

#include "base/err.h"
#include "data/block.h"

namespace moonshine {

using std::string;

class Delta {
public:
    Delta(string store_path_) : store_path(store_path_) {}

    void Load(Block &block) {
        // TODO: impl
        throw ErrNoImpl("Delta.Load");
    }

private:
    string store_path;
};

}
