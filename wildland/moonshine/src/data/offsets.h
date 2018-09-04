#pragma once

#include <vector>
#include <unordered_map>
#include <ostream>

#include "base/typedef.h"

namespace moonshine {

using std::vector;
using std::unordered_map;
using std::ostream;

struct PersistRange {
    PersistOffsetType offset;
    size_t size;

    friend ostream & operator <<(ostream &w, const PersistRange &x) {
        return w << "offset: " << x.offset << ", size: " << x.size;
    }
};

struct Offsets : public vector<PersistOffsetType> {
    Offsets() : vector<PersistOffsetType>(1, 0) {}

    size_t RangeCount() const {
        return size() - 1;
    }

    void AppendOffset(PersistOffsetType offset) {
        push_back(offset);
    }

    PersistOffsetType AppendSize(size_t size) {
        PersistOffsetType offset = back();
        push_back(offset + size);
        return offset;
    }

    PersistOffsetType OffsetAt(size_t pos) const {
        return (*this)[pos];
    }

    size_t SizeAt(size_t pos) const {
        return (*this)[pos] - (*this)[pos - 1];
    }

    PersistRange RangeAt(size_t pos) const {
        PersistOffsetType offset = (*this)[pos];
        return PersistRange{offset, (*this)[pos + 1] - offset};
    }
};

using OffsetsMap = unordered_map<string, Offsets>;

}
