#pragma once

#include <vector>
#include <memory>

#include "data/type.h"

namespace moonshine {

using std::vector;
using std::shared_ptr;

class Index {
public:
    size_t Size() const {
        return data.empty() ? 0 : data.size() / ElementSize();
    }

    Buf MinAt(size_t pos) const {
        return Buf(At(pos), type.data_size);
    }

    Buf MaxAt(size_t pos) const {
        return Buf(At(pos) + 1, type.data_size);
    }

    void AppendMinMax(const IColumn &column, const Type &type) {
        if (this->type.code == Type::Invalid)
            this->type = type;
        else if (this->type != type)
            throw ErrWrongUsage("call AppendMinMax with different column type");
        if (!type.is_fixed_size)
            throw ErrWrongUsage("call AppendMinMax with unfixed size data type column");
        if (column.Size() == 0)
            throw ErrWrongUsage("call AppendMinMax with empty column");
        data.resize(data.size() + ElementSize());
        memcpy((char *)&data[0] + data.size() - type.data_size * 2, column.Data(0, 1).data, type.data_size);
        memcpy((char *)&data[0] + data.size() - type.data_size, column.Data(column.Size() - 2, 1).data, type.data_size);
    };

    Buf Data(size_t first, size_t count) const {
        return Buf(At(first), count * ElementSize());
    }

    char* AssignPrepare(size_t bytes_count, const Type &type) {
        this->type = type;
        data.resize(bytes_count);
        return (char *)&data[0];
    }

private:
    const char * At(size_t pos) const {
        return (const char *)&data[0] + ElementSize() * pos;
    }

    size_t ElementSize() const {
        return type.data_size * 2;
    }

    Type type;
    vector<UInt8> data;
};

using IndexPtr = shared_ptr<Index>;

}
