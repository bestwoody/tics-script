#pragma once

#include <ostream>
#include <iomanip>

namespace moonshine {

using std::ostream;
using std::setbase;

struct Buf {
    const char *data;
    size_t size;

    Buf() : data(NULL), size(0) {}

    Buf(const char *data_, size_t size_) : data(data_), size(size_) {}

    operator bool() const {
        return data != NULL && size != 0;
    }

    friend ostream & operator <<(ostream &w, const Buf &buf) {
        return w << "@" << setbase(16) << size_t(buf.data) << setbase(10) << ", size: " << buf.size;
    }
};

}
