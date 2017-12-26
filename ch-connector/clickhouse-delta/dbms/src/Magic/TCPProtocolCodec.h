#pragma once

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>

namespace DB
{

inline Int64 readInt64(ReadBuffer & istr)
{
    Int64 x = 0;
    for (size_t i = 0; i < 8; ++i)
    {
        if (istr.eof())
            throwReadAfterEOF();

        UInt8 byte = *istr.position();
        ++istr.position();
        x |= (byte) << (8 * (7 - i));
    }
    return x;
}

inline void readString(std::string & x, ReadBuffer & istr)
{
    Int64 size = readInt64(istr);
    x.resize(size);
    istr.readStrict(&x[0], size);
}

// TODO: Use big-endian now, may be use little-endian is better
inline void writeInt64(Int64 x, WriteBuffer & ostr)
{
    UInt8 byte = 0;
    for (size_t i = 0; i < 8; ++i)
    {
        byte = (x >> (8 * (7 - i))) & 0xFF;
        ostr.write((const char*)&byte, 1);
    }
}

}
