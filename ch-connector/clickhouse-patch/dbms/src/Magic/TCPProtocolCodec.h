#pragma once

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>

namespace Magic
{

namespace Protocol
{
    enum
    {
        Header = 0,
        End = 1,
        Utf8Error = 8,
        Utf8Query = 9,
        ArrowSchema = 10,
        ArrowData = 11,
    };
}

// TODO: Use big-endian for now, may be use little-endian is better
inline Int64 readInt64(DB::ReadBuffer & istr)
{
    Int64 x = 0;
    for (size_t i = 0; i < 8; ++i)
    {
        if (istr.eof())
            DB::throwReadAfterEOF();

        UInt8 byte = *istr.position();
        ++istr.position();
        x |= (byte) << (8 * (7 - i));
    }
    return x;
}

// TODO: Use big-endian for now, may be use little-endian is better
inline void writeInt64(Int64 x, DB::WriteBuffer & ostr)
{
    UInt8 byte = 0;
    for (size_t i = 0; i < 8; ++i)
    {
        byte = (x >> (8 * (7 - i))) & 0xFF;
        ostr.write((const char*)&byte, 1);
    }
}

inline void readString(std::string & x, DB::ReadBuffer & istr)
{
    Int64 size = readInt64(istr);
    if (size == 0)
        return;
    x.resize(size);
    istr.readStrict(&x[0], size);
}

}
