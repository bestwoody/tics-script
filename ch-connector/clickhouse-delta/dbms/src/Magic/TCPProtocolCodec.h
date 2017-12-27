#pragma once

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

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

Int64 readInt64(DB::ReadBuffer & istr);

void readString(std::string & x, DB::ReadBuffer & istr);

// TODO: Use big-endian now, may be use little-endian is better
void writeInt64(Int64 x, DB::WriteBuffer & ostr);

}
