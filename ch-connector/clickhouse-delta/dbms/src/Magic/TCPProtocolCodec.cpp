#include <IO/ReadHelpers.h>
#include "TCPProtocolCodec.h"

namespace Magic
{

Int64 readInt64(DB::ReadBuffer & istr)
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

void readString(std::string & x, DB::ReadBuffer & istr)
{
    Int64 size = readInt64(istr);
    if (size == 0)
        return;
    x.resize(size);
    istr.readStrict(&x[0], size);
}

void writeInt64(Int64 x, DB::WriteBuffer & ostr)
{
    UInt8 byte = 0;
    for (size_t i = 0; i < 8; ++i)
    {
        byte = (x >> (8 * (7 - i))) & 0xFF;
        ostr.write((const char*)&byte, 1);
    }
}

}
