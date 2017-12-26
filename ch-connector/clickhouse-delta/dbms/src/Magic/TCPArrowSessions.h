#pragma once

#include <sstream>

#include "TCPProtocolCodec.h"
#include "TCPArrowHandler.h"

namespace DB
{

class TCPArrowSessions
{
public:
    static Poco::Net::TCPServerConnection * create(IServer & server, const Poco::Net::StreamSocket & socket)
    {
        auto connection = new TCPArrowHandler(server, socket);
        auto in = connection->getReader();

        Int64 flag = readInt64(*in);
        if (flag != ::Magic::Protocol::Header)
            throw Exception("TCP arrow request: first package should be header.");
        Int64 protocol_version = readInt64(*in);
        Int64 PROTOCOL_VERSION = 111;
        if (protocol_version != PROTOCOL_VERSION)
        {
            std::stringstream ss;
            ss << "TCP arrow request: protocol version not match: " <<
                protocol_version << " vs " << PROTOCOL_VERSION;
            throw Exception(ss.str());
        }

        std::string qid;
        readString(qid, *in);

        // TODO: Impl: token vs connections

        return connection;
    }
};

}
