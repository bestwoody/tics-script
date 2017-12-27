#pragma once

#include <sstream>

#include "TCPArrowHandler.h"

namespace DB
{

class TCPArrowSessions
{
public:
    static Poco::Net::TCPServerConnection * create(IServer & server, const Poco::Net::StreamSocket & socket)
    {
        auto conn = new TCPArrowHandler(server, socket);
        // TODO: Impl: token vs connections
        return conn;
    }
};

}
