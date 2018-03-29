#pragma once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <common/logger_useful.h>
#include "Server/IServer.h"

#include "TCPArrowSessions.h"

namespace Poco
{
    class Logger;
}

namespace TheFlash
{

class TCPArrowHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
    DB::IServer & server;
    Poco::Logger * log;

public:
    explicit TCPArrowHandlerFactory(DB::IServer & server_)
        : server(server_), log(&Logger::get("TCPArrowHandlerFactory"))
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        return TCPArrowSessions::instance().create(server, socket);
    }
};

}
