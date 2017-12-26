#pragma once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <common/logger_useful.h>
#include "Server/IServer.h"

#include "TCPArrowSessions.h"

namespace Poco { class Logger; }

namespace DB
{

class TCPArrowHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;

public:
    explicit TCPArrowHandlerFactory(IServer & server_)
        : server(server_), log(&Logger::get("TCPArrowHandlerFactory"))
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        LOG_TRACE(log, "TCP arrow request. Address: " << socket.peerAddress().toString());
        return TCPArrowSessions::create(server, socket);
    }
};

}
