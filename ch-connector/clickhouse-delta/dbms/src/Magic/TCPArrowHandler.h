#pragma once

#include <Poco/Net/TCPServerConnection.h>

#include <Common/CurrentMetrics.h>

#include <DataStreams/BlockIO.h>

#include "Server/IServer.h"

namespace Magic
{
    namespace Protocol
    {
        enum
        {
            End = 0,
            Utf8Error = 1,
            Utf8Query = 2,
            ArrowSchema = 3,
            ArrowData = 4,
        };
    }
}


namespace CurrentMetrics
{
    extern const Metric TCPConnection;
}


namespace Poco { class Logger; }


namespace DB
{


struct ArrowQueryState
{
    String query;
    BlockIO io;

    void reset()
    {
        *this = ArrowQueryState();
    }
};


class TCPArrowHandler : public Poco::Net::TCPServerConnection
{
public:
    TCPArrowHandler(IServer & server_, const Poco::Net::StreamSocket & socket_) :
        Poco::Net::TCPServerConnection(socket_), server(server_), log(&Poco::Logger::get("TCPArrowHandler")),
        connection_context(server.context()), query_context(server.context())
    {
    }

    ~TCPArrowHandler()
    {
        LOG_INFO(log, "~TCPArrowHandler");
        state.io.onFinish();
    }

    void run();

private:
    IServer & server;
    Poco::Logger * log;

    Context connection_context;
    Context query_context;

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    String default_database;

    ArrowQueryState state;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TCPConnection};

private:
    void runImpl();

    void processOrdinaryQuery();
    void recvQuery();
    void sendError(const std::string & msg);

    void initBlockInput();
    void initBlockOutput();
};

}
