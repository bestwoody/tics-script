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
            Header = 0,
            End = 1,
            Utf8Error = 8,
            Utf8Query = 9,
            ArrowSchema = 10,
            ArrowData = 11,
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
        Settings global_settings = connection_context.getSettings();
        socket().setReceiveTimeout(global_settings.receive_timeout);

        in = std::make_shared<ReadBufferFromPocoSocket>(socket());
        out = std::make_shared<WriteBufferFromPocoSocket>(socket());
    }

    ~TCPArrowHandler()
    {
        LOG_INFO(log, "~TCPArrowHandler");
        state.io.onFinish();
    }

    std::shared_ptr<ReadBuffer> getReader()
    {
        return in;
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
