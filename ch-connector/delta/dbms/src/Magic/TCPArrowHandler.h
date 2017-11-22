#pragma once

#include <Poco/Net/TCPServerConnection.h>

#include <Common/CurrentMetrics.h>
#include <DataStreams/BlockIO.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include "Server/IServer.h"

namespace CurrentMetrics
{
    extern const Metric TCPConnection;
}

namespace Poco { class Logger; }

namespace DB
{


struct ArrowQueryState
{
    // TODO: use Arrow
    BlockOutputStreamPtr block_out;

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
    void sendData(Block & block);

    void initBlockInput();
    void initBlockOutput();
};

}
