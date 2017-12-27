#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Common/CurrentMetrics.h>
#include <DataStreams/BlockIO.h>

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include "Server/IServer.h"
#include "ArrowEncoderParall.h"

namespace CurrentMetrics
{
    extern const Metric TCPConnection;
}

namespace Poco { class Logger; }


namespace DB
{


struct ArrowQueryState
{
    String query_id;
    String query;
    BlockIO io;
};


// TODO: Bad running sequence, refact.
class TCPArrowHandler : public Poco::Net::TCPServerConnection
{
public:
    using EncoderPtr = std::shared_ptr<Magic::ArrowEncoderParall>;

    TCPArrowHandler(IServer & server_, const Poco::Net::StreamSocket & socket_) :
        Poco::Net::TCPServerConnection(socket_), server(server_), log(&Poco::Logger::get("TCPArrowHandler")),
        connection_context(server.context()), query_context(server.context()), failed(false)
    {
        init();
    }

    ~TCPArrowHandler();

    void startExecuting();

    void run();

    String getQueryId()
    {
        return state.query_id;
    }

    EncoderPtr getExecution()
    {
        if (!encoder)
            throw Exception("Share empty arrow encoder");
        return encoder;
    }

    void setExecution(EncoderPtr & encoder_)
    {
        encoder = encoder_;
    }

private:
    IServer & server;
    Poco::Logger * log;

    Context connection_context;
    Context query_context;

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    String default_database;

    Int64 protocol_version_major;
    Int64 protocol_version_minor;

    String client_name;
    String user;
    // TODO: Not safe
    String password;

    String encoder_name;
    Int64 encoder_version;
    Int64 encoder_count;

    ArrowQueryState state;
    EncoderPtr encoder;
    bool failed;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TCPConnection};

private:
    void init();
    void runImpl();

    void processOrdinaryQuery();
    void recvHeader();
    void recvQuery();
    void sendError(const std::string & msg);

    void initBlockInput();
    void initBlockOutput();
};

}
