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

namespace Poco
{
    class Logger;
}

namespace DB
{

class TCPArrowHandler : public Poco::Net::TCPServerConnection
{
public:
    using EncoderPtr = std::shared_ptr<Magic::ArrowEncoderParall>;

    String getQueryId()
    {
        return query_id;
    }

    String getQuery()
    {
        return query;
    }

    Int64 getClientCount()
    {
        return client_count;
    }

    Int64 getClientIndex()
    {
        return client_index;
    }

    EncoderPtr getExecution()
    {
        if (!encoder)
            throw Exception("Share empty arrow encoder");
        return encoder;
    }

    void setExecution(EncoderPtr & encoder)
    {
        this->encoder = encoder;
    }

    TCPArrowHandler(IServer & server_, const Poco::Net::StreamSocket & socket_);

    ~TCPArrowHandler();

    void startExecuting();

    void run();

private:
    void processOrdinaryQuery();
    void recvHeader();
    void recvQuery();

    void onException(std::string msg = "");

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

    Int64 client_count;
    Int64 client_index;

    String query_id;
    String query;
    BlockIO io;

    EncoderPtr encoder;
    bool failed;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TCPConnection};
};

}
