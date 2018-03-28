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

namespace TheFlash
{

class TCPArrowHandler : public Poco::Net::TCPServerConnection
{
public:
    using EncoderPtr = std::shared_ptr<ArrowEncoderParall>;

    DB::String getQueryId()
    {
        return query_id;
    }

    DB::String getQuery()
    {
        return query;
    }

    DB::Int64 getClientCount()
    {
        return client_count;
    }

    DB::Int64 getClientIndex()
    {
        return client_index;
    }

    bool isFailed()
    {
        return failed;
    }

    void setExecution(EncoderPtr & encoder)
    {
        this->encoder = encoder;
        joined = true;
    }

    TCPArrowHandler(DB::IServer & server_, const Poco::Net::StreamSocket & socket_);

    ~TCPArrowHandler();

    void startExecuting();

    void run();

    EncoderPtr getExecution();

    void onException(std::string msg = "");

    std::string toStr();

private:
    void processOrdinaryQuery();
    void recvHeader();
    void recvQuery();

private:
    DB::IServer & server;
    Poco::Logger * log;

    DB::Context connection_context;
    DB::Context query_context;

    std::shared_ptr<DB::ReadBuffer> in;
    std::shared_ptr<DB::WriteBuffer> out;

    DB::String default_database;

    DB::Int64 protocol_version_major;
    DB::Int64 protocol_version_minor;

    DB::String client_name;
    DB::String user;

    // TODO: Not safe, but Server/TCPHandler.cpp do that too.
    DB::String password;

    DB::String encoder_name;
    DB::Int64 encoder_version;
    DB::Int64 encoder_count;

    DB::Int64 client_count;
    DB::Int64 client_index;

    DB::String query_id;
    DB::String query;
    DB::BlockIO io;

    EncoderPtr encoder;
    bool failed;
    bool joined;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TCPConnection};
};

#define ARROW_HANDLER_LOG_ERROR(message) \
    do { \
        LOG_ERROR(log, toStr() << ". " << message); \
    } while(false)

#define ARROW_HANDLER_LOG_WARNING(message) \
    do { \
        LOG_WARNING(log, toStr() << ". " << message); \
    } while(false)

#define ARROW_HANDLER_LOG_TRACE(message) \
    do { \
        LOG_TRACE(log, toStr() << ". " << message); \
    } while(false)

}
