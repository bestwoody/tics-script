#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Common/CurrentMetrics.h>
#include <DataStreams/BlockIO.h>

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

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
    String query_id;
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
        try
        {
            Settings global_settings = connection_context.getSettings();
            socket().setReceiveTimeout(global_settings.receive_timeout);

            in = std::make_shared<ReadBufferFromPocoSocket>(socket());
            out = std::make_shared<WriteBufferFromPocoSocket>(socket());

            while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(global_settings.poll_interval * 1000000) && !server.isCancelled());

            if (server.isCancelled())
            {
                LOG_WARNING(log, "Server have been cancelled.");
                return;
            }

            if (in->eof())
            {
                LOG_WARNING(log, "Client has not sent any data.");
                return;
            }

            recvHeader();
        }
        catch (Exception e)
        {
            auto msg = DB::getCurrentExceptionMessage(true, true);
            LOG_ERROR(log, msg);
            // Protocol: Send error string
            sendError(msg);
        }
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

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TCPConnection};

private:
    void runImpl();

    void processOrdinaryQuery();
    void recvHeader();
    void recvQuery();
    void sendError(const std::string & msg);

    void initBlockInput();
    void initBlockOutput();
};

}
