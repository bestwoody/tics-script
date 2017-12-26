#include <iomanip>

#include <common/logger_useful.h>

#include <Core/Protocol.h>

#include <Common/ClickHouseRevision.h>
#include <Common/Stopwatch.h>

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/copyData.h>

#include <DataStreams/NativeBlockOutputStream.h>
#include <Interpreters/executeQuery.h>

#include "TCPProtocolCodec.h"
#include "TCPArrowHandler.h"
#include "ArrowEncoderParall.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int POCO_EXCEPTION;
}


// TODO: Catch error when connection lost
void TCPArrowHandler::runImpl()
{
    connection_context = server.context();
    connection_context.setSessionContext(connection_context);

    Settings global_settings = connection_context.getSettings();

    socket().setReceiveTimeout(global_settings.receive_timeout);
    socket().setSendTimeout(global_settings.send_timeout);
    socket().setNoDelay(true);

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

    connection_context.setUser("default", "", socket().peerAddress(), "");
    connection_context.setCurrentDatabase("default");

    bool failed = false;
    while (!failed)
    {
        /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
        while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(global_settings.poll_interval * 1000000) && !server.isCancelled());

        /// If we need to shut down, or client disconnects.
        if (server.isCancelled() || in->eof())
            break;

        Stopwatch watch;
        state.reset();

        try
        {
            query_context = connection_context;

            // Protocol: Receive query string
            recvQuery();

            state.io = executeQuery(state.query, query_context, false, QueryProcessingStage::Complete);

            if (state.io.out)
                throw Exception("TCPArrowHandler do not support insert query.");

            processOrdinaryQuery();
        }
        catch (Exception e)
        {
            auto msg = DB::getCurrentExceptionMessage(true, true);
            LOG_ERROR(log, msg);
            state.io.onException();
            failed = true;

            // Protocol: Send error string
            sendError(msg);
        }

        watch.stop();
        LOG_INFO(log, std::fixed << std::setprecision(3) << "Processed in " << watch.elapsedSeconds() << " sec.");
    }
}


void TCPArrowHandler::processOrdinaryQuery()
{
    size_t decoders = 8;
    if (server.config().has("arrow_encoders"))
        decoders = server.config().getInt("arrow_encoders");

    LOG_INFO(log, "Start process ordinary query, arrow encoder threads: " << decoders);

    Magic::ArrowEncoderParall encoder(state.io, decoders);

    if (encoder.hasError())
        throw Exception(encoder.getErrorString());

    auto schema = encoder.getEncodedSchema();
    if (encoder.hasError())
        throw Exception(encoder.getErrorString());

    // Protocol: Send encoded schema
    writeInt64(::Magic::Protocol::ArrowSchema, *out);
    writeInt64(schema->size(), *out);
    out->write((const char*)schema->data(), schema->size());
    out->next();

    while (true)
    {
        auto block = encoder.getPreparedEncodedBlock();
        if (encoder.hasError())
            throw Exception(encoder.getErrorString());
        if (!block)
            break;

        // Protocol: Send encoded bock
        // TODO: May block forever, if client dead
        writeInt64(::Magic::Protocol::ArrowData, *out);
        writeInt64(block->size(), *out);
        out->write((const char*)block->data(), block->size());
        out->next();
    }

    // Protocol: Send ending mark
    writeInt64(::Magic::Protocol::End, *out);
    writeInt64(0, *out);
    out->next();
}


void TCPArrowHandler::recvQuery()
{
    Int64 flag = readInt64(*in);
    if (flag != ::Magic::Protocol::Utf8Query)
        throw Exception("TCPArrowHandler only receive query string.");
    readString(state.query, *in);
}


void TCPArrowHandler::sendError(const std::string & msg)
{
    writeInt64(::Magic::Protocol::Utf8Error, *out);
    writeInt64(msg.size(), *out);
    out->write((const char*)msg.c_str(), msg.size());
    out->next();
}


void TCPArrowHandler::run()
{
    try
    {
        runImpl();
        LOG_INFO(log, "Done processing connection.");
    }
    catch (Poco::Exception & e)
    {
        /// Timeout - not an error.
        if (!strcmp(e.what(), "Timeout"))
        {
            LOG_DEBUG(log, "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
                << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
        }
        else
        {
            throw;
        }
    }
}

}
