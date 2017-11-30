#include <iomanip>

#include <common/logger_useful.h>

#include <Core/Protocol.h>

#include <Common/ClickHouseRevision.h>
#include <Common/Stopwatch.h>

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/copyData.h>

#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Interpreters/executeQuery.h>

#include "TCPArrowHandler.h"
#include "Session.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int POCO_EXCEPTION;
}


void TCPArrowHandler::runImpl()
{
    connection_context = server.context();
    connection_context.setSessionContext(connection_context);

    Settings global_settings = connection_context.getSettings();

    socket().setReceiveTimeout(global_settings.receive_timeout);
    socket().setSendTimeout(global_settings.send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    if (in->eof())
    {
        LOG_WARNING(log, "Client has not sent any data.");
        return;
    }

    connection_context.setUser("default", "", socket().peerAddress(), "");
    connection_context.setCurrentDatabase("default");

    while (1)
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
            recvQuery();
            state.io = executeQuery(state.query, query_context, false, QueryProcessingStage::Complete);

            if (state.io.out)
                throw Exception("TCPArrowHandler do not support insert query.");

            processOrdinaryQuery();
        }
        catch (...)
        {
            LOG_ERROR(log, "Exception, TODO: details.");
            state.io.onException();
        }

        watch.stop();
        LOG_INFO(log, std::fixed << std::setprecision(3) << "Processed in " << watch.elapsedSeconds() << " sec.");
    }
}


// TODO: async encoding
void TCPArrowHandler::processOrdinaryQuery()
{
    Magic::Session session(state.io);

    auto schema = session.getEncodedSchema();
    writeVarUInt(::Magic::Protocol::ArrowSchema, *out);
    out->write((const char*)schema->data(), schema->size());
    out->next();

    while (true)
    {
        auto block = session.getEncodedBlock();
        writeVarUInt(::Magic::Protocol::ArrowData, *out);
        out->write((const char*)block->data(), block->size());
        out->next();
    }

    writeVarUInt(::Magic::Protocol::End, *out);
    out->next();
}


void TCPArrowHandler::recvQuery()
{
    size_t flag = 0;
    readVarUInt(flag, *in);
    if (flag != ::Magic::Protocol::Utf8Query)
        throw Exception("TCPArrowHandler only receive query string.");

    readStringBinary(state.query, *in);
}

void TCPArrowHandler::sendData(Block & block)
{
    if (!state.block_out)
        state.block_out = std::make_shared<NativeBlockOutputStream>(*out, DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE);

    writeVarUInt(Protocol::Server::Data, *out);
    writeStringBinary("", *out);

    state.block_out->write(block);
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
