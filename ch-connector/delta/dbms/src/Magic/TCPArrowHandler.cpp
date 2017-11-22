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
            readStringBinary(state.query, *in);
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


// TODO: use arrow
void TCPArrowHandler::processOrdinaryQuery()
{
    /// Pull query execution result, if exists, and send it to network.
    if (state.io.in)
    {
        AsynchronousBlockInputStream async_in(state.io.in);
        async_in.readPrefix();

        while (true)
        {
            Block block;

            while (true)
            {
                if (async_in.poll(query_context.getSettingsRef().interactive_delay / 1000))
                {
                    /// There is the following result block.
                    block = async_in.read();
                    break;
                }
            }

            sendData(block);
            if (!block)
                break;
        }

        async_in.readSuffix();
    }

    state.io.onFinish();
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
