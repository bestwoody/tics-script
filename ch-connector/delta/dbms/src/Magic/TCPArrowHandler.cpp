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
#include "AsyncArrowEncoder.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int POCO_EXCEPTION;
}

inline Int64 readInt64(ReadBuffer & istr)
{
    Int64 x = 0;
    for (size_t i = 0; i < 8; ++i)
    {
        if (istr.eof())
            throwReadAfterEOF();

        UInt8 byte = *istr.position();
        ++istr.position();
        x |= (byte) << (8 * (7 - i));
    }
    return x;
}

inline void readString(std::string & x, ReadBuffer & istr)
{
    Int64 size = readInt64(istr);
    x.resize(size);
    istr.readStrict(&x[0], size);
}

// TODO: use big-endian now, may be use little-endian is better
inline void writeInt64(Int64 x, WriteBuffer & ostr)
{
    UInt8 byte = 0;
    for (size_t i = 0; i < 8; ++i)
    {
        byte = (x >> (8 * (7 - i))) & 0xFF;
        ostr.write((const char*)&byte, 1);
    }
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
            failed = true;
        }

        watch.stop();
        LOG_INFO(log, std::fixed << std::setprecision(3) << "Processed in " << watch.elapsedSeconds() << " sec.");
    }
}


// TODO: async encoding
void TCPArrowHandler::processOrdinaryQuery()
{
    Magic::AsyncArrowEncoder encoder(state.io);

    auto schema = encoder.getEncodedSchema();
    writeInt64(::Magic::Protocol::ArrowSchema, *out);
    writeInt64(schema->size(), *out);
    out->write((const char*)schema->data(), schema->size());
    out->next();

    while (true)
    {
        auto block = encoder.getPreparedEncodedBlock();
        if (!block)
            break;
        writeInt64(::Magic::Protocol::ArrowData, *out);
        writeInt64(block->size(), *out);
        out->write((const char*)block->data(), block->size());
        out->next();
    }

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
