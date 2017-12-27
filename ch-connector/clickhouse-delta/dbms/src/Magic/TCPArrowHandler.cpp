#include <iomanip>

#include <common/logger_useful.h>

#include <Core/Protocol.h>

#include <Common/ClickHouseRevision.h>
#include <Common/Stopwatch.h>

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
    extern const int UNKNOWN_DATABASE;
}

static Int64 PROTOCOL_VERSION_MAJOR = 1;
static Int64 PROTOCOL_VERSION_MINOR = 1;
static Int64 PROTOCOL_ENCODER_VERSION = 1;

// TODO: Catch error when connection lost
void TCPArrowHandler::runImpl()
{
    connection_context = server.context();
    connection_context.setSessionContext(connection_context);

    Settings global_settings = connection_context.getSettings();

    socket().setReceiveTimeout(global_settings.receive_timeout);
    socket().setSendTimeout(global_settings.send_timeout);
    socket().setNoDelay(true);

    if (!default_database.empty())
    {
        if (!connection_context.isDatabaseExist(default_database))
        {
            Exception e("Database " + default_database + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
            LOG_ERROR(log, "Code: " << e.code() << ", e.displayText() = " << e.displayText()
                << ", Stack trace:\n\n" << e.getStackTrace().toString());
            throw e;
        }

        connection_context.setCurrentDatabase(default_database);
    }

    connection_context.setUser(user, password, socket().peerAddress(), "");

    // Client info
    // TODO: More info
    {
        ClientInfo & client_info = query_context.getClientInfo();
        client_info.client_name = client_name;
        client_info.client_version_major = protocol_version_major;
        client_info.client_version_minor = protocol_version_minor;
        client_info.interface = ClientInfo::Interface::TCP;
    }

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
        catch (Exception e)
        {
            auto msg = DB::getCurrentExceptionMessage(true, true);
            LOG_ERROR(log, msg);
            state.io.onException();
            failed = true;

            sendError(msg);
        }

        watch.stop();
        LOG_INFO(log, std::fixed << std::setprecision(3) << "Processed in " << watch.elapsedSeconds() << " sec.");
    }
}


void TCPArrowHandler::processOrdinaryQuery()
{
    size_t this_encoder_count = 8;
    if (server.config().has("arrow_encoders"))
        this_encoder_count = server.config().getInt("arrow_encoders");
    if (encoder_count > 0)
        this_encoder_count = encoder_count;
    if (this_encoder_count <= 0)
        throw Exception("Encoder number invalid.");

    LOG_INFO(log, "Start process ordinary query, arrow encoder threads: " << this_encoder_count);

    Magic::ArrowEncoderParall encoder(state.io, this_encoder_count);

    if (encoder.hasError())
        throw Exception(encoder.getErrorString());

    auto schema = encoder.getEncodedSchema();
    if (encoder.hasError())
        throw Exception(encoder.getErrorString());

    Magic::writeInt64(Magic::Protocol::ArrowSchema, *out);
    Magic::writeInt64(schema->size(), *out);
    out->write((const char*)schema->data(), schema->size());
    out->next();

    while (true)
    {
        auto block = encoder.getEncodedBlock();
        if (encoder.hasError())
            throw Exception(encoder.getErrorString());
        if (!block)
            break;

        // TODO: May block forever, if client dead
        Magic::writeInt64(Magic::Protocol::ArrowData, *out);
        Magic::writeInt64(block->size(), *out);
        out->write((const char*)block->data(), block->size());
        out->next();
    }

    Magic::writeInt64(Magic::Protocol::End, *out);
    Magic::writeInt64(0, *out);
    out->next();

    auto residue = encoder.residue();
    LOG_INFO(log, "End process ordinary query, residue: " << residue);
    if (residue != 0)
        throw Exception("End process ordinary query, residue != 0");
}


void TCPArrowHandler::recvHeader()
{
    // TODO: better throw exceptions

    Int64 flag = Magic::readInt64(*in);
    if (flag != Magic::Protocol::Header)
        throw Exception("TCP arrow request: first package should be header.");

    protocol_version_major = Magic::readInt64(*in);
    protocol_version_minor = Magic::readInt64(*in);
    if (protocol_version_major != PROTOCOL_VERSION_MAJOR || protocol_version_minor != PROTOCOL_VERSION_MINOR)
    {
        std::stringstream ss;
        ss << "TCP arrow request: protocol version not match: " <<
            protocol_version_major << "." << protocol_version_minor << " vs " <<
            PROTOCOL_VERSION_MAJOR << "." << PROTOCOL_VERSION_MINOR;
        throw Exception(ss.str());
    }

    Magic::readString(client_name, *in);
    LOG_INFO(log, "TCPArrowHandler client name: " << client_name);

    Magic::readString(default_database, *in);
    LOG_INFO(log, "TCPArrowHandler default database: " << default_database);

    user = "default";
    Magic::readString(user, *in);
    Magic::readString(password, *in);
    LOG_INFO(log, "TCPArrowHandler user: " << user);

    Magic::readString(encoder_name, *in);
    LOG_INFO(log, "TCPArrowHandler encoder: " << encoder_name);
    if (encoder_name != "arrow")
        throw Exception("TCPArrowHandler only support arrow encoding, required: `" + encoder_name + "`");

    encoder_version = Magic::readInt64(*in);
    if (encoder_version != PROTOCOL_ENCODER_VERSION)
    {
        std::stringstream ss;
        ss << "TCPArrowHandler encoder version not match: " << encoder_version << " vs " << PROTOCOL_ENCODER_VERSION;
        throw Exception(ss.str());
    }
    encoder_count = Magic::readInt64(*in);
}


void TCPArrowHandler::recvQuery()
{
    Int64 flag = Magic::readInt64(*in);
    if (flag != Magic::Protocol::Utf8Query)
        throw Exception("TCPArrowHandler only receive query string.");
    Magic::readString(state.query_id, *in);
    query_context.setCurrentQueryId(state.query_id);
    Magic::readString(state.query, *in);
}


void TCPArrowHandler::sendError(const std::string & msg)
{
    Magic::writeInt64(Magic::Protocol::Utf8Error, *out);
    Magic::writeInt64(msg.size(), *out);
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
