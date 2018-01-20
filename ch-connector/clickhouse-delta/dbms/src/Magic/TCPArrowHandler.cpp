#include <iomanip>

#include <common/logger_useful.h>

#include <Core/Protocol.h>

#include <Common/ClickHouseRevision.h>
#include <Common/Stopwatch.h>

#include <DataStreams/NativeBlockOutputStream.h>
#include <Interpreters/executeQuery.h>

#include "TCPProtocolCodec.h"
#include "TCPArrowHandler.h"
#include "TCPArrowSessions.h"

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

TCPArrowHandler::TCPArrowHandler(IServer & server_, const Poco::Net::StreamSocket & socket_) :
    Poco::Net::TCPServerConnection(socket_), server(server_), log(&Poco::Logger::get("TCPArrowHandler")),
    connection_context(server.context()), query_context(server.context()), failed(false)
{
    try
    {
        connection_context.setSessionContext(connection_context);
        query_context = connection_context;

        Settings global_settings = connection_context.getSettings();
        socket().setReceiveTimeout(global_settings.receive_timeout);
        socket().setSendTimeout(global_settings.send_timeout);
        socket().setNoDelay(true);

        in = std::make_shared<ReadBufferFromPocoSocket>(socket());
        out = std::make_shared<WriteBufferFromPocoSocket>(socket());

        auto interval = global_settings.poll_interval * 1000000;
        while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(interval) && !server.isCancelled());

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

        // TODO: More client info
        {
            ClientInfo & client_info = query_context.getClientInfo();
            client_info.client_name = client_name;
            client_info.client_version_major = protocol_version_major;
            client_info.client_version_minor = protocol_version_minor;
            client_info.interface = ClientInfo::Interface::TCP;
        }

        while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(interval) && !server.isCancelled());

        if (server.isCancelled())
        {
            LOG_WARNING(log, "Server have been cancelled.");
            return;
        }
        if (in->eof())
        {
            LOG_WARNING(log, "Client has not sent any query.");
            return;
        }

        recvQuery();
    }
    catch (...)
    {
        onException();
    }
}

TCPArrowHandler::~TCPArrowHandler()
{
    TCPArrowSessions::instance().clear(this);
}

void TCPArrowHandler::startExecuting()
{
    if (failed)
        return;
    if (encoder)
        return;

    try
    {
        io = executeQuery(query, query_context, false, QueryProcessingStage::Complete);
        if (io.out)
            throw Exception("TCPArrowHandler do not support insert query.");

        size_t this_encoder_count = 8;

        if (server.config().has("arrow_encoders"))
            this_encoder_count = server.config().getInt("arrow_encoders");
        if (encoder_count > 0)
            this_encoder_count = encoder_count;
        if (this_encoder_count <= 0)
            throw Exception("Encoder number invalid.");

        encoder = std::make_shared<Magic::ArrowEncoderParall>(io, this_encoder_count);
        if (encoder->hasError())
            throw Exception(encoder->getErrorString());

        LOG_INFO(log, "TCPArrowHandler create arrow encoder, concurrent threads: " << this_encoder_count <<
            ", execution ref: " << encoder.use_count());
    }
    catch (...)
    {
        auto msg = DB::getCurrentExceptionMessage(true, true);
        encoder = std::make_shared<Magic::ArrowEncoderParall>(msg);
        onException(msg);
    }
}

void TCPArrowHandler::run()
{
    if (failed)
        return;

    Stopwatch watch;

    try
    {
       processOrdinaryQuery();
    }
    catch (...)
    {
        onException();
    }

    watch.stop();

    LOG_INFO(log, std::fixed << std::setprecision(3) << "Processed in " << watch.elapsedSeconds() << " sec.");
}

void TCPArrowHandler::onException(std::string msg)
{
    if (encoder)
        encoder->cancal(true);

    failed = true;

    if (msg.empty())
        msg = DB::getCurrentExceptionMessage(true, true);
    LOG_ERROR(log, msg);

    try
    {
        Magic::writeInt64(Magic::Protocol::Utf8Error, *out);
        Magic::writeInt64(msg.size(), *out);
        out->write((const char*)msg.c_str(), msg.size());
        out->next();
    }
    catch (...)
    {
        // Ignore sending errors, connection may have been lost
    }
}

void TCPArrowHandler::processOrdinaryQuery()
{
    auto schema = encoder->getEncodedSchema();
    if (encoder->hasError())
        throw Exception(encoder->getErrorString());

    Magic::writeInt64(Magic::Protocol::ArrowSchema, *out);
    Magic::writeInt64(schema->size(), *out);
    out->write((const char*)schema->data(), schema->size());
    out->next();

    while (!server.isCancelled())
    {
        auto block = encoder->getEncodedBlock();
        if (encoder->hasError())
            throw Exception(encoder->getErrorString());
        if (!block)
            break;

        Magic::writeInt64(Magic::Protocol::ArrowData, *out);
        Magic::writeInt64(block->size(), *out);
        out->write((const char*)block->data(), block->size());
        out->next();
    }

    Magic::writeInt64(Magic::Protocol::End, *out);
    Magic::writeInt64(0, *out);
    out->next();

    auto residue = encoder->residue();
    if (residue != 0)
    {
        std::string msg = "End process ordinary query, residue != 0";
        LOG_ERROR(log, msg << residue);
        throw Exception(msg);
    }
}

void TCPArrowHandler::recvHeader()
{
    Int64 flag = Magic::readInt64(*in);
    if (flag != Magic::Protocol::Header)
        throw Exception("TCP arrow request: first package should be header.");

    protocol_version_major = Magic::readInt64(*in);
    protocol_version_minor = Magic::readInt64(*in);
    if (protocol_version_major != PROTOCOL_VERSION_MAJOR || protocol_version_minor != PROTOCOL_VERSION_MINOR)
    {
        std::stringstream error_ss;
        error_ss << "TCP arrow request: protocol version not match: " <<
            protocol_version_major << "." << protocol_version_minor << " vs " <<
            PROTOCOL_VERSION_MAJOR << "." << PROTOCOL_VERSION_MINOR;
        throw Exception(error_ss.str());
    }

    Magic::readString(client_name, *in);

    Magic::readString(default_database, *in);

    user = "default";
    Magic::readString(user, *in);
    Magic::readString(password, *in);

    Magic::readString(encoder_name, *in);
    if (encoder_name != "arrow")
        throw Exception("TCPArrowHandler only support arrow encoding, required: `" + encoder_name + "`");

    encoder_version = Magic::readInt64(*in);
    if (encoder_version != PROTOCOL_ENCODER_VERSION)
    {
        std::stringstream error_ss;
        error_ss << "TCPArrowHandler encoder version not match: " << encoder_version << " vs " << PROTOCOL_ENCODER_VERSION;
        throw Exception(error_ss.str());
    }
    encoder_count = Magic::readInt64(*in);

    client_count = Magic::readInt64(*in);
    client_index = Magic::readInt64(*in);

    LOG_INFO(log, "TCPArrowHandler default database: " << default_database << ", client name: "
        << client_name << ", user: " << user << ", encoder: " << encoder_name);
}

void TCPArrowHandler::recvQuery()
{
    Int64 flag = Magic::readInt64(*in);
    if (flag != Magic::Protocol::Utf8Query)
        throw Exception("TCPArrowHandler only receive query string.");

    Magic::readString(query_id, *in);
    if (query_id.empty())
        throw Exception("Receive empty query_id.");
    query_context.setCurrentQueryId(query_id);
    Magic::readString(query, *in);
}

}
