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

}

namespace Magic
{

static Int64 PROTOCOL_VERSION_MAJOR = 1;
static Int64 PROTOCOL_VERSION_MINOR = 1;
static Int64 PROTOCOL_ENCODER_VERSION = 1;

TCPArrowHandler::TCPArrowHandler(DB::IServer & server_, const Poco::Net::StreamSocket & socket_) :
    Poco::Net::TCPServerConnection(socket_),
    server(server_),
    log(&Poco::Logger::get("TCPArrowHandler")),
    connection_context(server.context()),
    query_context(server.context()),
    failed(false)
{
    try
    {
        connection_context.setSessionContext(connection_context);
        query_context = connection_context;

        DB::Settings global_settings = connection_context.getSettings();
        socket().setReceiveTimeout(global_settings.receive_timeout);
        socket().setSendTimeout(global_settings.send_timeout);
        socket().setNoDelay(true);

        in = std::make_shared<DB::ReadBufferFromPocoSocket>(socket());
        out = std::make_shared<DB::WriteBufferFromPocoSocket>(socket());

        auto interval = global_settings.poll_interval * 1000000;
        while (!static_cast<DB::ReadBufferFromPocoSocket &>(*in).poll(interval) && !server.isCancelled());

        if (server.isCancelled())
        {
            ARROW_HANDLER_LOG_WARNING("Server have been cancelled.");
            return;
        }
        if (in->eof())
        {
            ARROW_HANDLER_LOG_WARNING("Client has not sent any data.");
            return;
        }

        recvHeader();

        if (!default_database.empty())
        {
            if (!connection_context.isDatabaseExist(default_database))
                throw DB::Exception("Default database not exists.", DB::ErrorCodes::UNKNOWN_DATABASE);
            connection_context.setCurrentDatabase(default_database);
        }

        connection_context.setUser(user, password, socket().peerAddress(), "");

        // TODO: More client info
        {
            DB::ClientInfo & client_info = query_context.getClientInfo();
            client_info.client_name = client_name;
            client_info.client_version_major = protocol_version_major;
            client_info.client_version_minor = protocol_version_minor;
            client_info.interface = DB::ClientInfo::Interface::TCP;
        }

        while (!static_cast<DB::ReadBufferFromPocoSocket &>(*in).poll(interval) && !server.isCancelled());

        if (server.isCancelled())
        {
            ARROW_HANDLER_LOG_WARNING("Server have been cancelled.");
            return;
        }
        if (in->eof())
        {
            ARROW_HANDLER_LOG_WARNING("Client has not sent any data.");
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
        io = executeQuery(query, query_context, false, DB::QueryProcessingStage::Complete);
        if (io.out)
            throw DB::Exception("Not support insert query.");

        size_t this_encoder_count = 8;

        if (server.config().has("arrow_encoders"))
            this_encoder_count = server.config().getInt("arrow_encoders");
        if (encoder_count > 0)
            this_encoder_count = encoder_count;
        if (this_encoder_count <= 0)
            throw DB::Exception("Invalid encoder count.");

        encoder = std::make_shared<ArrowEncoderParall>(io, this_encoder_count);
        if (encoder->hasError())
            throw DB::Exception(encoder->getErrorString());

        ARROW_HANDLER_LOG_TRACE("Create " << this_encoder_count << " encoders.");
    }
    catch (...)
    {
        auto msg = DB::getCurrentExceptionMessage(false);
        encoder = std::make_shared<ArrowEncoderParall>(msg);
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

    ARROW_HANDLER_LOG_TRACE(std::fixed << std::setprecision(3) <<
        "Processed in " << watch.elapsedSeconds() << " sec.");
}

TCPArrowHandler::EncoderPtr TCPArrowHandler::getExecution()
{
    if (!encoder)
        throw DB::Exception("Sharing empty arrow encoder.");
    return encoder;
}

void TCPArrowHandler::onException(std::string msg)
{
    if (encoder)
        encoder->cancal(true);

    failed = true;

    if (msg.empty())
        msg = DB::getCurrentExceptionMessage(false);
    msg = toStr() + ". " + msg;

    LOG_ERROR(log, msg);

    try
    {
        writeInt64(Protocol::Utf8Error, *out);
        writeInt64(msg.size(), *out);
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
        throw DB::Exception(encoder->getErrorString());

    writeInt64(Protocol::ArrowSchema, *out);
    writeInt64(schema->size(), *out);
    out->write((const char*)schema->data(), schema->size());
    out->next();

    while (!server.isCancelled())
    {
        auto block = encoder->getEncodedBlock();
        if (encoder->hasError())
            throw DB::Exception(encoder->getErrorString());
        if (!block)
            break;

        writeInt64(Protocol::ArrowData, *out);
        writeInt64(block->size(), *out);
        out->write((const char*)block->data(), block->size());
        out->next();
    }

    writeInt64(Protocol::End, *out);
    writeInt64(0, *out);
    out->next();

    auto residue = encoder->residue();
    if (residue != 0)
    {
        std::stringstream error_ss;
        error_ss << "End process query, residue != 0: " << residue;
        throw DB::Exception(error_ss.str());
    }
}

void TCPArrowHandler::recvHeader()
{
    Int64 flag = Magic::readInt64(*in);
    if (flag != Protocol::Header)
        throw DB::Exception("First package should be header.");

    protocol_version_major = Magic::readInt64(*in);
    protocol_version_minor = Magic::readInt64(*in);
    if (protocol_version_major != PROTOCOL_VERSION_MAJOR || protocol_version_minor != PROTOCOL_VERSION_MINOR)
    {
        std::stringstream error_ss;
        error_ss <<
            "Protocol version not match: " <<
            protocol_version_major <<
            "." <<
            protocol_version_minor <<
            " vs " <<
            PROTOCOL_VERSION_MAJOR <<
            "." <<
            PROTOCOL_VERSION_MINOR;
        throw DB::Exception(error_ss.str());
    }

    Magic::readString(client_name, *in);

    Magic::readString(default_database, *in);

    user = "default";
    Magic::readString(user, *in);
    Magic::readString(password, *in);

    Magic::readString(encoder_name, *in);
    if (encoder_name != "arrow")
        throw DB::Exception("Only support arrow encoding.");

    encoder_version = Magic::readInt64(*in);
    if (encoder_version != PROTOCOL_ENCODER_VERSION)
    {
        std::stringstream error_ss;
        error_ss <<
            "Encoder version not match: " <<
            encoder_version <<
            " vs " <<
            PROTOCOL_ENCODER_VERSION;
        throw DB::Exception(error_ss.str());
    }
    encoder_count = Magic::readInt64(*in);

    client_count = Magic::readInt64(*in);
    client_index = Magic::readInt64(*in);

    LOG_TRACE(log,
        "Received header, proto ver: " <<
        protocol_version_major <<
        ", minor ver: " <<
        protocol_version_minor <<
        ", default database: '" <<
        default_database <<
        "', client name: " <<
        client_name <<
        ", user: " <<
        user <<
        ", encoder name: " <<
        encoder_name <<
        ", encoder ver: " <<
        encoder_version <<
        ", encoders: " <<
        encoder_count);
}

void TCPArrowHandler::recvQuery()
{
    DB::Int64 flag = Magic::readInt64(*in);
    if (flag != Protocol::Utf8Query)
        throw DB::Exception("Only receive query string after header.");

    Magic::readString(query_id, *in);
    if (query_id.empty())
        throw DB::Exception("Receive empty query_id.");
    query_context.setCurrentQueryId(query_id);
    Magic::readString(query, *in);
}

std::string TCPArrowHandler::toStr()
{
    std::stringstream info_ss;

    info_ss <<
        query_id <<
        ", #" <<
        client_index <<
        "/" <<
        client_count <<
        ", execution: " <<
        ((bool)encoder ? "good" : "null") <<
        ", ref: " <<
        encoder.use_count() <<
        ", status: " <<
        (failed ? "failed" : "good");

    return info_ss.str();
}

}
