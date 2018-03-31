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
    extern const int THEFLASH_BAD_REQUEST;
    extern const int THEFLASH_ENCODER_ERROR;
    extern const int THEFLASH_SESSION_ERROR;
}

}

namespace TheFlash
{

static Int64 PROTOCOL_VERSION_MAJOR = 1;
static Int64 PROTOCOL_VERSION_MINOR = 1;
static Int64 PROTOCOL_ENCODER_VERSION = 1;
static Int64 MAX_CLIENT_COUNT = 1024;

// Not neccesary, but we still keep it for double safety.
inline std::string getExceptionMessage()
{
    try
    {
        return DB::getCurrentExceptionMessage(false);
    }
    catch (...)
    {
        return "Exception when getExceptionMessage.";
    }
}

TCPArrowHandler::TCPArrowHandler(DB::IServer & server_, const Poco::Net::StreamSocket & socket_) :
    Poco::Net::TCPServerConnection(socket_),
    server(server_),
    log(&Poco::Logger::get("TCPArrowHandler")),
    connection_context(server.context()),
    query_context(server.context()),
    protocol_version_major(-1),
    protocol_version_minor(-1),
    encoder_version(-1),
    encoder_count(-1),
    client_count(-1),
    client_index(-1),
    failed(false),
    joined(false)
{
    size_t interval = 0;

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

        interval = global_settings.poll_interval * 1000000;

        try
        {
            LOG_TRACE(log, "Socket poll interval: " << interval);

            // TODO: This might have crush problem on MacOs, not sure yet, waiting for next crush for more info
            // while (!static_cast<DB::ReadBufferFromPocoSocket &>(*in).poll(interval) && !server.isCancelled());

            DB::ReadBufferFromPocoSocket * in_socket = static_cast<DB::ReadBufferFromPocoSocket *>(in.get());
            while (true)
            {
                if (in_socket->poll(interval))
                {
                    break;
                }
                LOG_TRACE(log, "Socket polling might failed.");
                if (server.isCancelled())
                {
                    LOG_TRACE(log, "Server have been cancelled.");
                    break;
                }
            }

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

            LOG_TRACE(log, "Reading connection header.");
            recvHeader();
        }
        catch (...)
        {
            LOG_ERROR(log, "Recv header failed.");
            auto msg = getExceptionMessage();
            onException("Recv header failed: " + msg);
            return;
        }
    }
    catch (...)
    {
        LOG_ERROR(log, "Init failed.");
        auto msg = getExceptionMessage();
        onException("Init failed: " + msg);
        return;
    }

    try
    {
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
    }
    catch (...)
    {
        LOG_ERROR(log, "Context setup failed.");
        auto msg = getExceptionMessage();
        onException("Context setup failed: " + msg);
        return;
    }

    try
    {
        // TODO: This might have crush problem on MacOs, not sure yet, waiting for next crush for more info
        // while (!static_cast<DB::ReadBufferFromPocoSocket &>(*in).poll(interval) && !server.isCancelled());
        DB::ReadBufferFromPocoSocket * in_socket = static_cast<DB::ReadBufferFromPocoSocket *>(in.get());
        while (true)
        {
            if (in_socket->poll(interval))
            {
                break;
            }
            LOG_TRACE(log, "Socket polling might failed.");
            if (server.isCancelled())
            {
                LOG_TRACE(log, "Server have been cancelled.");
                break;
            }
        }

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

        LOG_TRACE(log, "Reading query.");
        recvQuery();
    }
    catch (...)
    {
        LOG_ERROR(log, "Recv query failed.");
        auto msg = getExceptionMessage();
        onException("Recv query failed: " + msg);
        return;
    }
}

TCPArrowHandler::~TCPArrowHandler()
{
    if (joined)
        TCPArrowSessions::instance().clear(this, failed);
    else
        ARROW_HANDLER_LOG_TRACE("Connection done.");
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
            throw DB::Exception("Not support insert query.", DB::ErrorCodes::THEFLASH_BAD_REQUEST);

        size_t this_encoder_count = 8;

        if (server.config().has("arrow_encoders"))
            this_encoder_count = server.config().getInt("arrow_encoders");
        if (encoder_count > 0)
            this_encoder_count = encoder_count;
        if (this_encoder_count <= 0)
            throw DB::Exception("Invalid encoder count.", DB::ErrorCodes::THEFLASH_BAD_REQUEST);

        encoder = std::make_shared<ArrowEncoderParall>(io, this_encoder_count);
        if (encoder->hasError())
            throw DB::Exception(encoder->getErrorString(), DB::ErrorCodes::THEFLASH_ENCODER_ERROR);

        joined = true;

        ARROW_HANDLER_LOG_TRACE("Create " << this_encoder_count << " encoders.");
    }
    catch (...)
    {
        LOG_ERROR(log, "Start executing failed.");
        auto msg = getExceptionMessage();
        encoder = std::make_shared<ArrowEncoderParall>(msg);
        onException("Start executing failed:" + msg);
    }
}

void TCPArrowHandler::run()
{
    Stopwatch watch;

    if (!failed)
    {
        try
        {
           processOrdinaryQuery();
        }
        catch (...)
        {
            LOG_ERROR(log, "Process query failed.");
            auto msg = getExceptionMessage();
            onException("Process query failed: " + msg);
        }
    }
    else
    {
        // Trying to send exception to client
        onException("Pulling data from a failed connection.");
    }

    watch.stop();
    ARROW_HANDLER_LOG_TRACE(std::fixed << std::setprecision(3) <<
        "Processed in " << watch.elapsedSeconds() << " sec.");
}

TCPArrowHandler::EncoderPtr TCPArrowHandler::getExecution()
{
    if (!encoder)
        throw DB::Exception("Sharing empty arrow encoder.", DB::ErrorCodes::THEFLASH_SESSION_ERROR);
    return encoder;
}

void TCPArrowHandler::onException(std::string msg)
{
    LOG_TRACE(log, "Handling exception.");
    try
    {
        if (encoder)
        {
            LOG_TRACE(log, "Try closing encoder.");
            encoder->cancal(true);
        }
    }
    catch (...)
    {
        LOG_ERROR(log, "Try closing encoder but failed, origin error: " + msg);
    }

    LOG_TRACE(log, "Setting failed flag.");
    failed = true;

    try
    {
        LOG_TRACE(log, "Gathering error info.");
        if (msg.empty())
            msg = getExceptionMessage();
        msg = toStr() + ". " + msg;
        LOG_ERROR(log, msg);
    }
    catch (...)
    {
        LOG_ERROR(log, "Try gathering error info but failed, origin error: " + msg);
    }

    try
    {
        LOG_TRACE(log, "Trying to send exception to client.");
        writeInt64(Protocol::Utf8Error, *out);
        writeInt64(msg.size(), *out);
        out->write((const char*)msg.c_str(), msg.size());
        out->next();
        LOG_TRACE(log, "Exception had been sent.");
    }
    catch (...)
    {
        // Ignore sending errors, connection may have been lost
        LOG_ERROR(log, "Sending exception to client failed.");
    }
    LOG_TRACE(log, "Exception handled.");
}

void TCPArrowHandler::processOrdinaryQuery()
{
    auto schema = encoder->getEncodedSchema();
    if (encoder->hasError())
        throw DB::Exception(encoder->getErrorString(), DB::ErrorCodes::THEFLASH_ENCODER_ERROR);

    writeInt64(Protocol::ArrowSchema, *out);
    writeInt64(schema->size(), *out);
    out->write((const char*)schema->data(), schema->size());
    out->next();

    while (!server.isCancelled() && !failed)
    {
        auto block = encoder->getEncodedBlock();
        if (encoder->hasError())
            throw DB::Exception(encoder->getErrorString(), DB::ErrorCodes::THEFLASH_ENCODER_ERROR);
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
        throw DB::Exception(error_ss.str(), DB::ErrorCodes::THEFLASH_SESSION_ERROR);
    }
}

void TCPArrowHandler::recvHeader()
{
    Int64 flag = TheFlash::readInt64(*in);
    if (flag != Protocol::Header)
        throw DB::Exception("First package should be header.", DB::ErrorCodes::THEFLASH_BAD_REQUEST);

    protocol_version_major = TheFlash::readInt64(*in);
    protocol_version_minor = TheFlash::readInt64(*in);
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
        throw DB::Exception(error_ss.str(), DB::ErrorCodes::THEFLASH_BAD_REQUEST);
    }

    TheFlash::readString(client_name, *in);
    TheFlash::readString(default_database, *in);

    user = "default";
    TheFlash::readString(user, *in);
    TheFlash::readString(password, *in);

    TheFlash::readString(encoder_name, *in);
    if (encoder_name != "arrow")
        throw DB::Exception("Only support arrow encoding.", DB::ErrorCodes::THEFLASH_BAD_REQUEST);

    encoder_version = TheFlash::readInt64(*in);
    if (encoder_version != PROTOCOL_ENCODER_VERSION)
    {
        std::stringstream error_ss;
        error_ss <<
            "Encoder version not match: " <<
            encoder_version <<
            " vs " <<
            PROTOCOL_ENCODER_VERSION;
        throw DB::Exception(error_ss.str(), DB::ErrorCodes::THEFLASH_BAD_REQUEST);
    }
    encoder_count = TheFlash::readInt64(*in);

    client_count = TheFlash::readInt64(*in);
    client_index = TheFlash::readInt64(*in);
    if (client_count <= client_index || client_count <= 0 || client_index < 0 || client_count >= MAX_CLIENT_COUNT)
        throw DB::Exception("Invalid client index/count.", DB::ErrorCodes::THEFLASH_BAD_REQUEST);

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
    DB::Int64 flag = TheFlash::readInt64(*in);
    if (flag != Protocol::Utf8Query)
        throw DB::Exception("Only receive query string after header.", DB::ErrorCodes::THEFLASH_BAD_REQUEST);

    TheFlash::readString(query_id, *in);
    if (query_id.empty())
        throw DB::Exception("Receive empty query_id.", DB::ErrorCodes::THEFLASH_BAD_REQUEST);
    query_context.setCurrentQueryId(query_id);
    TheFlash::readString(query, *in);
}

std::string TCPArrowHandler::toStr()
{
    std::stringstream info_ss;

    info_ss <<
        query_id <<
        " #" <<
        client_index <<
        "/" <<
        client_count <<
        " r" <<
        ((bool)encoder ? "+" : "-") <<
        encoder.use_count() <<
        " " <<
        (failed ? "failed" : "good");

    return info_ss.str();
}

}
