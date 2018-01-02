#pragma once

#include <sstream>
#include <mutex>
#include <condition_variable>

#include <common/logger_useful.h>
#include "TCPArrowHandler.h"

namespace DB
{

class TCPArrowSessions
{
public:
    static TCPArrowSessions & instance()
    {
        static TCPArrowSessions global_instance;
        return global_instance;
    }

    TCPArrowSessions() : log(&Logger::get("TCPArrowSessions"))
    {
    }

    // RISK: may slow down TCP connection accepting, since we receive data in creating function
    Poco::Net::TCPServerConnection * create(IServer & server, const Poco::Net::StreamSocket & socket)
    {
        auto connection = new TCPArrowHandler(server, socket);
        auto query_id = connection->getQueryId();
        auto client_index = connection->getClientIndex();
        auto client_count = connection->getClientCount();
        LOG_TRACE(log, "TCP arrow connection established, query_id: " << query_id);

        std::unique_lock<std::mutex> lock{mutex};

        Sessions::iterator session = sessions.find(query_id);
        if (session != sessions.end())
        {
            LOG_TRACE(log, "Connection join to query_id: " << query_id <<
                ", client #" << client_index << "/" << client_count);
            if (!session->second.execution)
                throw Exception("Join to expired session, query_id: " + query_id);
            connection->setExecution(session->second.execution);
        }
        else
        {
            auto client_count = connection->getClientCount();
            LOG_TRACE(log, "First connection in query_id: " << query_id <<
                ", client #" << client_index << "/" << client_count);

            connection->startExecuting();

            sessions.emplace(query_id, Session(client_count, connection->getExecution()));
            session = sessions.find(query_id);
        }

        session->second.active_clients.emplace(client_index, true);

        return connection;
    }

    void clear(TCPArrowHandler * connection)
    {
        std::unique_lock<std::mutex> lock{mutex};

        auto query_id = connection->getQueryId();
        auto client_index = connection->getClientIndex();
        Sessions::iterator it = sessions.find(query_id);

        if (it == sessions.end())
        {
            auto msg = "Clear connection in query_id: " + query_id + " failed, session not found.";
            LOG_TRACE(log, msg);
            throw Exception(msg);
        }

        Session & session = it->second;

        session.active_clients[client_index] = false;
        session.finished_clients += 1;

        LOG_TRACE(log, "Connection done in query_id: " << query_id << ", sessions: " <<
            sessions.size() << ", connections: " << (session.client_count - session.finished_clients) <<
            ", client #" << client_index << "/" << session.client_count);

        // Can't clear up immidiatly, may cause double run.
        // TODO: Tombstone + expired clean
        if (session.finished_clients == session.client_count)
        {
            session.active_clients.clear();
            session.execution = NULL;
            LOG_TRACE(log, "Clear session query_id: " << query_id << ", sessions: " << sessions.size());
        }
    }

private:
    struct Session
    {
        Int64 client_count;
        TCPArrowHandler::EncoderPtr execution;
        Int64 finished_clients;
        std::unordered_map<Int64, bool> active_clients;

        Session(Int64 client_count_, TCPArrowHandler::EncoderPtr execution_) :
            client_count(client_count_), execution(execution_), finished_clients(0) {}
    };

    using Sessions = std::unordered_map<String, Session>;

    Poco::Logger * log;
    Sessions sessions;
    std::mutex mutex;
};

}
