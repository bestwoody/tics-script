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
        LOG_TRACE(log, "TCP arrow connection established, query_id: " + query_id);

        std::unique_lock<std::mutex> lock{mutex};

        Sessions::iterator session = sessions.find(query_id);
        if (session != sessions.end() && !session->second.empty())
        {
            LOG_TRACE(log, "Connection join to query_id: " << query_id);
            auto first = session->second.begin();
            auto execution = (*first)->getExecution();
            connection->setExecution(execution);
        }
        else
        {
            LOG_TRACE(log, "First connection in query_id: " << query_id);
            connection->startExecuting();
            sessions.emplace(query_id, Session());
            session = sessions.find(query_id);
        }

        session->second.insert(connection);

        return connection;
    }

    void clear(TCPArrowHandler * connection)
    {
        std::unique_lock<std::mutex> lock{mutex};

        auto query_id = connection->getQueryId();
        Sessions::iterator session = sessions.find(query_id);
        if (session == sessions.end())
        {
            auto msg = "Clear connection in query_id: " + query_id + " failed, session not found.";
            LOG_TRACE(log, msg);
            throw Exception(msg);
        }

        session->second.erase(connection);
        LOG_TRACE(log, "Clear connection in query_id: " << query_id << ", sessions: " <<
            sessions.size() << ", connections - 1: " << session->second.size());

        if (session->second.empty())
        {
            sessions.erase(session);
            LOG_TRACE(log, "Clear session query_id: " << query_id << ", sessions: " << sessions.size());
        }
    }

private:
    using Session = std::unordered_set<TCPArrowHandler *>;
    using Sessions = std::unordered_map<String, Session>;

    Poco::Logger * log;
    Sessions sessions;
    std::mutex mutex;
};

}
