#pragma once

#include <sstream>
#include <mutex>
#include <condition_variable>

#include <ctime>
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

        std::stringstream info_ss;
        info_ss << "query_id: " << query_id << ", client #" << client_index << "/" << client_count;
        std::string query_info = info_ss.str();

        LOG_TRACE(log, "TCP arrow connection established, " << query_info);

        std::unique_lock<std::mutex> lock{mutex};

        Sessions::iterator session = sessions.find(query_id);
        if (session != sessions.end())
        {
            if (size_t(session->second.client_count) <= session->second.active_clients.size())
                throw Exception("Join to session fail, too much clients: " + query_info);
            LOG_TRACE(log, "Connection join to " << query_info);
            if (!session->second.execution)
                throw Exception("Join to expired session, " + query_info);
            connection->setExecution(session->second.execution);
        }
        else
        {
            auto client_count = connection->getClientCount();
            LOG_TRACE(log, "First connection, " << query_info << ", query: " << connection->getQuery());

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

        LOG_TRACE(log, "Connection done in query_id: " << query_id <<
            ", connections: " << session.client_count << "-" << session.finished_clients <<
            "=" << (session.client_count - session.finished_clients) <<
            ", client #" << client_index << "/" << session.client_count <<
            ", execution ref: " << session.execution.use_count());

        // Can't remove session immidiatly, may cause double running.
        // Leave a tombstone for further clean up
        if (session.finished_clients == session.client_count)
        {
            session.active_clients.clear();
            session.execution = NULL;
            LOG_TRACE(log, "Clear session query_id: " << query_id << ", sessions: " << sessions.size() <<
                ", execution ref: " << session.execution.use_count());
        }

        // TODO: move to config file
        static size_t max_sessions_count = 1024;
        static size_t session_expired_seconds = 60 * 60 * 24;

        // The further operation: clean up tombstones
        if (sessions.size() >= max_sessions_count)
        {
            time_t now = time(0);
            auto it = sessions.begin();
            while (it != sessions.end())
            {
                auto & session = it->second;
                auto seconds = difftime(now, it->second.create_time);
                if (seconds >= session_expired_seconds)
                {
                    LOG_TRACE(log, "Session expired, cleaning tombstone. query_id: " <<
                        it->first << ", created: " << seconds << "s.");
                    if (session.client_count != session.finished_clients)
                    {
                        LOG_TRACE(log, "Session expired, but not finished, active clients: " <<
                            session.active_clients.size() << ", execution ref: " << session.execution.use_count() <<
                            ". Force clean now.");
                        session.active_clients.clear();
                        // TODO: force disconnect
                        session.execution->cancal(false);
                        session.execution = NULL;
                    }
                    it = sessions.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }

private:
    struct Session
    {
        Int64 client_count;
        TCPArrowHandler::EncoderPtr execution;
        Int64 finished_clients;
        std::unordered_map<Int64, bool> active_clients;
        time_t create_time;

        Session(Int64 client_count_, TCPArrowHandler::EncoderPtr execution_) :
            client_count(client_count_), execution(execution_), finished_clients(0), create_time(time(0)) {}
    };

    using Sessions = std::unordered_map<String, Session>;

    Poco::Logger * log;
    Sessions sessions;
    std::mutex mutex;
};

}
