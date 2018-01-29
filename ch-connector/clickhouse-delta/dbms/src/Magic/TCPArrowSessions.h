#pragma once

#include <sstream>
#include <mutex>
#include <condition_variable>
#include <ctime>

#include <common/logger_useful.h>

#include "TCPArrowHandler.h"

namespace Magic
{

// TODO: Move to config file
static size_t max_sessions_count = 32;
static size_t unfinished_session_expired_seconds = 60 * 60;
static size_t finished_session_expired_seconds = 2;

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

    Poco::Net::TCPServerConnection * create(DB::IServer & server, const Poco::Net::StreamSocket & socket)
    {
        auto connection = new TCPArrowHandler(server, socket);
        auto query_id = connection->getQueryId();
        auto client_index = connection->getClientIndex();
        auto client_count = connection->getClientCount();

        std::stringstream info_ss;
        info_ss << "query_id: " << query_id << ", client #" << client_index << "/" << client_count;
        std::string query_info = info_ss.str();

        LOG_TRACE(log, "TCP arrow connection established, " << query_info << ", " <<
            connection->getQuery() << ", address: " << socket.peerAddress().toString());

        std::unique_lock<std::mutex> lock{mutex};

        Sessions::iterator it = sessions.find(query_id);

        if (it != sessions.end())
        {
            Session & session = it->second;

            if (size_t(session.client_count) <= session.active_clients.size())
                throw DB::Exception("Join to session fail, too much clients: " + query_info);

            LOG_TRACE(log, "Connection join to " << query_info);

            if (!session.execution)
            {
                time_t now = time(0);
                auto seconds = difftime(now, session.create_time);
                if (seconds >= finished_session_expired_seconds && session.finished())
                {
                    LOG_WARNING(log, "Relaunch query found, clean and re-execute: " << query_info);
                    sessions.erase(it);
                    it = sessions.end();
                }
                else
                {
                    throw DB::Exception("Join to expired session, " + query_info);
                }
            }
            else
            {
                auto activation = session.active_clients.find(client_index);
                if (activation != session.active_clients.end())
                {
                    throw DB::Exception("Double join to running session, " + query_info +
                        ", previous is " + (activation->second ? "active" : "inactive"));
                }
                else
                {
                    connection->setExecution(session.execution);
                }
            }
        }

        if (it == sessions.end())
        {
            auto client_count = connection->getClientCount();
            LOG_TRACE(log, "First connection, " << query_info << ", query: " << connection->getQuery());

            connection->startExecuting();

            sessions.emplace(query_id, Session(client_count, connection->getExecution()));
            it = sessions.find(query_id);
        }

        it->second.active_clients.emplace(client_index, true);

        return connection;
    }

    void clear(TCPArrowHandler * connection)
    {
        std::unique_lock<std::mutex> lock{mutex};

        auto query_id = connection->getQueryId();
        auto client_index = connection->getClientIndex();
        Sessions::iterator it = sessions.find(query_id);

        if (it == sessions.end())
            throw DB::Exception("Clear connection in query_id: " + query_id + " failed, session not found.");

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
        if (session.finished())
        {
            LOG_TRACE(log, "Clear session query_id: " << query_id << ", sessions: " << sessions.size() <<
                ", execution ref: " << session.execution.use_count());
            session.active_clients.clear();
            session.execution = NULL;
        }

        // The further operation: clean up tombstones
        if (sessions.size() >= max_sessions_count)
        {
            time_t now = time(0);
            auto it = sessions.begin();
            while (it != sessions.end())
            {
                auto & session = it->second;
                auto seconds = difftime(now, session.create_time);
                if (!session.finished())
                {
                    if (seconds >= unfinished_session_expired_seconds)
                    {
                        LOG_TRACE(log, "Session expired, but not finished, active clients: " <<
                            session.active_clients.size() << ", execution ref: " << session.execution.use_count() <<
                            ". Force clean now.");
                        session.active_clients.clear();
                        // TODO: Force disconnect
                        session.execution->cancal(false);
                        session.execution = NULL;
                        it = sessions.erase(it);
                        continue;
                    }
                }
                else
                {
                    // Not clean it too fast, for relaunch query detecting.
                    if (seconds >= unfinished_session_expired_seconds)
                    {
                        LOG_TRACE(log, "Session expired, cleaning tombstone. query_id: " <<
                            it->first << ", created: " << seconds << "s.");
                        it = sessions.erase(it);
                        continue;
                    }
                }

                ++it;
            }
        }
    }

private:
    struct Session
    {
        DB::Int64 client_count;
        TCPArrowHandler::EncoderPtr execution;
        DB::Int64 finished_clients;
        std::unordered_map<Int64, bool> active_clients;
        time_t create_time;

        Session(Int64 client_count_, TCPArrowHandler::EncoderPtr execution_) :
            client_count(client_count_), execution(execution_), finished_clients(0), create_time(time(0))
        {
        }

        bool finished()
        {
            return finished_clients >= client_count;
        }
    };

    using Sessions = std::unordered_map<DB::String, Session>;

    Poco::Logger * log;
    Sessions sessions;
    std::mutex mutex;
};

}
