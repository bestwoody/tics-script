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

    TCPArrowSessions() : log(&Logger::get("TCPArrowSession"))
    {
    }

    Poco::Net::TCPServerConnection * create(DB::IServer & server, const Poco::Net::StreamSocket & socket)
    {
        auto conn = new TCPArrowHandler(server, socket);
        auto query_id = conn->getQueryId();
        auto client_index = conn->getClientIndex();

        std::string conn_info = conn->toStr();

        LOG_TRACE(log, conn_info << ", " << socket.peerAddress().toString() <<
            " connected. " << conn->getQuery());

        std::unique_lock<std::mutex> lock{mutex};

        Sessions::iterator it = sessions.find(query_id);

        if (it != sessions.end())
        {
            Session & session = it->second;

            if (size_t(session.client_count) <= session.active_clients.size())
            {
                conn->onException("Join to session fail, too many clients.");
                return conn;
            }

            if (!session.execution)
            {
                time_t now = time(0);
                auto seconds = difftime(now, session.create_time);
                if (seconds >= finished_session_expired_seconds && session.finished())
                {
                    LOG_WARNING(log, conn_info << ". Relaunch query found, clean and re-execute.");
                    sessions.erase(it);
                    it = sessions.end();
                }
                else
                {
                    conn->onException("Join failed, too many clients.");
                    return conn;
                }
            }
            else
            {
                auto activation = session.active_clients.find(client_index);
                if (activation != session.active_clients.end())
                {
                    conn->onException(". Session: " + session.str() +
                        ". Double join, prev: [" + (activation->second ? "+" : "-") + "]");
                    return conn;
                }
                else
                {
                    conn->setExecution(session.execution);
                    LOG_TRACE(log, conn_info << ". Session: " << session.str() << ". Connection joint.");
                }
            }
        }

        if (it == sessions.end())
        {
            auto client_count = conn->getClientCount();
            LOG_TRACE(log, conn_info << ". First connection, sessions: " << sessions.size());

            conn->startExecuting();

            sessions.emplace(query_id, Session(client_count, conn->getExecution()));
            it = sessions.find(query_id);
        }

        it->second.active_clients.emplace(client_index, true);

        return conn;
    }

    void clear(TCPArrowHandler * conn)
    {
        std::unique_lock<std::mutex> lock{mutex};

        auto query_id = conn->getQueryId();
        auto client_index = conn->getClientIndex();
        std::string conn_info = conn->toStr();

        Sessions::iterator it = sessions.find(query_id);

        if (it == sessions.end())
        {
            LOG_ERROR(log, conn_info << ". Clear connection failed, session not found.");
            return;
        }

        Session & session = it->second;
        session.finish(client_index);

        conn_info += ". Session: " + session.str();

        LOG_TRACE(log, conn_info << ". Connection done.");

        // Can't remove session immidiatly, may cause double running.
        // Leave a tombstone for further clean up
        if (session.finished())
        {
            LOG_TRACE(log, conn_info << ". Clear session. sessions: " << sessions.size());
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
                        LOG_WARNING(log, conn_info << ". Session expired but not finished, force clean now.");
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
                        LOG_TRACE(log, conn_info << ". Session expired, cleaning tombstone.");
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

        // TODO: Use vector instead of map.
        std::unordered_map<Int64, bool> active_clients;

        time_t create_time;

        Session(Int64 client_count_, TCPArrowHandler::EncoderPtr execution_) :
            client_count(client_count_), execution(execution_), finished_clients(0), create_time(time(0))
        {
        }

        void finish(DB::Int64 client_index)
        {
            active_clients[client_index] = false;
            finished_clients += 1;
        }

        bool finished()
        {
            return finished_clients >= client_count;
        }

        std::string str()
        {
            std::stringstream ss;
            ss << finished_clients << "/" << client_count << " [";

            std::vector<DB::UInt8> flags(client_count, 0);
            for (auto it = active_clients.begin(); it != active_clients.end(); ++it)
                flags[it->first] = (it->second ? 1 : 2);
            for (auto it = flags.begin(); it != flags.end(); ++it)
                ss << ((*it == 0) ? "-" : ((*it == 1) ? "+" : "*"));

            time_t now = time(0);
            ss << "] lived " << difftime(now, create_time) << " s";
            return ss.str();
        }
    };

    using Sessions = std::unordered_map<DB::String, Session>;

    Poco::Logger * log;
    Sessions sessions;
    std::mutex mutex;
};

}
