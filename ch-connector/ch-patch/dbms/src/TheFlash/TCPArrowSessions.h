#pragma once

#include <sstream>
#include <mutex>
#include <condition_variable>
#include <ctime>

#include <common/logger_useful.h>

#include "TCPArrowHandler.h"

namespace TheFlash
{

// TODO: Move to config file
static size_t max_sessions_count = 32;
static size_t unfinished_session_expired_seconds = 60 * 60;
static size_t finished_session_expired_seconds = 3;

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

    // TODO: Keep the connection failed in initing, until all connections in the same session are failed.
    // TODO: Session corrupted flag
    Poco::Net::TCPServerConnection * create(DB::IServer & server, const Poco::Net::StreamSocket & socket)
    {
        auto conn = new TCPArrowHandler(server, socket);
        auto query_id = conn->getQueryId();
        auto client_index = conn->getClientIndex();

        std::string conn_info = conn->toStr();

        if (conn->isFailed())
        {
            if (query_id.empty())
                LOG_ERROR(log, conn_info << ". Receivd a broken connection, can't find it's session.");
            else
                LOG_ERROR(log, conn_info << ". Receivd a broken connection.");
            return conn;
        }

        LOG_TRACE(log, conn_info << ", " << socket.peerAddress().toString() << " connected. " << conn->getQuery());

        std::unique_lock<std::mutex> lock{mutex};

        Sessions::iterator it = sessions.find(query_id);

        if (it != sessions.end())
        {
            Session & session = it->second;

            if (!session.execution)
            {
                time_t now = time(0);
                auto seconds = difftime(now, session.create_time);
                if (session.finished())
                {
                    if (seconds >= finished_session_expired_seconds)
                    {
                        LOG_WARNING(log, conn_info << ". Old: " << session.str() << ". Relaunch query found, re-run.");
                        sessions.erase(it);
                        it = sessions.end();
                    }
                    else
                    {
                        conn->onException("Old: " + session.str() + ". Relaunch query found, abort.");
                        return conn;
                    }
                }
                else
                {
                    conn->onException(session.str() + ". Join failed, session unfinished but no execution.");
                    return conn;
                }
            }
            else
            {
                auto conn_status = session.clients[client_index];
                if (conn_status != Session::ConnUnconnect)
                {
                    conn->onException("Session: " + session.str() +
                        ". Double join, prev: [" + Session::connStatusStr(conn_status) + "]");
                    return conn;
                }
                else
                {
                    conn->setExecution(session.execution);
                    LOG_TRACE(log, conn->toStr() << ". Session: " << session.str() << ". Connection joined.");
                }
            }
        }

        if (it == sessions.end())
        {
            auto client_count = conn->getClientCount();
            LOG_TRACE(log, conn_info << ". First connection, sessions: " << sessions.size());

            conn->startExecuting();

            sessions.emplace(query_id, Session(query_id, client_count, conn->getExecution()));
            it = sessions.find(query_id);
        }

        it->second.connected(client_index);

        return conn;
    }

    void clear(TCPArrowHandler * conn, bool failed)
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
        if (failed)
            session.failed(client_index);
        else
            session.done(client_index);

        conn_info += ". Session: " + session.str();

        if (session.finished())
        {
            if (session.client_count == 1)
            {
                // Fast clean up for one client session
                LOG_TRACE(log, conn_info << ". Connection done, session cleared. sessions: " << sessions.size());
                sessions.erase(it);
                return;
            }
            else
            {
                // Can't remove session immidiatly, may cause double running.
                // Leave a tombstone for further clean up
                LOG_TRACE(log, conn_info << ". Connection done, session=>tombstone. sessions: " << sessions.size());
                session.execution = NULL;
            }
        }
        else
        {
            LOG_TRACE(log, conn_info << ". Connection done.");
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
                        LOG_WARNING(log, session.str(true) << ". Session expired but not finished, force clean now.");
                        // TODO: Force disconnect
                        session.execution->cancal(false);
                        it = sessions.erase(it);
                        continue;
                    }
                }
                else
                {
                    // Not clean it too fast, for relaunch query detecting.
                    if (seconds >= unfinished_session_expired_seconds)
                    {
                        LOG_TRACE(log, session.str(true) << ". Session expired, cleaning tombstone.");
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
        std::string query_id;
        size_t client_count;
        TCPArrowHandler::EncoderPtr execution;
        size_t finished_clients;
        size_t connected_clients;

        std::vector<DB::UInt8> clients;

        time_t create_time;

        enum
        {
            ConnUnconnect = DB::UInt8(0),
            ConnConnected = DB::UInt8(1),
            ConnFinished = DB::UInt8(2),
            ConnFailed = DB::UInt8(3),
        };

        Session(const std::string & query_id_, size_t client_count_, TCPArrowHandler::EncoderPtr execution_) :
            query_id(query_id_),
            client_count(client_count_),
            execution(execution_),
            finished_clients(0),
            connected_clients(0),
            clients(client_count_, ConnUnconnect),
            create_time(time(0))
        {
        }

        void connected(size_t client_index)
        {
            clients[client_index] = ConnConnected;
            connected_clients += 1;
        }

        void done(size_t client_index)
        {
            clients[client_index] = ConnFinished;
            finished_clients += 1;
        }

        void failed(size_t client_index)
        {
            clients[client_index] = ConnFailed;
            finished_clients += 1;
        }

        bool finished()
        {
            return finished_clients >= client_count;
        }

        std::string str(bool with_conn_info = false)
        {
            std::stringstream ss;

            if (with_conn_info)
                ss << query_id << " r" << ((bool)execution ? "+" : "-") << execution.use_count() << ". ";

            ss << finished_clients << "/" << client_count << " [";

            for (auto it = clients.begin(); it != clients.end(); ++it)
                ss << connStatusStr(*it);

            time_t now = time(0);
            ss << "] lived " << difftime(now, create_time) << "s";
            return ss.str();
        }

        static std::string connStatusStr(DB::UInt8 status)
        {
            switch (status)
            {
                case ConnUnconnect:
                    return "-";
                case ConnConnected:
                    return "+";
                case ConnFailed:
                    return "|";
                case ConnFinished:
                    return "*";
            }
            return "?";
        }
    };

    using Sessions = std::unordered_map<DB::String, Session>;

    Poco::Logger * log;
    Sessions sessions;
    std::mutex mutex;
};

}
