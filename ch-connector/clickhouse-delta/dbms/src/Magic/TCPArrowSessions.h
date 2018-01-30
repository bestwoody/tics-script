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

            if (size_t(session.client_count) <= session.connected_clients)
            {
                conn->onException(session.str() + ". Join to session fail, too many clients.");
                return conn;
            }

            if (!session.execution)
            {
                time_t now = time(0);
                auto seconds = difftime(now, session.create_time);
                if (session.finished())
                {
                    if (seconds >= finished_session_expired_seconds)
                    {
                        LOG_WARNING(log, conn_info << ". Relaunch query found, re-run.");
                        sessions.erase(it);
                        it = sessions.end();
                    }
                    else
                    {
                        conn->onException(session.str() + ". Relaunch query found, aborting.");
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

            sessions.emplace(query_id, Session(client_count, conn->getExecution()));
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

        LOG_TRACE(log, conn_info << ". Connection done.");

        // Can't remove session immidiatly, may cause double running.
        // Leave a tombstone for further clean up
        if (session.finished())
        {
            LOG_TRACE(log, conn_info << ". Clear session. sessions: " << sessions.size());
            session.clients.clear();
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
                        session.clients.clear();
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

        Session(size_t client_count_, TCPArrowHandler::EncoderPtr execution_) :
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
        }

        bool finished()
        {
            return finished_clients >= client_count;
        }

        std::string str()
        {
            std::stringstream ss;
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
