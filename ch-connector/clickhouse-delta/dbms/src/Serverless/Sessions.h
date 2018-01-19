#pragma once

#include <unordered_map>
#include <mutex>

#include "Context.h"
#include "ArrowEncoder.h"

namespace Magic {

class Sessions
{
public:
    using Session = ArrowEncoder;

    Sessions() : id_gen(10000)
    {
    }

    std::string init(const char * config)
    {
        std::string error;
        try
        {
            app = std::make_shared<DB::Application>(config);
        }
        catch (const DB::Exception & e)
        {
            error = e.displayText();
        }
        return error;
    }

    struct QueryResult
    {
        std::string error;
        long token;
    };
    QueryResult newSession(const char * query)
    {
        std::shared_ptr<Session> session;

        try
        {
            auto result = DB::executeQuery(query, app->context(), false);
            session = std::make_shared<Session>(result);
            std::unique_lock<std::mutex> lock{mtx};
            auto token = id_gen++;
            sessions[token] = session;
            return QueryResult{"", token};
        }
        catch (const DB::Exception & e)
        {
            return QueryResult{e.displayText(), -1};
        }
    }

    void closeSession(int64_t token)
    {
        std::unique_lock<std::mutex> lock{mtx};
        sessions.erase(token);
    }

    std::shared_ptr<Session> getSession(int64_t token)
    {
        std::unique_lock<std::mutex> lock{mtx};
        auto it = sessions.find(token);
        if (it == sessions.end())
            return NULL;
        return it->second;
    }

    std::string close()
    {
        // TODO: Check all sessions are closed
        std::unique_lock<std::mutex> lock{mtx};
        if (!app)
            return "app instance is null";
        std::string error;
        try
        {
            app->close();
        }
        catch (const DB::Exception & e)
        {
            error = e.displayText();
        }
        app = NULL;
        return error;
    }

    static Sessions* global()
    {
        static Sessions instance;
        return &instance;
    }

private:
    std::shared_ptr<DB::Application> app;
    std::unordered_map<int64_t, std::shared_ptr<Session>> sessions;
    long id_gen;
    mutable std::mutex mtx;
};

}
