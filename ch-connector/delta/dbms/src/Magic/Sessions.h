#include <unordered_map>
#include "Context.h"
#include "Session.h"

namespace Magic {

class Sessions
{
public:
    Sessions() : id_gen(10000) {}

    void init(const char * config)
    {
        // Why Poco use non-const args?
        std::string flag = "--config-file";
        std::string arg = config;
        char * args[] = {(char *)flag.c_str(), (char *)arg.c_str()};
        app = std::make_shared<DB::Application>(2, args);
    }

    int64_t newSession(const char * query)
    {
        auto result = DB::executeQuery(query, app->context(), false);
        auto session = std::make_shared<Session>(result);
        auto token = id_gen++;
        // TODO: lock_guard
        sessions[token] = session;
        return token;
    }

    void closeSession(int64_t token)
    {
        // TODO: lock_guard
        sessions.erase(token);
    }

    std::shared_ptr<Session> getSession(int64_t token)
    {
        // TODO: lock_guard
        auto it = sessions.find(token);
        if (it == sessions.end())
            return NULL;
        return it->second;
    }

    void close()
    {
        // TODO: check all sessions are closed
        app->close();
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
};

}
