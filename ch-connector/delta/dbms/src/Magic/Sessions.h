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
        const char * args[] = {"--config-file", config};
        app = new Application(args);
    }

    int64_t newSession(const char * query)
    {
        // TODO: lock_guard
        auto result = DB::executeQuery(query, app.context(), false);
        auto session = std::make_shared<Session>(result);
        sessions[id_gen++] = session;
    }

    void closeSession(int64_t token)
    {
        // TODO: if .. app.cancel(...);
        // TODO: lock_guard
        sessions.erase(token);
    }

    Session * getSession(int64_t token)
    {
        // TODO: lock_guard
        auto it = sessions.find(token);
        return (it == sessions.end()) ? NULL : (&*it);
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
