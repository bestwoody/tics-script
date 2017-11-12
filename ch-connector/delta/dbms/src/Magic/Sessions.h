#include <unordered_map>
#include <mutex>

#include "Context.h"
#include "Session.h"

namespace Magic {

class Sessions
{
public:
    Sessions() : id_gen(10000) {}

    void init(const char * config)
    {
        app = std::make_shared<DB::Application>(config);
    }

    int64_t newSession(const char * query)
    {
        std::shared_ptr<Session> session;

        try
        {
            auto result = DB::executeQuery(query, app->context(), false);
            session = std::make_shared<Session>(result);
            std::unique_lock<std::mutex> lock{mtx};
            auto token = id_gen++;
            sessions[token] = session;
            return token;
        }
        catch (const DB::Exception & e)
        {
            // TODO: error string
            std::cerr << "libch new session: " << e.displayText() << std::endl;
            return -1;
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

    void close()
    {
        // TODO: check all sessions are closed
        std::unique_lock<std::mutex> lock{mtx};
        if (!app)
            return;
        app->close();
        app = NULL;
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
