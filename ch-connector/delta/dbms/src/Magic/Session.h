#include "pingcap_com_MagicProto.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/ipc/writer.h"

namespace Magic {

// TODO: write error info to string
class Sessions
{
public:
    class Session
    {
    public:
        Session(const BlockIO & result_) : result(result_)
        {
        }

        BufferPtr getEncodedSchema()
        {
            // TODO: session.result.sample => arrow encoded schema

            auto f0 = arrow::field("f0", array->type());
            std::vector<std::shared_ptr<arrow::Field>> fields = {f0};
            auto schema = std::make_shared<arrow::Schema>(fields);

            std::shared_ptr<arrow::Buffer> serialized;
            auto pool = arrow::default_memory_pool();
            auto status = arrow::ipc::SerializeSchema(schema, pool, &serialized);
            if (!status.ok())
                return result;
            return serialized;
        }

        BufferPtr getEncodedBlock()
        {
            // TODO: session.in => arrow encoded block

            ::arrow::Status status;

            arrow::Int64Builder builder;
            for (size_t i = 0; i < 10; i++) {
                status = builder.Append(i);
                if (!status.ok())
                    return result;
            }
            std::shared_ptr<arrow::Array> array;
            status = builder.Finish(&array);
            if (!status.ok())
                return NULL;

            arrow::RecordBatch batch(schema, 0, {array});
            std::shared_ptr<arrow::Buffer> serialized;
            auto pool = arrow::default_memory_pool();
            status = arrow::ipc::SerializeRecordBatch(batch, pool, &serialized);
            if (!status.ok())
                return NULL;
            return serialized;
        }

        const std::string & getErrorString()
        {
            return error;
        }

    private:
        BlockIO result;
        std::string error;
    };

    using BufferPtr = std::shared_ptr<arrow::Buffer>;

    Sessions() : id_gen(10000)
    {
    }

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
