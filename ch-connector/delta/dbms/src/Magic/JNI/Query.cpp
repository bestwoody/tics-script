#include "pingcap_com_MagicProto.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/ipc/writer.h"

namespace Magic {

class Sessions
{
public:
    class Session
    {
    };

    Sessions() : id_gen(10000)
    {
    }

    void init(const char * config)
    {
        // TODO
        // config => args
        // app = new Application(args)
    }

    int64_t newSession(const char * query)
    {
        // TODO
        // auto result = DB::executeQuery(query, app.context(), false);
        // auto session = make_share<Session>(result)
        // lock_guard
        // sessions[id_gen++] = session
    }

    void closeSession(int64_t token)
    {
        // TODO
        // sessions.remove(token);
        // if .. app.cancel(...);
    }

    void close()
    {
        // TODO
        // close app
        // check all sessions are closed
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

JNIEXPORT void JNICALL Java_pingcap_com_MagicProto_init(JNIEnv * env, jobject obj, jstring config)
{
    auto sessions = Magic::Sessions::global();
    sessions->init();
}

JNIEXPORT void JNICALL Java_pingcap_com_MagicProto_finish(JNIEnv * env, jobject obj)
{
    sessions->close();
}

JNIEXPORT jlong JNICALL Java_pingcap_com_MagicProto_query(JNIEnv * env, jobject obj, jstring query)
{
    auto query_string = env->GetStringUTFChars(query, false);
    if (!query_string)
        return -1;
    auto sessions = Magic::Sessions::global();
    // TODO: auto token = sessions->newSession(query);
    env->ReleaseStringUTFChars(query_string, str);
    return token;
}

JNIEXPORT void JNICALL Java_pingcap_com_MagicProto_close(JNIEnv * env, jobject obj, jlong token)
{
    auto sessions = Magic::Sessions::global();
    // TODO: sessions->closeSession(token);
}

JNIEXPORT jbyteArray JNICALL Java_pingcap_com_MagicProto_next(JNIEnv * env, jobject obj, jlong token)
{
    auto sessions = Magic::Sessions::global();
    // TODO
    // auto session = sessions->session(token);
    // session.result => arrow encoding => result

    // test mock below:

    // TODO: test: return java nil
    jbyteArray result = 0;
    ::arrow::Status status;

    // Create array
    arrow::Int64Builder builder;
    for (size_t i = 0; i < 10; i++) {
        status = builder.Append(i);
        if (!status.ok())
            return result;
    }
    std::shared_ptr<arrow::Array> array;
    status = builder.Finish(&array);
    if (!status.ok())
        return result;

    // Create schema
    auto f0 = arrow::field("f0", array->type());
    std::vector<std::shared_ptr<arrow::Field>> fields = {f0};
    auto schema = std::make_shared<arrow::Schema>(fields);

    // Create batch
    arrow::RecordBatch batch(schema, 0, {array});
    std::shared_ptr<arrow::Buffer> serialized;
    auto pool = arrow::default_memory_pool();
    status = arrow::ipc::SerializeRecordBatch(batch, pool, &serialized);
    if (!status.ok())
        return result;
    auto cb = serialized->size();
    uint8_t *data = (uint8_t*)serialized->data();

    void *buf = malloc(cb);
    result = env->NewByteArray(cb);
    env->SetByteArrayRegion(result, 0, cb, (jbyte*)data);
    free(buf);
    return result;
}

JNIEXPORT jbyteArray JNICALL Java_pingcap_com_MagicProto_schema(JNIEnv * env, jobject obj, jlong token)
{
    auto sessions = Magic::Sessions::global();
    // TODO
    // auto session = sessions->session(token);
    // session.result.sample => arrow encoding => result

    jbyteArray result = 0;
    return result;
}
