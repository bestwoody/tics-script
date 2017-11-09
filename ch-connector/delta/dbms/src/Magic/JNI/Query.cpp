#include "pingcap_com_MagicProto.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/ipc/writer.h"

JNIEXPORT jbyteArray JNICALL Java_pingcap_com_MagicProto_query(JNIEnv * env, jobject obj, jstring query)
{
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
