#include "pingcap_com_MagicProto.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/ipc/writer.h"

namespace Magic {

// TODO: handle all types
arrow::Type dataTypeToArrowType(DataTypePtr & type)
{
    auto name = type->getName();
    if (name == "Int64")
        return arrow::Type::Int64;
    return arrow::Type::NA;
}

// TODO: write error info to string
class Session
{
public:
    using SchemePtr = std::shared_ptr<arrow::Schema>;

    Session(const BlockIO & result_) : result(result_) {}

    SchemePtr getSchema()
    {
        if (!schema)
        {
            std::vector<std::shared_ptr<arrow::Field>> fields;
            for (size_t i = 0; i < result.in_sample.columns(); ++i)
            {
                auto & column = result.in_sample.getByPosition(i);
                auto type = arrow::DataType(dataTypeToArrowType(column.type));
                auto field = arrow::field(column.name, type, column.type.isNullable());
                fields.push_back(field);
            }
            schema = std::make_shared<arrow::Schema>(fields);
        }
        return schema;
    }

    BufferPtr getEncodedSchema()
    {
        std::shared_ptr<arrow::Buffer> serialized;
        auto pool = arrow::default_memory_pool();
        auto status = arrow::ipc::SerializeSchema(getSchema(), pool, &serialized);
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
}
