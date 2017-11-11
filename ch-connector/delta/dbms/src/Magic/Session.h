#include "pingcap_com_MagicProto.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/ipc/writer.h"

namespace Magic {


// TODO: handle all types
arrow::Type dataTypeToArrowType(DB::DataTypePtr & type)
{
    auto name = type->getName();
    if (name == "Int64")
        return arrow::Type::Int64;
    return arrow::Type::NA;
}


// TODO: handle all types
// TODO: faster copy
std::shared_ptr<Array> columnToArrowArray(DB::DataTypePtr & type, DB::ColumnPtr & column, size_t rows)
{
    auto name = type->getName();
    std::shared_ptr<arrow::Array> array;

    if (name == "Int64")
    {
        const auto & data = typeid_cast<DB::ColumnInt64 &>(*column);
        arrow::Int64Builder builder;
        for (size_t i = 0; i < rows; ++i)
        {
            status = builder.Append(data.getElement(i));
            if (!status.ok())
                return NULL;
        }
        status = builder.Finish(&array);
        if (!status.ok())
            return NULL;
    }

    return array;
}


// TODO: write error info to string
class Session
{
public:
    using SchemePtr = std::shared_ptr<arrow::Schema>;
    using Block = arrow::RecordBatch;

    Session(const DB::BlockIO & result_) : result(result_)
    {
        result.in.readPrefix();

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

    SchemePtr getSchema()
    {
        return schema;
    }

    BufferPtr getEncodedSchema()
    {
        std::shared_ptr<arrow::Buffer> serialized;
        auto pool = arrow::default_memory_pool();
        auto status = arrow::ipc::SerializeSchema(schema, pool, &serialized);
        if (!status.ok())
            return result;
        return serialized;
    }

    Block getBlock()
    {
        DB::Block block = result.in.read();
        std::vector<std::shared_ptr<Array>> arrays;
        ::arrow::Status status;

        for (size_t i = 0; i < block.columns(); ++i)
        {
            auto & column = result.in_sample.getByPosition(i);
            auto array = columnToArrowArray(column.type, column.column, block.rows());
            arrays.push_back(array);
        }

        arrow::RecordBatch batch(schema, block.rows(), arrays);
        return batch;
    }

    BufferPtr getEncodedBlock()
    {
        auto batch = getBlock();
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
