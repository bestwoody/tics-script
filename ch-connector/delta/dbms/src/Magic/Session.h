#include "pingcap_com_MagicProto.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/ipc/writer.h"

namespace Magic
{

class Session
{
public:
    using SchemaPtr = std::shared_ptr<arrow::Schema>;
    using BlockPtr = std::shared_ptr<arrow::RecordBatch>;
    using BufferPtr = std::shared_ptr<arrow::Buffer>;

    Session(const std::string & error_) : error(error_) {}

    Session(const DB::BlockIO & result_) : result(result_)
    {
        try
        {
            result.in->readPrefix();

            std::vector<std::shared_ptr<arrow::Field>> fields;
            for (size_t i = 0; i < result.in_sample.columns(); ++i)
            {
                auto & column = result.in_sample.getByPosition(i);
                auto type = dataTypeToArrowType(column.type);
                auto field = arrow::field(column.name, type, column.type->isNullable());
                fields.push_back(field);
            }
            schema = std::make_shared<arrow::Schema>(fields);
        }
        catch (const DB::Exception & e)
        {
            error = e.displayText();
        }
    }

    SchemaPtr getSchema()
    {
        return schema;
    }

    BufferPtr getEncodedSchema()
    {
        std::shared_ptr<arrow::Buffer> serialized;
        auto pool = arrow::default_memory_pool();
        auto status = arrow::ipc::SerializeSchema(*schema, pool, &serialized);
        if (!status.ok())
        {
            error = "arrow::ipc::SerializeSchema " + status.ToString();
            return NULL;
        }
        return serialized;
    }

    BlockPtr getBlock()
    {
        try
        {
            DB::Block block = result.in->read();
            if (!block)
                return NULL;
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            arrow::Status status;

            for (size_t i = 0; i < block.columns(); ++i)
            {
                auto & column = result.in_sample.getByPosition(i);
                auto array = columnToArrowArray(column.type, column.column, block.rows());
                arrays.push_back(array);
            }

            return std::make_shared<arrow::RecordBatch>(schema, block.rows(), arrays);
        }
        catch (const DB::Exception & e)
        {
            error = e.displayText();
        }

        return NULL;
    }

    BufferPtr getEncodedBlock()
    {
        auto block = getBlock();
        if (!block)
            return NULL;
        std::shared_ptr<arrow::Buffer> serialized;
        auto pool = arrow::default_memory_pool();
        auto status = arrow::ipc::SerializeRecordBatch(*block, pool, &serialized);
        if (!status.ok())
        {
            error = "arrow::ipc::SerializeRecordBatch " + status.ToString();
            return NULL;
        }
        return serialized;
    }

    const std::string & getErrorString()
    {
        return error;
    }

private:
    // TODO: faster copy
    // TODO: handle all types
    std::shared_ptr<arrow::Array> columnToArrowArray(DB::DataTypePtr & type, DB::ColumnPtr & column, size_t rows)
    {
        auto name = type->getName();
        std::shared_ptr<arrow::Array> array;
        arrow::Status status;

        if (name == "Int64")
        {
            const auto & data = typeid_cast<DB::ColumnInt64 &>(*column);
            arrow::Int64Builder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    error = "arrow::Int64Builder.Append " + status.ToString();
                    return NULL;
                }
            }
            status = builder.Finish(&array);
            if (!status.ok())
            {
                error = "arrow::Int64Builder.Finish" + status.ToString();
                return NULL;
            }
        }

        return array;
    }

    // TODO: handle all types
    static std::shared_ptr<arrow::DataType> dataTypeToArrowType(DB::DataTypePtr & type)
    {
        auto name = type->getName();
        if (name == "Int64")
            return arrow::int64();
        return arrow::null();
    }

private:
    DB::BlockIO result;
    std::string error;
    SchemaPtr schema;
};

}
