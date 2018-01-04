#pragma once

#include <mutex>
#include <condition_variable>

#include <Common/typeid_cast.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/ipc/writer.h"

#include "SafeBlockIO.h"


namespace Magic
{

class ArrowEncoder
{
public:
    using SchemaPtr = std::shared_ptr<arrow::Schema>;
    using BlockPtr = std::shared_ptr<arrow::RecordBatch>;
    using BufferPtr = std::shared_ptr<arrow::Buffer>;

    ArrowEncoder(const std::string & error_)
    {
        std::unique_lock<std::mutex> lock{mutex};
        error = error_;
    }

    ArrowEncoder(DB::BlockIO & input_)
    {
        std::unique_lock<std::mutex> lock{mutex};
        input = std::make_shared<SafeBlockIO>(input_);
        schema = getSchema();
        encodedSchema = encodeSchema(schema);
    }

    BufferPtr getEncodedSchema()
    {
        std::unique_lock<std::mutex> lock{mutex};
        return encodedSchema;
    }

    inline DB::Block getOriginBlock()
    {
        std::unique_lock<std::mutex> lock{mutex};
        return input->read();
    }

    BlockPtr encodeBlock(DB::Block block)
    {
        try
        {
            if (!block)
                return NULL;

            std::vector<std::shared_ptr<arrow::Array>> arrays;
            arrow::Status status;

            size_t rows = 0;
            for (size_t i = 0; i < block.columns(); ++i)
            {
                auto & column = block.getByPosition(i);
                rows += column.column->size();
                auto array = columnToArrowArray(column.type, column.column);
                if (!array)
                    return NULL;
                arrays.push_back(array);
            }

            return std::make_shared<arrow::RecordBatch>(schema, rows, arrays);
        }
        catch (const DB::Exception & e)
        {
            onError(e.displayText());
        }

        return NULL;
    }

    BufferPtr serializedBlock(BlockPtr block)
    {
        if (!block)
            return NULL;
        std::shared_ptr<arrow::Buffer> serialized;
        auto pool = arrow::default_memory_pool();
        auto status = arrow::ipc::SerializeRecordBatch(*block, pool, &serialized);
        if (!status.ok())
        {
            onError("arrow::ipc::SerializeRecordBatch " + status.ToString());
            return NULL;
        }
        // std::cerr << "PROBE pool alloced: " << pool->bytes_allocated() << ", peak: " << pool->max_memory() << std::endl;
        return serialized;
    }

    inline bool hasError()
    {
        std::unique_lock<std::mutex> lock{mutex};
        return !error.empty();
    }

    std::string getErrorString()
    {
        std::unique_lock<std::mutex> lock{mutex};
        return error;
    }

    size_t blocks()
    {
        std::unique_lock<std::mutex> lock{mutex};
        return input->blocks();
    }

    void cancal(bool exception = false)
    {
        std::unique_lock<std::mutex> lock{mutex};
        if (input)
            input->cancal(exception);
        input = NULL;
    }

protected:
    void onError(const std::string msg)
    {
        std::unique_lock<std::mutex> lock{mutex};
        error = msg;
        input->cancal(true);
        input = NULL;
    }

private:
    SchemaPtr getSchema()
    {
        try
        {
            auto sample = input->sample();
            std::vector<std::shared_ptr<arrow::Field>> fields;
            for (size_t i = 0; i < sample.columns(); ++i)
            {
                auto & column = sample.getByPosition(i);
                auto type = dataTypeToArrowType(column.type);
                auto field = arrow::field(column.name, type, column.type->isNullable());
                fields.push_back(field);
            }
            return std::make_shared<arrow::Schema>(fields);
        }
        catch (const DB::Exception & e)
        {
            onError(e.displayText());
        }
        return NULL;
    }

    BufferPtr encodeSchema(SchemaPtr schema)
    {
        if (!schema)
            return NULL;

        std::shared_ptr<arrow::Buffer> serialized;
        auto pool = arrow::default_memory_pool();
        auto status = arrow::ipc::SerializeSchema(*schema, pool, &serialized);
        if (!status.ok())
        {
            onError("arrow::ipc::SerializeSchema " + status.ToString());
            return NULL;
        }
        // std::cerr << "PROBE pool alloced: " << pool->bytes_allocated() << ", peak: " << pool->max_memory() << std::endl;
        return serialized;
    }

private:
    // TODO: faster copy
    // TODO: handle all types
    // TODO: use template + trait for faster and cleaner code
    // TODO: check by id, not by name
    // TODO: only fetch types once
    std::shared_ptr<arrow::Array> columnToArrowArray(DB::DataTypePtr & type, DB::ColumnPtr & column)
    {
        auto pool = arrow::default_memory_pool();
        auto name = type->getName();
        std::shared_ptr<arrow::Array> array;
        arrow::Status status;

        if (name == "String")
        {
            arrow::StringBuilder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnString *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                auto ref = data->getDataAt(i);
                auto val = ref.toString();
                status = builder.Append(val);
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::StringBuilder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (std::string(type->getFamilyName()) == "FixedString")
        {
            arrow::StringBuilder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnFixedString *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                auto ref = data->getDataAt(i);
                auto val = ref.toString();
                status = builder.Append(val);
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::StringBuilder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }

        else if (name == "DateTime")
        {
            arrow::Time64Builder builder(arrow::time64(arrow::TimeUnit::NANO), pool);
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnUInt32 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                uint32_t val = data->getElement(i);
                status = builder.Append(val);
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::Time64Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Date")
        {
            arrow::Time64Builder builder(arrow::time64(arrow::TimeUnit::NANO), pool);
            static uint32_t date_to_sec = 24 * 60 * 60;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnUInt16 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                uint16_t val = data->getElement(i);
                status = builder.Append(date_to_sec * val);
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::Time64Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Int8")
        {
            arrow::Int8Builder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnInt8 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data->getElement(i));
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::Int8Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Int16")
        {
            arrow::Int16Builder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnInt16 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data->getElement(i));
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::Int16Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Int32")
        {
            arrow::Int32Builder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnInt32 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data->getElement(i));
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::Int32Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Int64")
        {
            arrow::Int64Builder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnInt64 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data->getElement(i));
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::Int64Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "UInt8")
        {
            arrow::UInt8Builder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnUInt8 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data->getElement(i));
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::UInt8Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "UInt16")
        {
            arrow::UInt16Builder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnUInt16 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data->getElement(i));
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::UInt16Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "UInt32")
        {
            arrow::UInt32Builder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnUInt32 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data->getElement(i));
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::UInt32Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "UInt64")
        {
            arrow::UInt64Builder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnUInt64 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data->getElement(i));
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::UInt64Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Float32")
        {
            arrow::FloatBuilder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnFloat32 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data->getElement(i));
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::FloatBuilder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Float64")
        {
            arrow::DoubleBuilder builder;
            auto rows = column->size();
            builder.Reserve(rows);
            const auto data = typeid_cast<const DB::ColumnFloat64 *>(column.get());
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data->getElement(i));
                if (!status.ok())
                {
                    onError("columnToArrowArray, arrow::DoubleBuilder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else
        {
            onError(std::string("columnToArrowArray failed, unhandled type name: ") + name);
            return NULL;
        }

        if (!status.ok() || array == NULL)
        {
            onError("columnToArrowArray unhandled error, status: " + status.ToString());
            return NULL;
        }
        return array;
    }

    // TODO: handle all types
    // TODO: check by id, not by name
    static std::shared_ptr<arrow::DataType> dataTypeToArrowType(DB::DataTypePtr & type)
    {
        auto name = type->getName();
        if (name == "String")
            return arrow::utf8();
        else if (std::string(type->getFamilyName()) == "FixedString")
            return arrow::utf8();
        else if (name == "DateTime")
            return arrow::time64(arrow::TimeUnit::NANO);
        else if (name == "Date")
            return arrow::time64(arrow::TimeUnit::NANO);
        else if (name == "Int8")
            return arrow::int8();
        else if (name == "Int16")
            return arrow::int16();
        else if (name == "Int32")
            return arrow::int32();
        else if (name == "Int64")
            return arrow::int64();
        else if (name == "UInt8")
            return arrow::uint8();
        else if (name == "UInt16")
            return arrow::uint16();
        else if (name == "UInt32")
            return arrow::uint32();
        else if (name == "UInt64")
            return arrow::uint64();
        else if (name == "Float32")
            return arrow::float32();
        else if (name == "Float64")
            return arrow::float64();
        else
            throw DB::Exception(std::string("dataTypeToArrowType failed: ") + name);
        return arrow::null();
    }

private:
    std::shared_ptr<SafeBlockIO> input;
    SchemaPtr schema;
    BufferPtr encodedSchema;

    std::string error;

    std::mutex mutex;
};

}
