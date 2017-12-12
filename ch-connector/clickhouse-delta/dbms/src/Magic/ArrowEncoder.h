#pragma once

#include <condition_variable>
#include <mutex>

#include <Common/typeid_cast.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataStreams/IBlockOutputStream.h>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/ipc/writer.h"

namespace Magic
{

// TODO: handle cancallation
class ArrowEncoder
{
public:
    using SchemaPtr = std::shared_ptr<arrow::Schema>;
    using BlockPtr = std::shared_ptr<arrow::RecordBatch>;
    using BufferPtr = std::shared_ptr<arrow::Buffer>;

    ArrowEncoder(const std::string & error_) : error(error_) {}

    ArrowEncoder(const DB::BlockIO & result_) : result(result_)
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
            setError(e.displayText());
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
            setError("arrow::ipc::SerializeSchema " + status.ToString());
            return NULL;
        }
        return serialized;
    }

    BlockPtr getBlock()
    {
        try
        {
            DB::Block block = result.in->read();
            if (!block) {
                result.in->readSuffix();
                result.onFinish();
                return NULL;
            }
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            arrow::Status status;

            for (size_t i = 0; i < block.columns(); ++i)
            {
                auto & column = block.getByPosition(i);
                auto array = columnToArrowArray(column.type, column.column, block.rows());
                arrays.push_back(array);
            }

            return std::make_shared<arrow::RecordBatch>(schema, block.rows(), arrays);
        }
        catch (const DB::Exception & e)
        {
            setError(e.displayText());
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
            setError("arrow::ipc::SerializeRecordBatch " + status.ToString());
            return NULL;
        }
        return serialized;
    }

    bool hasError()
    {
        std::unique_lock<std::mutex> lock{mutex};
        return !error.empty();
    }

    std::string getErrorString()
    {
        std::unique_lock<std::mutex> lock{mutex};
        return error;
    }

protected:
    void setError(const std::string msg)
    {
        std::unique_lock<std::mutex> lock{mutex};
        error = msg;
    }


private:
    // TODO: faster copy
    // TODO: handle all types
    // TODO: use template + trait for faster and cleaner code
    // TODO: check by id, not by name
    std::shared_ptr<arrow::Array> columnToArrowArray(DB::DataTypePtr & type, DB::ColumnPtr & column, size_t rows)
    {
        auto pool = arrow::default_memory_pool();
        auto name = type->getName();
        std::shared_ptr<arrow::Array> array;
        arrow::Status status;

        if (name == "String")
        {
            const auto & data = typeid_cast<DB::ColumnString &>(*column);
            arrow::StringBuilder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                auto ref = data.getDataAt(i);
                auto val = ref.toString();
                status = builder.Append(val);
                if (!status.ok())
                {
                    setError("arrow::StringBuilder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (std::string(type->getFamilyName()) == "FixedString")
        {
            const auto & data = typeid_cast<DB::ColumnFixedString &>(*column);
            arrow::StringBuilder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                auto ref = data.getDataAt(i);
                auto val = ref.toString();
                status = builder.Append(val);
                if (!status.ok())
                {
                    setError("arrow::StringBuilder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }

        else if (name == "DateTime")
        {
            const auto & data = typeid_cast<DB::ColumnUInt32 &>(*column);
            arrow::Time64Builder builder(arrow::time64(arrow::TimeUnit::NANO), pool);
            for (size_t i = 0; i < rows; ++i)
            {
                uint32_t val = data.getElement(i);
                status = builder.Append(val);
                if (!status.ok())
                {
                    setError("arrow::Time64Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Date")
        {
            static uint32_t date_to_sec = 24 * 60 * 60;
            const auto & data = typeid_cast<DB::ColumnUInt16 &>(*column);
            arrow::Time64Builder builder(arrow::time64(arrow::TimeUnit::NANO), pool);
            for (size_t i = 0; i < rows; ++i)
            {
                uint16_t val = data.getElement(i);
                status = builder.Append(date_to_sec * val);
                if (!status.ok())
                {
                    setError("arrow::Time64Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Int8")
        {
            const auto & data = typeid_cast<DB::ColumnInt8 &>(*column);
            arrow::Int8Builder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    setError("arrow::Int8Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Int16")
        {
            const auto & data = typeid_cast<DB::ColumnInt16 &>(*column);
            arrow::Int16Builder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    setError("arrow::Int16Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Int32")
        {
            const auto & data = typeid_cast<DB::ColumnInt32 &>(*column);
            arrow::Int32Builder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    setError("arrow::Int32Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Int64")
        {
            const auto & data = typeid_cast<DB::ColumnInt64 &>(*column);
            arrow::Int64Builder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    setError("arrow::Int64Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "UInt8")
        {
            const auto & data = typeid_cast<DB::ColumnUInt8 &>(*column);
            arrow::UInt8Builder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    setError("arrow::UInt8Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "UInt16")
        {
            const auto & data = typeid_cast<DB::ColumnUInt16 &>(*column);
            arrow::UInt16Builder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    setError("arrow::UInt16Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "UInt32")
        {
            const auto & data = typeid_cast<DB::ColumnUInt32 &>(*column);
            arrow::UInt32Builder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    setError("arrow::UInt32Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "UInt64")
        {
            const auto & data = typeid_cast<DB::ColumnUInt64 &>(*column);
            arrow::UInt64Builder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    setError("arrow::UInt64Builder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Float32")
        {
            const auto & data = typeid_cast<DB::ColumnFloat32 &>(*column);
            arrow::FloatBuilder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    setError("arrow::FloatBuilder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }
        else if (name == "Float64")
        {
            const auto & data = typeid_cast<DB::ColumnFloat64 &>(*column);
            arrow::DoubleBuilder builder;
            for (size_t i = 0; i < rows; ++i)
            {
                status = builder.Append(data.getElement(i));
                if (!status.ok())
                {
                    setError("arrow::DoubleBuilder.Append " + status.ToString());
                    return NULL;
                }
            }
            status = builder.Finish(&array);
        }

        if (!status.ok())
        {
            setError("arrow::StringBuilder.Finish" + status.ToString());
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
        return arrow::null();
    }

private:
    DB::BlockIO result;
    SchemaPtr schema;

    mutable std::mutex mutex;
    std::string error;
};

}
