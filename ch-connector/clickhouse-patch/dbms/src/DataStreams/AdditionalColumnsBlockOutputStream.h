#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Core/ColumnsWithTypeAndName.h>

namespace DB
{

class IAdditionalBlockGenerator
{
public:
    virtual ColumnWithTypeAndName genColumn(const Block & ref) const = 0;
    virtual const std::string & getName() const = 0;
    virtual ~IAdditionalBlockGenerator() {}
};

using AdditionalBlockGeneratorPtr = std::shared_ptr<IAdditionalBlockGenerator>;
using AdditionalBlockGenerators = std::vector<AdditionalBlockGeneratorPtr>;


template <typename T>
class AdditionalBlockGeneratorConst : public IAdditionalBlockGenerator
{
public:
    using AdditionalColumnType = ColumnVector<T>;
    using AdditionalDataType = DataTypeNumber<T>;

    AdditionalBlockGeneratorConst(const String name_, const T & value_) : name(name_), value(value_)
    {}

    ColumnWithTypeAndName genColumn(const Block & ref) const override
    {
        auto data_type = std::make_shared<AdditionalDataType>();
        size_t size = ref.rows();
        auto column = std::make_shared<AdditionalColumnType>();
        Field data = UInt64(value);
        for (size_t i = 0; i < size; i++)
            column->insert(data);
        return ColumnWithTypeAndName(column, data_type, name);
    }

    const std::string & getName() const override
    {
        return name;
    }

private:
    String name;
    const UInt64 value;
};


template <typename T>
class AdditionalBlockGeneratorIncrease : public IAdditionalBlockGenerator
{
public:
    using AdditionalColumnType = ColumnVector<T>;
    using AdditionalDataType = DataTypeNumber<T>;

    AdditionalBlockGeneratorIncrease(const String name_, const T & value_) : name(name_), value(value_)
    {}

    ColumnWithTypeAndName genColumn(const Block & ref) const override
    {
        auto data_type = std::make_shared<AdditionalDataType>();
        size_t size = ref.rows();
        auto column = std::make_shared<AdditionalColumnType>();
        for (size_t i = 0; i < size; i++)
        {
            Field data = value + i;
            column->insert(data);
        }
        return ColumnWithTypeAndName(column, data_type, name);
    }

    const std::string & getName() const override
    {
        return name;
    }

private:
    String name;
    UInt64 value;
};


class AdditionalColumnsBlockOutputStream: public IBlockOutputStream
{
public:
    AdditionalColumnsBlockOutputStream(BlockOutputStreamPtr output_, AdditionalBlockGenerators gens_)
        : output(output_), gens(gens_) {}

    void write(const Block & block) override
    {
        if (block)
        {
            Block clone = block;
            for (auto it: gens)
            {
                // TODO: dont' gen default columns
                if (clone.has(it->getName()))
                    clone.erase(it->getName());
                clone.insert(it->genColumn(block));
            }
            output->write(clone);
        } else
        {
            output->write(block);
        }
    }

    void writeSuffix() override
    {
        output->writeSuffix();
    }

    void writePrefix() override
    {
        output->writePrefix();
    }

    void flush() override
    {
        output->flush();
    }

    void setRowsBeforeLimit(size_t rows_before_limit) override
    {
        output->setRowsBeforeLimit(rows_before_limit);
    }

    void setTotals(const Block & totals) override
    {
        output->setTotals(totals);
    }

    void setExtremes(const Block & extremes) override
    {
        output->setExtremes(extremes);
    }

    void onProgress(const Progress & progress) override
    {
        output->onProgress(progress);
    }

    std::string getContentType() const override
    {
        return output->getContentType();
    }

private:
    BlockOutputStreamPtr output;
    AdditionalBlockGenerators gens;
};

}
