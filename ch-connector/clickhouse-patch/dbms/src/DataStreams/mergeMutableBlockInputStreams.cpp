#include <DataStreams/LimitByBlockInputStream.h>
#include <DataStreams/mergeMutableBlockInputStreams.h>
#include <DataStreams/ReplacingSortedBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/DedupSortedBlockInputStream.h>
#include <common/logger_useful.h>


namespace DB
{

class FilterDeletedOnePartBlockInputStream : public IProfilingBlockInputStream
{
public:
    FilterDeletedOnePartBlockInputStream(BlockInputStreamPtr & input_) : input(input_)
    {
        children.push_back(input_);
    }

    String getName() const override
    {
        return "FilterDeletedOnePart";
    }

    String getID() const override
    {
        std::stringstream res;
        res << getName() << "(" << input->getID() << ")";
        return res.str();
    }

    bool isGroupedOutput() const override
    {
        return input->isGroupedOutput();
    }

    // TODO: May be not right
    bool isSortedOutput() const override
    {
        return input->isSortedOutput();
    }
    const SortDescription & getSortDescription() const override
    {
        return input->getSortDescription();
    }

protected:
    Block readImpl()
    {
        Block block = children[0]->read();
        if (!block)
            return block;
        IColumn::Filter filter;
        setFilterByDeleteMarkColumn(block, filter, true);
        deleteRows(block, filter);
        return block;
    }

private:
    BlockInputStreamPtr input;
};

BlockInputStreams mergeMutableBlockInputStreams(BlockInputStreams inputs, const SortDescription & description,
    const String & version_column, size_t max_block_size)
{
    BlockInputStreams res;
    BlockInputStreamPtr wrapped = std::make_shared<ReplacingSortedBlockInputStream>(
        inputs, description, version_column, max_block_size, nullptr, true);
    wrapped = std::make_shared<FilterDeletedOnePartBlockInputStream>(wrapped);
    res.emplace_back(wrapped);
    return res;
}

BlockInputStreams mergeMutableBlockInputStreams(BlockInputStreams inputs, const SortDescription & description,
    const String & version_column, size_t max_block_size, DedupCalculator calculater)
{
    if (inputs.size() == 1)
        return mergeMutableBlockInputStreams(inputs, description, version_column, max_block_size);

    switch (calculater)
    {
        case DedupCalculatorAsynTable:
            return DedupSortedBlockInputStream::createStreams(inputs, description, false, true);
        case DedupCalculatorAsynQueue:
            return DedupSortedBlockInputStream::createStreams(inputs, description, false, false);
        case DedupCalculatorAsynParallel:
            return DedupSortedBlockInputStream::createStreams(inputs, description, true, true);
        default:
            return mergeMutableBlockInputStreams(inputs, description, version_column, max_block_size);
    }
}

}
