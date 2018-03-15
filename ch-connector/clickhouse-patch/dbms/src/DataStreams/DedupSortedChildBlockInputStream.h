#pragma once

#include <DataStreams/DedupSortedBlockInputStream.h>

namespace DB
{

class DedupSortedChildBlockInputStream : public IProfilingBlockInputStream
{
public:
    DedupSortedChildBlockInputStream(BlockInputStreamPtr & input_, const SortDescription & description_,
        std::shared_ptr<DedupSortedBlockInputStream> parent_, size_t position_)
        : input(input_), description(description_), parent(parent_), position(position_)
    {
        children.emplace_back(input_);
    }

    String getName() const override
    {
        return "DedupSortedChild";
    }

    String getID() const override
    {
        std::stringstream ostr(getName());
        ostr << "(" << position << ")";
        return ostr.str();
    }

    bool isGroupedOutput() const override
    {
        return true;
    }

    bool isSortedOutput() const override
    {
        return true;
    }

    const SortDescription & getSortDescription() const override
    {
        return description;
    }

private:
    Block readImpl() override
    {
        return parent->read(position);
    }

private:
    BlockInputStreamPtr input;
    const SortDescription description;
    std::shared_ptr<DedupSortedBlockInputStream> parent;
    size_t position;
};

}
