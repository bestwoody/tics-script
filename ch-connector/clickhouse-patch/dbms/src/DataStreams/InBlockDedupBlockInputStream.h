#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class InBlockDedupBlockInputStream : public IProfilingBlockInputStream
{
public:
    InBlockDedupBlockInputStream(BlockInputStreamPtr & input_, const SortDescription & description_, size_t position_)
        : input(input_), description(description_), position(position_)
    {
        log = &Logger::get("InBlockDedupInput");
        children.emplace_back(input_);
    }

    String getName() const override
    {
        return "InBlockDedupInput";
    }

    String getID() const override
    {
        std::stringstream ostr(getName());
        ostr << "(" << input->getID() << ")";
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
        return dedupInBlock(input->read(), description, position);
    }

private:
    Logger * log;
    BlockInputStreamPtr input;
    const SortDescription description;
    size_t position;
};

}
