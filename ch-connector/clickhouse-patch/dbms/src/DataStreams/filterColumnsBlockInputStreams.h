#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Core/Names.h>


namespace DB
{

/** Remove specified columns by name from a stream
  * TODO: Use RemoveColumnsBlockInputStream
  */
class FilterColumnsBlockInputStream : public IProfilingBlockInputStream
{
public:
    Block readImpl() override;

    FilterColumnsBlockInputStream(BlockInputStreamPtr & input_, const NameSet & names_)
        : input(input_), names(names_)
    {
        children.push_back(input_);
    }

    BlockExtraInfo getBlockExtraInfo() const override
    {
        return input->getBlockExtraInfo();
    }

    String getName() const override
    {
        return "FilterColumnsBlockInputStream";
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

    bool isSortedOutput() const override
    {
        return input->isSortedOutput();
    }

    const SortDescription & getSortDescription() const override
    {
        return input->getSortDescription();
    }

private:
    BlockInputStreamPtr input;
    NameSet names;
};


BlockInputStreams filterColumnsBlockInputStreams(BlockInputStreams inputs, const NameSet & filter_names);

}
