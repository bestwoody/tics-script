#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/dedupUtils.h>

namespace DB
{

class ExtraHeadTailBlockInputStream : public IProfilingBlockInputStream
{
public:
    ExtraHeadTailBlockInputStream(BlockInputStreamPtr & input_, const Block & head_, const Block & tail_)
        : input(input_), head(head_), tail(tail_), head_done(!head), input_done(false), tail_done(!tail)
    {
        log = &Logger::get("ExtraHeadTailInput");
        children.emplace_back(input_);
    }

    String getName() const override
    {
        return "ExtraHeadTailInput";
    }

    String getID() const override
    {
        std::stringstream ostr(getName());
        ostr << ", head: " << head.rows() << ", tail: " << tail.rows() << " (" << input->getID() << ")";
        return ostr.str();
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
        return description;
    }

private:
    Block readImpl() override
    {
        if (!head_done)
        {
            head_done = true;
            return head;
        }

        if (!input_done)
        {
            Block block = input->read();
            if (block)
                return block;

            input_done = true;

            if (tail_done)
                return block;
            tail_done = true;
            return tail;
        }
        else
        {
            if (tail_done)
                return Block();
            tail_done = true;
            return tail;
        }
    }

private:
    Logger * log;
    BlockInputStreamPtr input;
    Block head;
    Block tail;
    bool head_done;
    bool input_done;
    bool tail_done;
};

}
