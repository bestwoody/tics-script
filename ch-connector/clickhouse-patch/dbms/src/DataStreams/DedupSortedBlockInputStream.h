#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/dedupUtils.h>

#include <common/ThreadPool.h>
#include <common/logger_useful.h>

#include <thread>


namespace DB
{

class DedupSortedBlockInputStream
{
public:
    using ParentPtr = std::shared_ptr<DedupSortedBlockInputStream>;

    class DedupSortedChildBlockInputStream : public IProfilingBlockInputStream
    {
    public:
        DedupSortedChildBlockInputStream(BlockInputStreamPtr & input_, const SortDescription & description_, ParentPtr parent_, size_t position_)
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
        ParentPtr parent;
        size_t position;
    };


public:
    static BlockInputStreams createStreams(BlockInputStreams & inputs, const SortDescription & description);

    DedupSortedBlockInputStream(BlockInputStreams & inputs, const SortDescription & description);

    ~DedupSortedBlockInputStream();

    Block read(size_t position);


public:

private:
    void asynDedupByQueue();
    void asynRead(size_t pisition);

    template <typename Queue>
    void pushBlockBounds(const DedupingBlockPtr & block, Queue & queue, bool skip_one_row_top = true);

    void readFromSource(DedupCursors & output, BoundQueue & bounds, bool skip_one_row_top = true);

    bool outputAndUpdateCursor(DedupCursors & cursors, BoundQueue & bounds, DedupCursor & cursor);

private:
    Logger * log;
    BlockInputStreams children;
    const SortDescription description;

    const size_t queue_max;

    IdGen order;
    IdGen tracer;

    BlocksFifoPtrs source_blocks;
    BlocksFifoPtrs output_blocks;

    std::unique_ptr<std::thread> dedup_thread;

    ThreadPool readers;

    size_t finished_streams = 0;
};

}
