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
    static BlockInputStreams createStreams(BlockInputStreams & inputs, const SortDescription & description);

    DedupSortedBlockInputStream(BlockInputStreams & inputs, const SortDescription & description);

    ~DedupSortedBlockInputStream();

    Block read(size_t position);

private:
    void asynDedupByQueue();
    void asynFetch(size_t pisition);

    void fetchBlock(size_t pisition);

    void readFromSource(DedupCursors & output, BoundQueue & bounds);

    void pushBlockBounds(const DedupingBlockPtr & block, BoundQueue & queue);

    bool outputAndUpdateCursor(DedupCursors & cursors, BoundQueue & bounds, DedupCursor & cursor);

private:
    Logger * log;
    BlockInputStreams children;
    const SortDescription description;

    const size_t queue_max;

    BlocksFifoPtrs source_blocks;
    BlocksFifoPtrs output_blocks;

    std::unique_ptr<std::thread> dedup_thread;

    ThreadPool readers;

    size_t finished_streams = 0;
};

}
