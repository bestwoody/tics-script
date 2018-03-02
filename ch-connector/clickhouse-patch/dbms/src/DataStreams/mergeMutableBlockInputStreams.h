#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

enum DedupCalculator
{
    // Simple Deduping, Similar to ReplacingSortedBlockInputStream:
    //  - Input: mutl-streams, sorted, have duplication and deletion.
    //  - Output: one stream, sorted, deduped
    //  - Dedup row by row.
    //  - Correctness standard.
    //  - Bottle neck: lots of waitings.
    //  - Speed: best ~= 10%CH, worst ~= 5%CH (best: data is well merged. worst: lots of parts, not merged)
    DedupCalculatorSyn = 0,

    // Pipeline and Vertical Deduping, using hash-table:
    //  - Input:  mutli-streams, sorted, have duplication and deletion,
    //  - Output: multi-streams, sorted, deduped.
    //  - Vertical deduping:
    //      Computing row by row
    //      Batch operating data column by column. (with SSE)
    //  - Pipelining:
    //      read from streams, split to range, multi threads
    //        => (input queue)
    //          => calculate, single thread
    //            => (output queue)
    //              => output to streams, multi threads
    //  - Bottle neck: hash calculating and rows comparing in one thread. 1 core 100%
    //  - Speed: best ~= 60%CH, worst ~= 15%CH
    DedupCalculatorAsynTable = 1,

    // Pipeline, Vertical and Parallel Deduping:
    //  - Similar to DedupCalculatorAsynTable, except deduping in multi threads.
    //  - Bottle neck: data waiting, large-pks are waiting for small-pks to go through first, this reduced concurrence.
    //  - Speed: best ~= 60%CH, worst ~= 20%CH
    DedupCalculatorAsynParallel = 2,

    // Pipeline and Vertical Deduping, using priority-queue:
    //  - similar to DedupCalculatorAsynTable, except use priority-queue instead of hash-table.
    //  - Bottle neck: data waiting as above
    //  - Speed: best ~= 60%CH, worst ~= 55%CH
    DedupCalculatorAsynQueue = 3,
};

BlockInputStreams mergeMutableBlockInputStreams(BlockInputStreams inputs, const SortDescription & description,
    const String & version_column, size_t max_block_size, DedupCalculator calculater);

}
