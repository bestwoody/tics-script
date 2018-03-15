#include <DataStreams/DedupSortedBlockInputStream.h>
#include <DataStreams/ReplacingSortedBlockInputStream.h>

#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>

// #define DEDUP_TRACER
#define TRACE_ID false

#ifndef DEDUP_TRACER
    #define TRACER(message)
#else
    #define TRACER(message) LOG_TRACE(log, message)
#endif

namespace CurrentMetrics
{
    // TODO: increase it
    extern const Metric QueryThread;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

BlockInputStreams DedupSortedBlockInputStream::createStreams(BlockInputStreams & inputs, const SortDescription & description)
{
    // For speed test, no deleted handling.
    /*
    if (inputs.size() > 32)
    {
        size_t conc = 16;
        std::vector<BlockInputStreams> splited(conc);
        size_t curr = 0;
        for (size_t i = 0; i < inputs.size(); i++)
        {
            splited[curr].emplace_back(inputs[i]);
            curr += 1;
            if (curr >= conc)
                curr = 0;
        }
        BlockInputStreams subs;
        for (size_t i = 0; i < conc; i++)
        {
            BlockInputStreamPtr replacing = std::make_shared<ReplacingSortedBlockInputStream>(
                splited[i], description, MutableSupport::version_column_name, DEFAULT_MERGE_BLOCK_SIZE, nullptr, false);
            subs.emplace_back(replacing);
        }
        BlockInputStreams res;
        BlockInputStreamPtr finalize = std::make_shared<ReplacingSortedBlockInputStream>(
            subs, description, MutableSupport::version_column_name, DEFAULT_MERGE_BLOCK_SIZE, nullptr, false);
        res.emplace_back(finalize);
        return res;
    }
    */

    auto parent = std::make_shared<DedupSortedBlockInputStream>(inputs, description);

    BlockInputStreams res;
    for (size_t i = 0; i < inputs.size(); ++i)
        res.emplace_back(std::make_shared<DedupSortedChildBlockInputStream>(inputs[i], description, parent, i));

    return res;
}


DedupSortedBlockInputStream::DedupSortedBlockInputStream(BlockInputStreams & inputs_, const SortDescription & description_)
    : description(description_), queue_max(3), order(1),
        source_blocks(inputs_.size(), queue_max), output_blocks(inputs_.size(), queue_max), readers(inputs_.size())
{
    log = &Logger::get("DedupSorted");

    children.insert(children.end(), inputs_.begin(), inputs_.end());

    for (size_t i = 0; i < inputs_.size(); ++i)
        readers.schedule(std::bind(&DedupSortedBlockInputStream::asynRead, this, i));

    LOG_DEBUG(log, "Start deduping in single thread, using priority-queue");
    dedup_thread = std::make_unique<std::thread>([this] { asynDedupByQueue(); });
}


DedupSortedBlockInputStream::~DedupSortedBlockInputStream()
{
    readers.wait();
    if (dedup_thread && dedup_thread->joinable())
        dedup_thread->join();
}


Block DedupSortedBlockInputStream::read(size_t position)
{
    DedupingBlockPtr block = output_blocks[position]->pop();
    if (!*block)
        return Block();
    return block->finalize();
}


void DedupSortedBlockInputStream::asynRead(size_t position)
{
    while (true)
    {
        Block block = children[position]->read();
        // TRACER("A Origin read, #" << position << ", rows:" << block.rows());
        source_blocks[position]->push(std::make_shared<DedupingBlock>(block, position, true, tracer++));
        if (!block)
            break;
    }
}


void DedupSortedBlockInputStream::readFromSource(DedupCursors & output, BoundQueue & bounds, bool skip_one_row_top)
{
    std::vector<DedupingBlockPtr> blocks(source_blocks.size());
    std::vector<SortCursorImpl> cursors_initing(source_blocks.size());

    for (size_t i = 0; i < source_blocks.size(); i++)
    {
        DedupingBlockPtr block = source_blocks[i]->pop();
        if (!*block)
            continue;

        blocks[i] = block;
        TRACER("R Read #" << i << " " << blocks[i]->str(TRACE_ID));
        pushBlockBounds(block, bounds, skip_one_row_top);

        cursors_initing[i] = SortCursorImpl(*block, description, order++);
        if (cursors_initing[i].has_collation)
            throw Exception("Logical error: DedupSortedBlockInputStream does not support collations", ErrorCodes::LOGICAL_ERROR);
    }

    for (size_t i = 0; i < blocks.size(); i++)
    {
        if (blocks[i])
            output[i] = std::make_shared<DedupCursor>(cursors_initing[i], blocks[i], tracer++);
        else
        {
            TRACER("R Read Null #" << i);
            output[i] = std::make_shared<DedupCursor>(tracer++);
            output_blocks[i]->push(blocks[i]);
            finished_streams += 1;
        }
    }
}


void DedupSortedBlockInputStream::asynDedupByQueue()
{
    BoundQueue bounds;
    DedupCursors cursors(source_blocks.size());
    readFromSource(cursors, bounds, true);
    LOG_DEBUG(log, "P Init Bounds " << bounds.str(TRACE_ID) << " Cursors " << cursors.size());

    CursorQueue queue;
    DedupCursor max;

    while (!bounds.empty() || max)
    {
        if (bounds.empty())
        {
            TRACER("Q SecondWind Check " << max << " Bounds " << bounds.str(TRACE_ID));
            if (!max)
                throw Exception("Deduping: if bounds are empty and loops are going on, max must be assigned.");
            if (max.isLast())
            {
                bool finished = outputAndUpdateCursor(cursors, bounds, max);
                finished_streams += finished ? 1 : 0;
                max = DedupCursor();
                if (!finished)
                {
                    TRACER("Q SecondWind " << bounds.str(TRACE_ID));
                }
                else
                {
                    TRACER("Q No SecondWind " << bounds.str(TRACE_ID));
                    break;
                }
            }
            else
                throw Exception("Deduping: max must be the last row of the block here.");
        }

        DedupBound bound = bounds.top();
        bounds.pop();
        TRACER("P Pop " << bound.str(TRACE_ID) << " + " << bounds.str(TRACE_ID) << " Queue " << queue.str(TRACE_ID));

        size_t position = bound.position();

        // Skipping optimizations
        if (queue.size() == 1 && (!bound.is_bottom || queue.top().ptr->position() == position))
        {
            size_t skipped = 0;
            DedupCursor & skipping = *(queue.top().ptr);
            DedupCursor from = skipping;
            queue.pop();
            TRACER("Q Skipping Pop " << skipping.str(TRACE_ID));

            if (!bound.is_bottom)
            {
                TRACER("Q GreaterEqualB " << skipping.str(TRACE_ID));
                skipped = skipping.skipToGreaterEqualBySearch(bound);
                TRACER("Q GreaterEqualE " << skipping.str(TRACE_ID) << " Skipped " << skipped);
            }
            else if (skipping.position() == position)
            {
                TRACER("Q ToBottomB " << skipping.str(TRACE_ID));
                skipped = skipping.assignCursorPos(bound);
                TRACER("Q ToBottomE " << skipping.str(TRACE_ID) << " Skipped " << skipped);
            }

            if (max && skipped > 0)
            {
                TRACER("Q Skipping DedupB Max " << max.str(TRACE_ID) << " Cursor " << from.str(TRACE_ID));
                dedupCursor(max, from);
                TRACER("Q Skipping DedupE Max " << max.str(TRACE_ID) << " Cursor " << from.str(TRACE_ID));
                if (max.isLast())
                    finished_streams += outputAndUpdateCursor(cursors, bounds, max) ? 1 : 0;

                if (skipping.position() == position)
                {
                    max = DedupCursor();
                }
                else
                {
                    max = skipping;
                    max.backward();
                }
                TRACER("Q Skipping Max Update " << max.str(TRACE_ID));
            }

            TRACER("Q Skipping PushBack " << skipping.str(TRACE_ID) << " ~ " << queue.str(TRACE_ID));
            queue.push(CursorPlainPtr(&skipping));
        }

        if (!bound.is_bottom || bound.block->rows() == 1)
        {
            DedupCursor & cursor = *(cursors[position]);
            queue.push(CursorPlainPtr(&cursor));
            TRACER("Q Push " << cursor.str(TRACE_ID) << " ~ " << queue.str(TRACE_ID));
        }

        while (!queue.empty())
        {
            DedupCursor & cursor = *(queue.top().ptr);
            queue.pop();
            TRACER("Q Pop " << cursor.str(TRACE_ID) << " + " << queue.str(TRACE_ID));

            if (max)
            {
                TRACER("Q DedupB Max " << max.str(TRACE_ID) << " Cursor " << cursor.str(TRACE_ID));
                dedupCursor(max, cursor);
                TRACER("Q DedupE Max " << max.str(TRACE_ID) << " Cursor " << cursor.str(TRACE_ID));
                if (max.isLast())
                    finished_streams += outputAndUpdateCursor(cursors, bounds, max) ? 1 : 0;
            }

            max = cursor;
            TRACER("Q Max Update " << max.str(TRACE_ID));

            bool range_done = cursor.isTheSame(bound);
            TRACER("Q Range " << (range_done ? "" : "Not ") << "Done " << cursor.str(TRACE_ID) << " ?= " << bound.str(TRACE_ID));

            if (!cursor.isLast())
            {
                cursor.next();
                queue.push(CursorPlainPtr(&cursor));
                TRACER("Q Next Push " << cursor.str(TRACE_ID) << " ~ " << queue.str(TRACE_ID));
            }

            if (range_done)
                break;
        }
    }

    LOG_DEBUG(log, "P All Done. Bounds " << bounds.str(TRACE_ID) << " Queue " << queue.str(TRACE_ID) <<
        "Streams finished " << finished_streams << "/" << cursors.size());
}


bool DedupSortedBlockInputStream::outputAndUpdateCursor(DedupCursors & cursors, BoundQueue & bounds, DedupCursor & cursor)
{
    TRACER("Q Output " << cursor);
    size_t position = cursor.position();
    output_blocks[position]->push(cursor.block);

    DedupingBlockPtr block = source_blocks[position]->pop();
    if (!*block)
    {
        TRACER("Q Finish #" << position << " Bounds " << bounds.str(TRACE_ID) << " Cursors " << cursors.size());
        cursors[position] = std::make_shared<DedupCursor>(tracer++);
        output_blocks[position]->push(block);
        return true;
    }
    else
    {
        TRACER("Q New Block " << block->str(TRACE_ID) << " #" << position);
        pushBlockBounds(block, bounds, true);
        cursors[position] = std::make_shared<DedupCursor>(SortCursorImpl(*block, description, order++), block, tracer++);
        return false;
    }
}


template <typename Queue>
void DedupSortedBlockInputStream::pushBlockBounds(const DedupingBlockPtr & block, Queue & bounds, bool skip_one_row_top)
{
    TRACER("B Push " << block->str(TRACE_ID) << " To " << bounds.str(TRACE_ID));
    if (!skip_one_row_top || block->rows() > 1)
    {
        DedupBound bound(DedupCursor(SortCursorImpl(*block, description), block, tracer++));
        TRACER("B New Top " << bound.str(TRACE_ID));
        bounds.push(bound);
        TRACER("B Push Top To " << bounds.str(TRACE_ID));
    }

    DedupBound bottom(DedupCursor(SortCursorImpl(*block, description), block, tracer++));
    bottom.setToBottom();
    TRACER("B New Bottom " << bottom.str(TRACE_ID));
    bounds.push(bottom);
    TRACER("B Push Bottom To " << bounds.str(TRACE_ID));
}


}
