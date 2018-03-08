#include <DataStreams/DedupSortedBlockInputStream.h>

#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>

#define DEDUP_TRACER
#define TRACE_ID false

#ifndef DEDUP_TRACER
    #define TRACER(message)
#else
    #define TRACER(message) LOG_TRACE(log, message)
#endif


namespace CurrentMetrics
{
    extern const Metric QueryThread;
}

namespace DB
{

// TODO: Use 'children'
BlockInputStreams DedupSortedBlockInputStream::createStreams(BlockInputStreams & inputs, const SortDescription & description)
{
    auto parent = std::make_shared<DedupSortedBlockInputStream>(inputs, description);

    BlockInputStreams res;
    for (size_t i = 0; i < inputs.size(); ++i)
        res.emplace_back(std::make_shared<BlockInputStream>(inputs[i], description, parent, i));

    return res;
}


DedupSortedBlockInputStream::DedupSortedBlockInputStream(BlockInputStreams & inputs_, const SortDescription & description_)
    : description(description_), queue_max(3), has_collation(false), order(1),
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

    TRACER("Total compare rows: " << total_compared);
}


Block DedupSortedBlockInputStream::read(size_t position)
{
    BlockInfoPtr block = output_blocks[position]->pop();
    if (!*block)
        return Block();
    return block->finalize();
}


void DedupSortedBlockInputStream::asynRead(size_t position)
{
    while (true)
    {
        Block block = children[position]->read();
        TRACER("A Origin read, #" << position << ", rows:" << block.rows());
        source_blocks[position]->push(std::make_shared<BlockInfo>(block, position, tracer++));
        if (!block)
            break;
    }
}


void DedupSortedBlockInputStream::readFromSource(DedupCursors & output, BoundQueue & bounds, bool * has_collation, bool skip_one_row_top)
{
    std::vector<BlockInfoPtr> blocks(source_blocks.size());
    std::vector<SortCursorImpl> cursors_initing(source_blocks.size());

    for (size_t i = 0; i < source_blocks.size(); i++)
    {
        BlockInfoPtr block = source_blocks[i]->pop();
        if (!*block)
            continue;

        blocks[i] = block;
        TRACER("R Read #" << i << " " << blocks[i]->str(TRACE_ID));
        pushBlockBounds(block, bounds, skip_one_row_top);

        cursors_initing[i] = SortCursorImpl(*block, description, order++);
        if (has_collation)
            *has_collation |= cursors_initing[i].has_collation;
    }

    for (size_t i = 0; i < blocks.size(); i++)
    {
        if (blocks[i])
            output[i] = std::make_shared<DedupCursor>(cursors_initing[i], blocks[i], *has_collation, tracer++);
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
    // TRACER("D SortingDesc " << description.size());
    // for (auto it = description.begin(); it != description.end(); ++it)
    //     TRACER("D Column '" << it->column_name << "' #" << it->column_number << (it->direction > 0 ? " Asc" : " Desc"));
    // TRACER("D IsChildrenSorted " << children.size());
    // for (auto it = children.begin(); it != children.end(); ++it)
    //     TRACER("D Child " << (*it)->getID() << " " << ((*it)->isSortedOutput() ? "Sorted" : "Unsorted"));

    BoundQueue bounds;
    DedupCursors cursors(source_blocks.size());
    readFromSource(cursors, bounds, &has_collation, true);
    TRACER("P Init Bounds " << bounds.str(TRACE_ID) << " Cursors " << cursors.size());

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
                    TRACER("Q SecondWind " << bounds.str(TRACE_ID));
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
        size_t position = bound.position();
        DedupCursor & cursor = *(cursors[position]);
        TRACER("P Pop " << bound.str(TRACE_ID) << " + " << bounds.str(TRACE_ID) << " Queue " << queue.str(TRACE_ID));

        // TODO: if !MutableSupport::in_block_deduped_before_decup_calculator, wrap it, make sure skipping can work.
        if (MutableSupport::in_block_deduped_before_decup_calculator && queue.size() == 1)
        {
            if (max)
            {
                TRACER("Q Skipping DedupB Max " << max.str(TRACE_ID) << " Cursor " << cursor.str(TRACE_ID));
                dedupCursor(max, cursor);
                TRACER("Q Skipping DedupE Max " << max.str(TRACE_ID) << " Cursor " << cursor.str(TRACE_ID));
                if (max.isLast())
                    finished_streams += outputAndUpdateCursor(cursors, bounds, max) ? 1 : 0;
            }

            if (!bound.is_bottom)
            {
                TRACER("Q NotLessThanB " << cursor.str(TRACE_ID));
                cursor.skipToNotLessThan(bound);
                TRACER("Q NotLessThanE " << cursor.str(TRACE_ID));
            }
            else
            {
                TRACER("Q ToBottomB " << cursor.str(TRACE_ID));
                cursor.assignCursorPos(bound);
                TRACER("Q ToBottomE " << cursor.str(TRACE_ID));
            }
        }

        if (!bound.is_bottom || bound.block->rows() == 1)
        {
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
                auto deleted = dedupCursor(max, cursor);
                TRACER("Q DedupE Max " << max.str(TRACE_ID) << " Cursor " << cursor.str(TRACE_ID) << " Deleted " << deleted);
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

    TRACER("P All Done. Bounds " << bounds.str(TRACE_ID) << " Queue " << queue.str(TRACE_ID));

    if (finished_streams != cursors.size())
    {
        TRACER("P Streams not finished " << finished_streams << "/" << cursors.size());
        //throw Exception("Streams not finished in deduplicating.");
    }
}


bool DedupSortedBlockInputStream::outputAndUpdateCursor(DedupCursors & cursors, BoundQueue & bounds, DedupCursor & cursor)
{
    TRACER("Q Output " << cursor);
    size_t position = cursor.position();
    output_blocks[position]->push(cursor.block);

    BlockInfoPtr block = source_blocks[position]->pop();
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
        cursors[position] = std::make_shared<DedupCursor>(
            SortCursorImpl(*block, description, order++), block, has_collation, tracer++);
        return false;
    }
}


DedupSortedBlockInputStream::DedupCursor * DedupSortedBlockInputStream::dedupCursor(DedupCursor & lhs, DedupCursor & rhs)
{
    if (!lhs.equal(rhs))
    {
        TRACER("X Skip " << lhs << " != " << rhs);
        return 0;
    }

    DedupCursor * deleted = 0;

    UInt64 version_lhs = lhs.block->versions()[lhs.row()];
    UInt64 version_rhs = rhs.block->versions()[rhs.row()];

    if (version_lhs > version_rhs)
        deleted = &rhs;
    else
        deleted = &lhs;

    deleted->block->setDeleted(deleted->row());
    return deleted;
}


template <typename Queue>
void DedupSortedBlockInputStream::pushBlockBounds(const BlockInfoPtr & block, Queue & bounds, bool skip_one_row_top)
{
    TRACER("B Push " << block->str(TRACE_ID) << " To " << bounds.str(TRACE_ID));
    if (!skip_one_row_top || block->rows() > 1)
    {
        DedupBound bound(DedupCursor(SortCursorImpl(*block, description), block, has_collation, tracer++));
        TRACER("B New Top " << bound.str(TRACE_ID));
        bounds.push(bound);
        TRACER("B Push Top To " << bounds.str(TRACE_ID));
    }

    DedupBound bottom(DedupCursor(SortCursorImpl(*block, description), block, has_collation, tracer++));
    bottom.setToBottom();
    TRACER("B New Bottom " << bottom.str(TRACE_ID));
    bounds.push(bottom);
    TRACER("B Push Bottom To " << bounds.str(TRACE_ID));
}


void deleteRows(Block & block, const IColumn::Filter & filter)
{
    for (size_t i = 0; i < block.columns(); i++)
    {
        ColumnWithTypeAndName column = block.getByPosition(i);
        column.column = column.column->filter(filter, 0);
        block.erase(i);
        block.insert(i, column);
    }
}

size_t setFilterByDeleteMarkColumn(const Block & block, IColumn::Filter & filter, bool init)
{
    if (!block.has(MutableSupport::delmark_column_name))
        return 0;

    const ColumnWithTypeAndName & delmark_column =  block.getByName(MutableSupport::delmark_column_name);
    const ColumnUInt8 * column = typeid_cast<const ColumnUInt8 *>(delmark_column.column.get());
    if (!column)
        throw("Del-mark column should be type ColumnUInt8.");

    size_t rows = block.rows();
    filter.resize(rows);
    if (init)
        for (size_t i = 0; i < rows; i++)
            filter[i] = 1;

    size_t sum = 0;
    for (size_t i = 0; i < rows; i++)
    {
        if (column->getElement(i))
        {
            filter[i] = 0;
            sum += 1;
        }
    }

    return sum;
}

}
