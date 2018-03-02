#include <DataStreams/DedupSortedBlockInputStream.h>

#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>

#define DEDUP_TRACER

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
BlockInputStreams DedupSortedBlockInputStream::createStreams(
    BlockInputStreams & inputs, const SortDescription & description, bool parallel, bool hash_dedup)
{
    auto parent = std::make_shared<DedupSortedBlockInputStream>(inputs, description, parallel, hash_dedup);

    BlockInputStreams res;
    for (size_t i = 0; i < inputs.size(); ++i)
        res.emplace_back(std::make_shared<BlockInputStream>(inputs[i], description, parent, i));

    return res;
}


DedupSortedBlockInputStream::DedupSortedBlockInputStream(
    BlockInputStreams & inputs_, const SortDescription & description_, bool parallel, bool hash_dedup)
    : description(description_), queue_max(3), has_collation(false), order(1),
        source_blocks(inputs_.size(), queue_max), output_blocks(inputs_.size(), queue_max), readers(inputs_.size())
{
    log = &Logger::get("DedupSorted");

    children.insert(children.end(), inputs_.begin(), inputs_.end());

    for (size_t i = 0; i < inputs_.size(); ++i)
        readers.schedule(std::bind(&DedupSortedBlockInputStream::asynRead, this, i));

    if (parallel)
    {
        if (hash_dedup)
        {
            LOG_DEBUG(log, "Start deduping in parallel.");
            dedup_thread = std::make_unique<std::thread>([this] { asynDedupParallel(); });
        }
        else
        {
            throw("Parallel deduping (use priority queue) no impl.");
        }
    }
    else
    {
        if (hash_dedup)
        {
            LOG_DEBUG(log, "Start deduping in single thread, using hash-table.");
            dedup_thread = std::make_unique<std::thread>([this, hash_dedup] { asynDedupByTable(); });
        }
        else
        {
            LOG_DEBUG(log, "Start deduping in single thread, using priority-queue");
            dedup_thread = std::make_unique<std::thread>([this, hash_dedup] { asynDedupByQueue(); });
        }
    }
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
        source_blocks[position]->push(std::make_shared<BlockInfo>(block, position, tracer++));
        if (!block)
            break;
    }
}


void DedupSortedBlockInputStream::readFromSource(DedupCursors & output, BoundQueue & bounds, bool * has_collation)
{
    std::vector<BlockInfoPtr> blocks(source_blocks.size());
    std::vector<SortCursorImpl> cursors_initing(source_blocks.size());

    for (size_t i = 0; i < source_blocks.size(); i++)
    {
        BlockInfoPtr block = source_blocks[i]->pop();
        if (!*block)
            continue;

        blocks[i] = block;
        pushBlockBounds(block, bounds);

        cursors_initing[i] = SortCursorImpl(*block, description, order++);
        if (has_collation)
            *has_collation |= cursors_initing[i].has_collation;
    }

    for (size_t i = 0; i < blocks.size(); i++)
    {
        if (blocks[i])
            output[i] = std::make_shared<DedupCursor>(cursors_initing[i], blocks[i], *has_collation, tracer++);
        else
            output[i] = std::make_shared<DedupCursor>(tracer++);
    }
}


// Single thread dedup process, fast in in well-order data (speed up by skipping).
// Slow down when data is heavily overlap.

void DedupSortedBlockInputStream::asynDedupByTable()
{
    BoundQueue bounds;
    DedupCursors cursors(source_blocks.size());

    readFromSource(cursors, bounds, &has_collation);

    while (!bounds.empty())
    {
        DedupBound bound;
        do
        {
            TRACER("P Queue " << bounds.str(true));

            bound = bounds.top();
            bounds.pop();

            // For equal row comparing
            bound.setMaxOrder();

            DedupTable table;
            total_compared += dedupRange(cursors, bound, table);

            TRACER("P Compared " << total_compared);
        }
        while (!bound.is_bottom);

        TRACER("P Output " << bound);

        size_t position = bound.position();
        output_blocks[position]->push(cursors[position]->block);

        BlockInfoPtr block = source_blocks[position]->pop();

        if (!*block)
        {
            TRACER("P Finish #" << position << " " << *block);
            cursors[position] = std::make_shared<DedupCursor>(tracer++);
            output_blocks[position]->push(block);
            continue;
        }

        pushBlockBounds(block, bounds);

        cursors[position] = std::make_shared<DedupCursor>(
            SortCursorImpl(*block, description, order++), block, has_collation, tracer++);

        TRACER("P Cursor " << *(cursors[position]));
    }
}


void DedupSortedBlockInputStream::asynDedupByQueue()
{
    BoundQueue bounds;
    DedupCursors cursors(source_blocks.size());

    readFromSource(cursors, bounds, &has_collation);

    DedupCursor prev_max;

    while (!bounds.empty())
    {
        DedupBound bound;
        do
        {
            TRACER("P Queue " << bounds.str(true));

            bound = bounds.top();
            bounds.pop();

            // For equal row comparing
            bound.setMaxOrder();

            // Edge deduping. TODO: much slower on well-ordered data.
            BoundQueue edges = bounds;
            StreamMasks deduped;
            while (!edges.empty())
            {
                DedupBound it = edges.top();
                edges.pop();
                if (!deduped.flaged(it.position()))
                    dedupCursor(it, bound);
                deduped.flag(it.position());
            }

            total_compared += dedupRange(cursors, bound, prev_max);
            TRACER("P Compared " << total_compared);
        }
        while (!bound.is_bottom);

        prev_max = DedupCursor();

        TRACER("P Output " << bound);

        size_t position = bound.position();
        output_blocks[position]->push(cursors[position]->block);

        BlockInfoPtr block = source_blocks[position]->pop();

        if (!*block)
        {
            TRACER("P Finish #" << position << " " << *block);
            cursors[position] = std::make_shared<DedupCursor>(tracer++);
            output_blocks[position]->push(block);
            continue;
        }

        pushBlockBounds(block, bounds);

        cursors[position] = std::make_shared<DedupCursor>(
            SortCursorImpl(*block, description, order++), block, has_collation, tracer++);

        TRACER("P Cursor " << *(cursors[position]));
    }
}


DedupSortedBlockInputStream::DedupCursor * DedupSortedBlockInputStream::dedupCursor(DedupCursor & lhs, DedupCursor & rhs)
{
    if (!lhs.equal(rhs))
        return 0;

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


size_t DedupSortedBlockInputStream::dedupRange(DedupCursors & cursors, DedupBound & bound, DedupCursor & prev_max)
{
    StreamMasks streams(cursors.size());

    for (size_t i = 0; i < cursors.size(); i++)
    {
        DedupCursor & cursor = *(cursors[i]);
        if (!cursor)
            continue;
        if (bound.greater(cursor))
            streams.flag(i);
    }

    if (streams.flags() <= 0)
        return 0;

    if (streams.flags() == 1)
    {
        DedupCursor & cursor = *(cursors[bound.position()]);
        while (bound.greater(cursor))
            cursor.next();
        return 0;
    }

    CursorQueue queue;

    for (size_t i = 0; i < cursors.size(); i++)
    {
        if (!streams.flaged(i))
            continue;

        DedupCursor & cursor = *(cursors[i]);
        queue.push(CursorPlainPtr(&cursor));
    }

    size_t compared = 0;

    DedupCursor * max = &prev_max;

    while (!queue.empty())
    {
        DedupCursor & cursor = *(queue.top().ptr);
        queue.pop();

        if (!max || !*max)
        {
            max = &cursor;
            continue;
        }

        DedupCursor* deleted = dedupCursor(*max, cursor);

        if (deleted)
        {
            if (deleted == max)
                max = &cursor;
        }

        compared += 1;

        if (!cursor.isLast())
        {
            cursor.next();
            if (bound.greater(cursor))
                queue.push(CursorPlainPtr(&cursor));
        }
    }

    prev_max = *max;
    return compared;
}


size_t DedupSortedBlockInputStream::dedupRange(DedupCursors & cursors, DedupBound & bound, DedupTable & table)
{
    size_t overlaped = 1;
    size_t compared = 0;

    for (size_t i = 0; i < cursors.size(); i++)
    {
        DedupCursor & cursor = *(cursors[i]);
        if (!cursor)
            continue;

        // If bound is a leading bound, skip calculating hash. A lot faster if data is well-ordered.
        if (i != bound.position())
        {
            size_t column_compared_rows = dedupColumn(cursor, bound, table);
            overlaped += (column_compared_rows  > 0) ? 1 : 0;
            compared += column_compared_rows;
        }

        // For: if next bound is equal to this one
        cursor.backward();
    }

    DedupCursor & cursor = *(cursors[bound.position()]);
    if (overlaped > 1 && cursor)
    {
        compared += dedupColumn(cursor, bound, table);
        cursor.backward();
    }

    return compared;
}


void DedupSortedBlockInputStream::asynDedupParallel()
{
    DedupCursors cursors(source_blocks.size());

    BoundQueue queue;
    readFromSource(cursors, queue, &has_collation);

    BoundCalculater bounds(queue, cursors.size(), this);

    const size_t job_queue_max = queue_max * 2;
    DedupJobs jobs(cursors.size(), job_queue_max);

    ThreadPool workers(cursors.size());

    for (size_t i = 0; i < cursors.size(); ++i)
        workers.schedule(std::bind(&DedupSortedBlockInputStream::asynDedupRange, this, jobs[i], i));

    StreamMasks finisheds(cursors.size());

    TRACER("P Jobs " << jobs);

    while (!bounds.empty())
    {
        TRACER("P Queue " << bounds.str(true));

        BoundCalculater::Task task = bounds.pop(tracer++);
        size_t position = task.position();

        dedupEdgeByTable(bounds, task.bound);

        TRACER("P Task " << task << " #" << position << "EL " << jobs << " " << bounds);

        DedupTablePtr table = std::make_shared<DedupTable>();

        if (task.overlapeds <= 1)
        {
            DedupJobPtr job = std::make_shared<DedupJob>(task.bound.block, tracer++);
            jobs[position]->push(job);
            TRACER("P Q=>#" << position << " " << job->str(position) << " " << jobs);
        }
        else
        {
            DedupCounterPtr counter = std::make_shared<DedupCounter>(task.masks.flags());

            for (size_t i = 0; i < cursors.size(); ++i)
            {
                if (!task.masks.flaged(i))
                {
                    TRACER("P #" << i << task << " Filter M");
                    continue;
                }

                if (!*cursors[position])
                {
                    TRACER("P #" << i << *(cursors[position]) << " " << jobs << " Filter C");
                    continue;
                }

                DedupJobPtr job = std::make_shared<DedupJob>(cursors, task.bound, table, counter, tracer++);

                jobs[i]->push(job);
                TRACER("P J=>#" << i << " " << job->str(i) << " " << jobs);
            }
        }

        BlockInfoPtr block = source_blocks[position]->pop();
        TRACER("P Fetch " << *block);

        if (!*block)
        {
            DedupJobPtr job = std::make_shared<DedupJob>(block, tracer++);
            jobs[position]->push(job);
            TRACER("P N=>#" << position << " " << job->str(position));
            cursors[position] = std::make_shared<DedupCursor>(tracer++);
        }
        else
        {
            pushBlockBounds(block, bounds);
            cursors[position] = std::make_shared<DedupCursor>(SortCursorImpl(*block, description, order++),
                block, has_collation, tracer++);
        }

        TRACER("P #" << position << " " << *(cursors[position]) << " " << jobs << " Cursor");
    }

    workers.wait();
}


void DedupSortedBlockInputStream::asynDedupRange(DedupJobsFifoPtr & input, size_t position)
{
    while (true)
    {
        DedupJobPtr job = input->pop();
        TRACER("R #" << position << " " << job->str(position) << " Job");

        if (job->directOutput())
        {
            output_blocks[position]->push(job->direct);
            TRACER("R #" << position << " " << job->str(position) << " Direct");

            if (job->directEmpty())
            {
                TRACER("R #" << position << " " << job->str(position) << " Break");
                break;
            }
            continue;
        }

        size_t compared = dedupColumn(*(job->cursors[position]), job->bound, *(job->table));
        TRACER("R #" << position << " " << job->str(position) << " Dedup " << compared);

        {
            std::lock_guard<std::mutex> lock(mutex);
            total_compared += compared;
        }

        {
            std::lock_guard<std::mutex> lock(job->counter->locker());

            job->counter->increase();

            if (job->counter->deduped())
            {
                job->table->clear();
                output_blocks[position]->push(job->block());
                TRACER("R #" << position << " " << job->str(position) << " Output");
            }
        }
    }
}


size_t DedupSortedBlockInputStream::dedupColumn(DedupCursor & cursor, DedupBound & bound, DedupTable & table)
{
    size_t compared = 0;

    while (bound.greater(cursor))
    {
        dedupRow(cursor, table);
        compared += 1;

        if (cursor.isLast())
        {
            break;
        }
        cursor.next();
    }

    return compared;
}


void DedupSortedBlockInputStream::dedupRow(DedupCursor & cursor, DedupTable & table)
{
    UInt64 digest = cursor.hash();
    UInt64 version = cursor.version();

    RowRef matched = table.find(digest);
    if (matched.empty())
    {
        table.insert(digest, RowRef {version, cursor.block, cursor.row()});
        return;
    }

    if (matched.isTheSame(cursor.position(), cursor.row()))
    {
        // Meet self, do nothing
    }
    else if (matched.version < version)
    {
        matched.block->setDeleted(matched.row);
        table.insert(digest, RowRef {version, cursor.block, cursor.row()});
    }
    else
    {
        cursor.setDeleted(cursor.row());
    }
}


template <typename Queue>
void DedupSortedBlockInputStream::pushBlockBounds(const BlockInfoPtr & block, Queue & bounds)
{
    if (block->rows() > 1)
    {
        DedupBound bound(DedupCursor(SortCursorImpl(*block, description, order++), block, has_collation, tracer++));
        bounds.push(bound);
    }

    DedupBound bottom(DedupCursor(SortCursorImpl(*block, description, order++), block, has_collation, tracer++));
    bottom.setToBottom();
    bounds.push(bottom);
}


size_t DedupSortedBlockInputStream::dedupEdgeByTable(BoundQueue & bounds, DedupBound & bound)
{
    size_t overlapeds = 1;

    DedupTable table;

    dedupRow(bound, table);

    BoundQueue copy = bounds;
    while (!copy.empty())
    {
        DedupBound it = copy.top();
        copy.pop();

        if (bound.greater(it))
        {
            overlapeds += 1;
            dedupRow(it, table);
        }
    }

    return overlapeds;
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
