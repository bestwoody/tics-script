#include <DataStreams/ReplacingDeletingSortedBlockInputStream.h>
#include <Columns/ColumnsNumber.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void ReplacingDeletingSortedBlockInputStream::insertRow(MutableColumns & merged_columns, size_t & merged_rows)
{
    if (out_row_sources_buf)
    {
        /// true flag value means "skip row"
        current_row_sources.back().setSkipFlag(false);

        out_row_sources_buf->write(reinterpret_cast<const char *>(current_row_sources.data()),
                                   current_row_sources.size() * sizeof(RowSourcePart));
        current_row_sources.resize(0);
    }

    ++merged_rows;
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i]->insertFrom(*(*selected_row.columns)[i], selected_row.row_num);
}


Block ReplacingDeletingSortedBlockInputStream::readImpl()
{
    if (finished)
    {
        if (is_optimized)
            LOG_TRACE(log, "read by_row:" + toString(by_row) + ", by_column: " + toString(by_column) + ", "
                      + toString(((Float64)by_column) / (by_row + by_column) * 100, 2) + "%");
        return Block();
    }

    if (children.size() == 1 && skip_single_child)
        return children[0]->read();

    Block header;
    MutableColumns merged_columns;

    init(header, merged_columns);

    if (has_collation)
        throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

    if (merged_columns.empty())
        return Block();

    /// Additional initialization.
    if (selected_row.empty())
    {
        if (!version_column.empty())
            version_column_number = header.getPositionByName(version_column);
        if (!delmark_column.empty())
            delmark_column_number = header.getPositionByName(delmark_column);
    }

    if (is_optimized)
    {
        merge_opt(merged_columns, queue);
    }
    else
    {
        merge(merged_columns, queue);
    }

    auto res = header.cloneWithColumns(std::move(merged_columns));
    return res;
}


void ReplacingDeletingSortedBlockInputStream::merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue)
{
    size_t merged_rows = 0;

    /// Take the rows in needed order and put them into `merged_columns` until rows no more than `max_block_size`
    while (!queue.empty())
    {
        SortCursor current = queue.top();

        if (current_key.empty())
            setPrimaryKeyRef(current_key, current);

        UInt64 version = version_column_number != -1
            ? current->all_columns[version_column_number]->get64(current->pos)
            : 0;
        UInt64 delmark = delmark_column_number != -1
            ? current->all_columns[delmark_column_number]->get64(current->pos)
            : 0;

        setPrimaryKeyRef(next_key, current);

        bool key_differs = next_key != current_key;

        /// if there are enough rows and the last one is calculated completely
        if (key_differs && merged_rows >= max_block_size)
            return;

        queue.pop();

        if (key_differs)
        {
            max_version = 0;
            /// Write the data for the previous primary key.
            if (!max_delmark)
                insertRow(merged_columns, merged_rows);
            max_delmark = 0;
            current_key.swap(next_key);
        }

        /// Initially, skip all rows. Unskip last on insert.
        if (out_row_sources_buf)
            current_row_sources.emplace_back(current.impl->order, true);

        /// A non-strict comparison, since we select the last row for the same version values.
        if (version >= max_version)
        {
            max_version = version;
            max_delmark = delmark;
            setRowRef(selected_row, current);
        }

        if (!current->isLast())
        {
            current->next();
            queue.push(current);
        }
        else
        {
            /// We get the next block from the corresponding source, if there is one.
            fetchNextBlock(current, queue);
        }
    }

    /// We will write the data for the last primary key.
    if (!max_delmark)
        insertRow(merged_columns, merged_rows);

    finished = true;
}

void ReplacingDeletingSortedBlockInputStream::merge_opt(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue)
{
    size_t merged_rows = 0;

    /// Take the rows in needed order and put them into `merged_columns` until rows no more than `max_block_size`
    while (!queue.empty())
    {
        SortCursor current = queue.top();
        queue.pop();

        bool is_complete_top = queue.empty() || current.totallyLessOrEquals(queue.top());
        bool is_clean_top = is_complete_top && current->isFirst() && (queue.empty() || current.totallyLessIgnOrder(queue.top()));
        if(is_clean_top && merged_rows == 0 && current_key.empty())
        {
            size_t source_num = current.impl->order;

            bool direct_move = true;
            if(version_column_number != -1)
            {
                const auto del_column =  typeid_cast<const ColumnUInt8 *>(current->all_columns[delmark_column_number]);

                // reverse_filter - 1: delete, 0: remain.
                // filter         - 0: delete, 1: remain.
                const IColumn::Filter & reverse_filter = del_column->getData();
                IColumn::Filter filter(reverse_filter.size());
                bool no_delete = true;
                for(size_t i = 0; i < reverse_filter.size(); i ++)
                {
                    no_delete &= !reverse_filter[i];
                    filter[i] = reverse_filter[i] ^ (UInt8)1;
                }

                direct_move = no_delete;
                if(!direct_move){
                    for (size_t i = 0; i < num_columns; ++i)
                    {
                        merged_columns[i] = source_blocks[source_num]->getByPosition(i).column->filter(filter, -1);
                    }

                    RowSourcePart row_source(source_num);
                    current_row_sources.resize(filter.size());
                    for (size_t i = 0; i < filter.size(); ++i)
                    {
                        row_source.setSkipFlag(reverse_filter[i]);
                        current_row_sources[i] = row_source.data;
                    }
                    out_row_sources_buf->write(reinterpret_cast<const char *>(current_row_sources.data()),
                                               current_row_sources.size());
                    current_row_sources.resize(0);
                }
            }

            if(direct_move){
                for (size_t i = 0; i < num_columns; ++i)
                {
                    merged_columns[i] = source_blocks[source_num]->getByPosition(i).column->mutate();
                }

                if (out_row_sources_buf)
                {
                    for (size_t i = 0; i < merged_rows; ++i)
                    {
                        RowSourcePart row_source(source_num);
                        out_row_sources_buf->write(row_source.data);
                    }
                }
            }

            merged_rows = merged_columns[0]->size();

            fetchNextBlock(current, queue);

            by_column += merged_rows;

            continue;
        }

        while(true){
            /// If there are enough rows and the last one is calculated completely.
            if(merged_rows >= max_block_size){
                queue.push(current);
                return;
            }

            if (current_key.empty())
                setPrimaryKeyRef(current_key, current);

            setPrimaryKeyRef(next_key, current);

            if (next_key != current_key)
            {
                by_row ++;

                max_version = 0;
                /// Write the data for the previous primary key.
                if (!max_delmark)
                    insertRow(merged_columns, merged_rows);

                if(is_clean_top){
                    // Delete current cache and return.
                    // We will come back later and use current block's data directly.
                    max_delmark = 1;
                    current_key.reset();
                    selected_row.reset();
                    current_row_sources.resize(0);
                    queue.push(current);
                    return;
                }else{
                    max_delmark = 0;
                    current_key.swap(next_key);
                }
            }

            /// Initially, skip all rows. Unskip last on insert.
            if (out_row_sources_buf)
                current_row_sources.emplace_back(current.impl->order, true);

            UInt64 version = version_column_number != -1
                ? current->all_columns[version_column_number]->get64(current->pos)
                : 0;
            UInt64 delmark = delmark_column_number != -1
                ? current->all_columns[delmark_column_number]->get64(current->pos)
                : 0;

            /// A non-strict comparison, since we select the last row for the same version values.
            if (version >= max_version)
            {
                max_version = version;
                max_delmark = delmark;
                setRowRef(selected_row, current);
            }

            if (current->isLast())
            {
                /// We get the next block from the corresponding source, if there is one.
                fetchNextBlock(current, queue);
                break; // Break current block loop.
            }
            else
            {
                current->next();
                if(is_complete_top || queue.empty() || !(current.greater(queue.top())))
                {
                    continue; // Continue current block loop.
                }else{
                    queue.push(current);
                    break; // Break current block loop.
                }
            }
        }
    }

    /// We will write the data for the last primary key.
    if (!max_delmark && !current_key.empty())
        insertRow(merged_columns, merged_rows);

    finished = true;
}

}
