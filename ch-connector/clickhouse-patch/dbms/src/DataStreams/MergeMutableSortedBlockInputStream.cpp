#include <DataStreams/MergeMutableSortedBlockInputStream.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/HiddenColumns.h>
#include <common/logger_useful.h>


namespace DB
{

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
    if (!block.has(HiddenColumns::mutable_delmark_column_name))
        return 0;

    const ColumnWithTypeAndName & delmark_column =  block.getByName(HiddenColumns::mutable_delmark_column_name);
    const ColumnUInt8 * column = typeid_cast<ColumnUInt8 *>(delmark_column.column.get());
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


// TODO: OPTI do not copy data, just wrap the IColumn with "A new column + del-marks", lower version rows also treat as deleted
// TODO: OPTI delay batch insert
void MergeMutableSortedBlockInputStream::insertRow(MutableColumns & merged_columns, size_t & merged_rows)
{
    UInt64 delmark = delmark_column_number != -1
        ? selected_row.columns[delmark_column_number]->get64(selected_row.row_num)
        : 0;

    if (delmark)
        return;

    ++merged_rows;
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i]->insertFrom(*selected_row.columns[i], selected_row.row_num);
}


Block MergeMutableSortedBlockInputStream::readImpl()
{
    if (finished)
        return Block();

    Block merged_block;
    MutableColumns merged_columns;

    init(merged_block, merged_columns);
    if (merged_columns.empty())
        return Block();

    /// Additional initialization.
    if (selected_row.empty())
    {
        selected_row.columns.resize(num_columns);

        if (!version_column.empty())
            version_column_number = merged_block.getPositionByName(version_column);

        delmark_column = HiddenColumns::mutable_delmark_column_name;
        delmark_column_number = merged_block.getPositionByName(delmark_column);
    }

    if (has_collation)
        merge(merged_columns, queue_with_collation);
    else
        merge(merged_columns, queue);

    return merged_block;
}


template<class TSortCursor>
void MergeMutableSortedBlockInputStream::merge(MutableColumns & merged_columns, std::priority_queue<TSortCursor> & queue)
{
    size_t merged_rows = 0;

    /// Take the rows in needed order and put them into `merged_block` until rows no more than `max_block_size`
    while (!queue.empty())
    {
        TSortCursor current = queue.top();

        if (current_key.empty())
        {
            current_key.columns.resize(description.size());
            next_key.columns.resize(description.size());

            setPrimaryKeyRef(current_key, current);
        }

        UInt64 version = version_column_number != -1
            ? current->all_columns[version_column_number]->get64(current->pos)
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
            insertRow(merged_columns, merged_rows);
            current_key.swap(next_key);
        }

        /// A non-strict comparison, since we select the last row for the same version values.
        if (version >= max_version)
        {
            max_version = version;
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
    insertRow(merged_columns, merged_rows);

    finished = true;
}

}
