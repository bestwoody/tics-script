#pragma once

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

class ReplacingDeletingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    ReplacingDeletingSortedBlockInputStream(BlockInputStreams inputs_, const SortDescription & description_,
        const String & version_column_, const String & delmark_column_, size_t max_block_size_,
        WriteBuffer * out_row_sources_buf_, bool skip_single_child_, bool perform_deleting_)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, out_row_sources_buf_), version_column(version_column_),
            delmark_column(delmark_column_), skip_single_child(skip_single_child_), perform_deleting(perform_deleting_)
    {
    }

    String getName() const override { return "ReplacingDeletingSorted"; }

    String getID() const override
    {
        std::stringstream res;
        res << "ReplacingDeletingSorted(inputs";

        for (size_t i = 0; i < children.size(); ++i)
            res << ", " << children[i]->getID();

        res << ", description";

        for (size_t i = 0; i < description.size(); ++i)
            res << ", " << description[i].getID();

        res << ", version_column, " << version_column << ", delmark_column, " << delmark_column << ")";
        return res.str();
    }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    String version_column;
    String delmark_column;

    bool skip_single_child;
    bool perform_deleting;

    ssize_t version_column_number = -1;
    ssize_t delmark_column_number = -1;

    Logger * log = &Logger::get("ReplacingDeletingSorted");

    /// All data has been read.
    bool finished = false;

    RowRef current_key;            /// Primary key of current row.
    RowRef next_key;            /// Primary key of next row.

    RowRef selected_row;        /// Last row with maximum version for current primary key.

    UInt64 max_version = 0;        /// Max version for current primary key.
    UInt64 max_delmark = 0;        /// Deleted mark for current primary key.

    PODArray<RowSourcePart> current_row_sources;   /// Sources of rows with the current primary key

    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    /// Output into result the rows for current primary key.
    void insertRow(MutableColumns & merged_columns, size_t & merged_rows);
};

}
