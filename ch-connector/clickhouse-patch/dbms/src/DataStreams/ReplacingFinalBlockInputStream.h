#pragma once

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

/** In block deduping stream
  */
class ReplacingFinalBlockInputStream : public MergingSortedBlockInputStream
{
public:
    ReplacingFinalBlockInputStream(BlockInputStreams inputs_, const SortDescription & description_,
        const String & version_column_, size_t max_block_size_, WriteBuffer * out_row_sources_buf_ = nullptr)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, out_row_sources_buf_),
        version_column(version_column_)
    {
    }

    String getName() const override { return "ReplacingFinal"; }

    String getID() const override
    {
        std::stringstream res;
        res << "ReplacingFinal(inputs";

        for (size_t i = 0; i < children.size(); ++i)
            res << ", " << children[i]->getID();

        res << ", description";

        for (size_t i = 0; i < description.size(); ++i)
            res << ", " << description[i].getID();

        res << ", version_column, " << version_column << ")";
        return res.str();
    }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    String version_column;
    ssize_t version_column_number = -1;

    Logger * log = &Logger::get("ReplacingFinalBlockInputStream");

    /// All data has been read.
    bool finished = false;

    RowRef current_key;            /// Primary key of current row.
    RowRef next_key;            /// Primary key of next row.

    RowRef selected_row;        /// Last row with maximum version for current primary key.

    UInt64 max_version = 0;        /// Max version for current primary key.

    PODArray<RowSourcePart> current_row_sources;   /// Sources of rows with the current primary key

    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    /// Output into result the rows for current primary key.
    void insertRow(MutableColumns & merged_columns, size_t & merged_rows);
};

}
