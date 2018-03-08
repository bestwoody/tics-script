#pragma once

#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>

#include <Storages/MutableSupport.h>
#include <Columns/ColumnsNumber.h>

#include <Common/MemoryTracker.h>
#include <Common/ConcurrentBoundedQueue.h>

#include <common/ThreadPool.h>
#include <common/logger_useful.h>

#include <unordered_map>
#include <queue>
#include <thread>
#include <atomic>

#include <boost/noncopyable.hpp>


namespace DB
{

void deleteRows(Block & block, const IColumn::Filter & filter);

size_t setFilterByDeleteMarkColumn(const Block & block, IColumn::Filter & filter, bool init);

class DedupSortedBlockInputStream
{
public:
    using ParentPtr = std::shared_ptr<DedupSortedBlockInputStream>;

    class BlockInputStream : public IProfilingBlockInputStream
    {
    public:
        BlockInputStream(BlockInputStreamPtr & input_, const SortDescription & description_, ParentPtr parent_, size_t position_)
            : input(input_), description(description_), parent(parent_), position(position_)
        {
            children.emplace_back(input_);
        }

        String getName() const override
        {
            return "DedupSortedBlockInputStream";
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


private:
    class VersionColumn
    {
    public:
        VersionColumn(const Block & block) : column(0)
        {
            if (!block.has(MutableSupport::version_column_name))
                return;
            const ColumnWithTypeAndName & version_column = block.getByName(MutableSupport::version_column_name);
            column = typeid_cast<const ColumnUInt64 *>(version_column.column.get());
        }

        UInt64 operator [] (size_t row) const
        {
            return column->getElement(row);
        }

    private:
        const ColumnUInt64 * column;
    };


    class BlockInfo
    {
    private:
        BlockInfo(const BlockInfo &);
        BlockInfo & operator = (const BlockInfo &);

    public:
        BlockInfo(const Block & block_, const size_t stream_position_, size_t tracer_)
            : stream_position(stream_position_), tracer(tracer_), block(block_), filter(block_.rows()), deleted_rows(0)
        {
            std::lock_guard<std::mutex> lock(mutex);
            // TODO: unnecessary deleting, if there is InBlockDedupBlockInputStream in the pipeline.
            deleted_rows = setFilterByDeleteMarkColumn(block, filter, true);
        }

        operator bool ()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return bool(block);
        }

        operator const Block & ()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return block;
        }

        size_t rows()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return block.rows();
        }

        size_t deleteds()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return deleted_rows;
        }

        void setDeleted(size_t i)
        {
            std::lock_guard<std::mutex> lock(mutex);
            filter[i] = 0;
            deleted_rows += 1;
        }

        VersionColumn & versions()
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (!version_column)
                version_column = std::make_shared<VersionColumn>(block);
            return *version_column;
        }

        Block finalize()
        {
            std::lock_guard<std::mutex> lock(mutex);
            deleteRows(block, filter);
            return block;
        }

        String str(bool trace = false)
        {
            std::lock_guard<std::mutex> lock(mutex);

            std::stringstream ostr;
            ostr << "#";
            if (stream_position == size_t(-1))
                ostr << "?";
            else
                ostr << stream_position;
            if (trace)
                ostr << "@" << tracer;
            ostr << ":";
            if (block)
                ostr << block.rows() << "-" << deleted_rows;
            else
                ostr << "?";
            return ostr.str();
        }

        friend std::ostream & operator << (std::ostream & out, BlockInfo & self)
        {
            return out << self.str();
        }

    public:
        const size_t stream_position;
        const size_t tracer;

    private:
        Block block;

        IColumn::Filter filter;
        size_t deleted_rows;
        std::shared_ptr<VersionColumn> version_column;

        std::mutex mutex;
    };

    using BlockInfoPtr = std::shared_ptr<BlockInfo>;


    template <typename T>
    class SmallObjectFifo : public ConcurrentBoundedQueue<T>
    {
        using Self = ConcurrentBoundedQueue<T>;

    public:
        SmallObjectFifo(size_t size) : Self(size) {}

        T pop()
        {
            T value;
            Self::pop(value);
            return value;
        }
    };


    template <typename Fifo>
    class FifoPtrs : public std::vector<std::shared_ptr<Fifo>>
    {
        using Self = std::vector<std::shared_ptr<Fifo>>;
        using FifoPtr = std::shared_ptr<Fifo>;

    public:
        FifoPtrs(size_t size, size_t queue_max_) : Self(size), queue_max(queue_max_)
        {
            for (size_t i = 0; i < Self::size(); ++i)
                Self::operator[](i) = std::make_shared<Fifo>(queue_max_);
        }

        String str()
        {
            std::stringstream ostr;
            ostr << Self::size() << "*" << queue_max << "Q";
            for (size_t i = 0; i < Self::size(); ++i)
                ostr << ":" << Self::operator[](i)->size();
            return ostr.str();
        }

        friend std::ostream & operator << (std::ostream & out, FifoPtrs & self)
        {
            return out << self.str();
        }

    private:
        const size_t queue_max;
    };


    // Auto finished: return empty blocks when finished.
    class BlocksFifo : public SmallObjectFifo<BlockInfoPtr>
    {
        using Self = SmallObjectFifo<BlockInfoPtr>;

    public:
        BlocksFifo(size_t size) : Self(size) {}

        BlockInfoPtr pop()
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (last)
                return last;
            BlockInfoPtr block = Self::pop();
            if (!block || !*block)
            {
                last = std::make_shared<BlockInfo>(Block(), size_t(-1), 0);
                block = last;
            }
            return block;
        }

    private:
        BlockInfoPtr last;
        std::mutex mutex;
    };

    using BlocksFifoPtr = std::shared_ptr<BlocksFifo>;
    using BlocksFifoPtrs = FifoPtrs<BlocksFifo>;


    class DedupCursor
    {
    public:
        DedupCursor(size_t tracer_ = 0) : tracer(tracer_) {}

        DedupCursor(const DedupCursor & rhs)
            : block(rhs.block), cursor(rhs.cursor), has_collation(rhs.has_collation), tracer(rhs.tracer)
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (block)
                cursor.order = block->versions()[cursor.pos];
        }

        DedupCursor & operator = (const DedupCursor & rhs)
        {
            std::lock_guard<std::mutex> lock(mutex);
            block = rhs.block;
            cursor = rhs.cursor;
            if (block)
                cursor.order = block->versions()[cursor.pos];
            has_collation = rhs.has_collation;
            tracer = rhs.tracer;
            return *this;
        }

        DedupCursor(const SortCursorImpl & cursor_, const BlockInfoPtr & block_, bool has_collation_, size_t tracer_)
            : block(block_), cursor(cursor_), has_collation(has_collation_), tracer(tracer_)
        {
            std::lock_guard<std::mutex> lock(mutex);
            cursor.order = block->versions()[cursor.pos];
        }

        operator bool ()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return bool(block) && block->rows();
        }

        size_t setMaxOrder()
        {
            std::lock_guard<std::mutex> lock(mutex);
            size_t order = cursor.order;
            cursor.order = size_t(-1);
            return order;
        }

        UInt64 version()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return block->versions()[cursor.pos];
        }

        void setDeleted(size_t row)
        {
            std::lock_guard<std::mutex> lock(mutex);
            block->setDeleted(row);
        }

        void assignCursorPos(const DedupCursor & rhs)
        {
            std::lock_guard<std::mutex> lock(mutex);
            cursor.pos = rhs.cursor.pos;
            cursor.order = block->versions()[cursor.pos];
        }

        void skipToNotLessThan(DedupCursor & bound)
        {
            // TODO: binary search position
            std::lock_guard<std::mutex> lock(mutex);
            while (bound.greater(*this))
                cursor.next();
            cursor.order = block->versions()[cursor.pos];
        }

        bool isTheSame(const DedupCursor & rhs)
        {
            std::lock_guard<std::mutex> lock(mutex);
            return block->stream_position == rhs.block->stream_position && cursor.pos == rhs.cursor.pos;
        }

        size_t position()
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (!block)
                return size_t(-1);
            return block->stream_position;
        }

        size_t row()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return cursor.pos;
        }

        size_t order()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return cursor.order;
        }

        size_t rows()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return block->rows();
        }

        bool isLast()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return cursor.isLast();
        }

        operator Block ()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return (Block)*block;
        }

        void next()
        {
            std::lock_guard<std::mutex> lock(mutex);
            cursor.next();
            cursor.order = block->versions()[cursor.pos];
        }

        void backward()
        {
            std::lock_guard<std::mutex> lock(mutex);
            cursor.pos = cursor.pos > 0 ? cursor.pos - 1 : 0;
            cursor.order = block->versions()[cursor.pos];
        }

        UInt64 hash()
        {
            std::lock_guard<std::mutex> lock(mutex);

            size_t row = cursor.pos;
            SipHash hash;
            for (size_t i = 0; i < cursor.sort_columns_size; ++i)
                cursor.sort_columns[i]->updateHashWithValue(row, hash);
            return hash.get64();
        }

        bool greater(const DedupCursor & rhs)
        {
            std::lock_guard<std::mutex> lock(mutex);

            if (block->stream_position == rhs.block->stream_position)
                return (cursor.pos == rhs.cursor.pos) ? (cursor.order > rhs.cursor.order) : (cursor.pos > rhs.cursor.pos);

            SortCursorImpl * lc = const_cast<SortCursorImpl *>(&cursor);
            SortCursorImpl * rc = const_cast<SortCursorImpl *>(&rhs.cursor);
            if (!lc || !rc)
                throw("SortCursorImpl const_cast Failed!");

            if (has_collation)
                return SortCursorWithCollation(lc).greater(SortCursorWithCollation(rc));
            else
                return SortCursor(lc).greater(SortCursor(rc));
        }

        bool equal(const DedupCursor & rhs)
        {
            std::lock_guard<std::mutex> lock(mutex);

            if (MutableSupport::in_block_deduped_before_decup_calculator)
                return block->stream_position == rhs.block->stream_position && cursor.pos == rhs.cursor.pos;

            SortCursorImpl * lc = const_cast<SortCursorImpl *>(&cursor);
            SortCursorImpl * rc = const_cast<SortCursorImpl *>(&rhs.cursor);
            if (!lc || !rc)
                throw("SortCursorImpl const_cast Failed!");

            if (has_collation)
                return SortCursorWithCollation(lc).equalIgnOrder(SortCursorWithCollation(rc));
            else
                return SortCursor(lc).equalIgnOrder(SortCursor(rc));
        }

        // Inverst for pririoty queue
        bool operator < (const DedupCursor & rhs) const
        {
            return const_cast<DedupCursor *>(this)->greater(rhs);
        }

        String str(bool trace = false)
        {
            std::lock_guard<std::mutex> lock(mutex);
            std::stringstream ostr;

            if (trace)
                ostr << ">" << tracer;
            if (!block)
            {
                ostr << "#?";
                return ostr.str();
            }
            else
                ostr << block->str(trace);
            ostr << "/" << cursor.pos << "\\" << cursor.order;
            return ostr.str();
        }

        friend std::ostream & operator << (std::ostream & out, DedupCursor & self)
        {
            return out << self.str();
        }

    public:
        BlockInfoPtr block;

    protected:
        SortCursorImpl cursor;
        bool has_collation;
        std::mutex mutex;

    public:
        size_t tracer;
    };

    // For easy copy and sharing cursor.pos
    struct CursorPlainPtr
    {
        DedupCursor * ptr;

        CursorPlainPtr() : ptr(0) {}

        CursorPlainPtr(DedupCursor * ptr_) : ptr(ptr_) {}

        operator bool () const
        {
            return ptr != 0;
        }

        DedupCursor & operator * ()
        {
            return *ptr;
        }

        DedupCursor * operator -> ()
        {
            return ptr;
        }

        bool operator < (const CursorPlainPtr & rhs) const
        {
            return (*ptr) < (*rhs.ptr);
        }

        friend std::ostream & operator << (std::ostream & out, CursorPlainPtr & self)
        {
            return (self.ptr == 0) ? (out << "null") : (out << (*self.ptr));
        }
    };

    class CursorQueue : public std::priority_queue<CursorPlainPtr>
    {
    public:
        String str(bool trace = false)
        {
            std::stringstream ostr;
            ostr << "Q:" << size();

            CursorQueue copy = *this;
            while (!copy.empty())
            {
                CursorPlainPtr it = copy.top();
                copy.pop();
                ostr << "|" << it->str(trace);
            }
            return ostr.str();
        }

        friend std::ostream & operator << (std::ostream & out, CursorQueue & self)
        {
            return out << self.str();
        }
    };


    struct DedupBound : public DedupCursor
    {
        bool is_bottom;

        DedupBound(size_t tracer = 0) : DedupCursor(tracer), is_bottom(false) {}

        DedupBound(const DedupCursor & rhs) : DedupCursor(rhs), is_bottom(false) {}

        DedupBound(const SortCursorImpl & cursor_, const BlockInfoPtr & block_, bool has_collation_, size_t tracer_)
            : DedupCursor(cursor_, block_, has_collation_, tracer_), is_bottom(false) {}

        void setToBottom()
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (block->rows() > 1)
                cursor.pos = block->rows() - 1;
            else
                cursor.pos = 0;
            cursor.order = block->versions()[cursor.pos];
            is_bottom = true;
        }

        bool greater(const DedupBound & rhs)
        {
            return DedupCursor::greater(rhs);
        }

        bool greater(const DedupCursor & rhs)
        {
            return DedupCursor::greater(rhs);
        }

        String str(bool trace = false)
        {
            std::stringstream ostr;
            ostr << DedupCursor::str(trace);
            std::lock_guard<std::mutex> lock(mutex);
            ostr << (is_bottom ? "L" : "F");
            return ostr.str();
        }

        friend std::ostream & operator << (std::ostream & out, DedupBound & self)
        {
            return out << self.str();
        }
    };

    using DedupBoundPtr = std::shared_ptr<DedupBound>;


    class BoundQueue : public std::priority_queue<DedupBound>
    {
    public:
        String str(bool trace = false)
        {
            std::stringstream ostr;
            ostr << "Q:" << size();

            BoundQueue copy = *this;
            while (!copy.empty())
            {
                DedupBound it = copy.top();
                copy.pop();
                ostr << "|" << it.str(trace);
            }
            return ostr.str();
        }

        friend std::ostream & operator << (std::ostream & out, BoundQueue & self)
        {
            return out << self.str();
        }
    };

    using DedupCursorPtr = std::shared_ptr<DedupCursor>;
    using DedupCursors = std::vector<DedupCursorPtr>;

    class StreamMasks
    {
    public:
        using Data = std::vector<bool>;

        StreamMasks(size_t size = 0) : data(size, 0), sum(0) {}

        void assign(const Data & data_)
        {
            sum = 0;
            data = data_;
            for (Data::iterator it = data.begin(); it != data.end(); ++it)
                sum += (*it) ? 1 : 0;
        }

        size_t flags()
        {
            return sum;
        }

        void flag(size_t i)
        {
            if (!data[i])
                sum++;
            data[i] = true;
        }

        bool flaged(size_t i)
        {
            return data[i];
        }

        String str()
        {
            std::stringstream ostr;
            ostr << "[";
            for (Data::iterator it = data.begin(); it != data.end(); ++it)
                ostr << ((*it) ? "+" : "-");
            ostr << "]";
            return ostr.str();
        }

        friend std::ostream & operator << (std::ostream & out, StreamMasks & self)
        {
            return out << self.str();
        }

    private:
        Data data;
        size_t sum;
    };


    class IdGen
    {
    public:
        IdGen(size_t begin = 10000) : id(begin) {}

        void reset(size_t begin = 10000)
        {
            id = begin;
        }

        size_t operator ++ (int)
        {
            return id++;
        }

    private:
        std::atomic<size_t> id;
    };


private:
    void asynDedupByQueue();
    void asynRead(size_t pisition);

    DedupCursor * dedupCursor(DedupCursor & lhs, DedupCursor & rhs);
    template <typename Queue>
    void pushBlockBounds(const BlockInfoPtr & block, Queue & queue, bool skip_one_row_top = true);
    void readFromSource(DedupCursors & output, BoundQueue & bounds, bool * collation = 0, bool skip_one_row_top = true);
    bool outputAndUpdateCursor(DedupCursors & cursors, BoundQueue & bounds, DedupCursor & cursor);

private:
    Logger * log;
    BlockInputStreams children;
    const SortDescription description;

    const size_t queue_max;
    bool has_collation = false;

    IdGen order;
    IdGen tracer;

    BlocksFifoPtrs source_blocks;
    BlocksFifoPtrs output_blocks;

    std::unique_ptr<std::thread> dedup_thread;

    ThreadPool readers;

    size_t finished_streams = 0;
    size_t total_compared = 0;
    std::mutex mutex;
};

}
