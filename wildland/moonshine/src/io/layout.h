#pragma once

#include <string>
#include <memory>

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

#include "data/block.h"
#include "data/index.h"
#include "io/index.h"
#include "io/offsets.h"
#include "io/fds.h"

namespace moonshine {

using std::string;
using std::make_shared;

class PersistLocationInFile : public PersistLocation {
public:
    PersistLocationInFile(const string &address_, int fd_, size_t offset_, size_t size_, bool do_fsync_) :
        PersistLocation(address_, fd_, offset_, size_), do_fsync(do_fsync_) {}

    void Close() override {
        if (do_fsync && ::fsync(fd) < 0)
            throw ErrFileSyncFailed(address);
    }

private:
    bool do_fsync;
};

class ColumnsLayoutByBlock : public IColumnsLayout {
public:
    ColumnsLayoutByBlock(const string &path_) : path(path_), index(path + "/_encoded_pk.idx"), column_offsets(path_, ".off") {
    }

    PersistLocationPtr PrepareColumnPartialWrite(const IColumn &column, const TypeAndName &info, size_t pos_in_block_stream, size_t rows, size_t cb) override {
        string file = path + "/" + info.name + ".dat";
        int fd = column_fds.GetFD(file);
        PersistOffsetType offset = column_offsets.AppendAndGetOffset(info, cb);
        return make_shared<PersistLocationInFile>(file, fd, offset, cb, true);
    }

    void OnEncodedPKColumnFound(const IColumn &column, const TypeAndName &info) override {
        if (index.Size() == 0)
            index.Read(*info.type);
        index.AppendMinMax(column, *info.type);
    }

    void OnWriteDone() override {
        index.Write();
        column_offsets.FlushAll();
    }

    PersistLocationPtr GetReadLocation(const TypeAndName &info, size_t pos_in_block_stream) override {
        const string file = path + "/" + info.name + ".dat";
        int fd = ::open(file.c_str(), O_RDONLY);
        if (fd < 0)
            throw ErrFileCannotOpenForRead(file);
        struct ::stat sb;
        if (fstat(fd, &sb) < 0)
            throw ErrFileCannotStat(file);
        PersistRange range = column_offsets.RangeAt(info, pos_in_block_stream);
        return make_shared<PersistLocationInFile>(file, fd, range.offset, range.size, false);
    }

    size_t GetBlockCount(const SortedSchema &schema) override {
        if (schema.empty())
            return 0;
        if (column_offsets.empty())
            column_offsets.LoadAll(schema);
        return column_offsets[schema[0].name].RangeCount();
    }

private:
    const string path;

    IndexPersistSingleFile index;
    OffsetsMapPersistSingleFile column_offsets;
    FDMap column_fds;
};

template <typename TColumnsLayout>
struct TablesLayoutByDir : public ITablesLayout {
    TablesLayoutByDir(const string & path_) : path(path_) {
        if (::mkdir(path.c_str(), 0700) < 0 && errno != EEXIST)
            throw ErrCreateDirFailed(path);
    }

    ColumnsLayoutPtr GetColumnsLayout(const string &table) override {
        string dir = path + "/" + table;
        if (::mkdir(dir.c_str(), 0700) < 0 && errno != EEXIST)
            throw ErrCreateDirFailed(dir);
        return make_shared<TColumnsLayout>(dir);
    }

private:
    const string path;
};

}
