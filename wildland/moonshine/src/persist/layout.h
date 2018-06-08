#pragma once

#include <string>
#include <memory>

#include "data/block.h"
#include "data/index.h"
#include "fs/fs.h"
#include "persist/index.h"
#include "persist/offsets.h"
#include "persist/fds.h"

namespace moonshine {

using std::string;
using std::make_shared;

class PersistLocationInFile : public PersistLocation {
public:
    PersistLocationInFile(const FSPtr fs_, const string &path_, IFS::FileHandle fd_, size_t offset_, size_t size_, bool do_fsync_) :
        PersistLocation(path_, fd_, offset_, size_), fs(fs_), do_fsync(do_fsync_) {}

    void Close() override {
        if (do_fsync)
            fs->FSync(fd, path);
    }

private:
    FSPtr fs;
    bool do_fsync;
};

class ColumnsLayoutByBlock : public IColumnsLayout {
public:
    ColumnsLayoutByBlock(const FSPtr &fs_, const string &path_) :
        fs(fs_), path(path_), index(fs_, path + "/encoded_pk.idx"), column_offsets(fs_, path_, ".off"), column_fds(fs_) {}

    PersistLocationPtr PrepareColumnPartialWrite(
        const IColumn &column, const TypeAndName &info, size_t pos_in_block_stream, size_t rows, size_t cb) override {

        string file = path + "/" + info.name + ".dat";
        IFS::FileHandle fd = column_fds.GetFD(file);
        PersistOffsetType offset = column_offsets.AppendAndGetOffset(info, cb);
        return make_shared<PersistLocationInFile>(fs, file, fd, offset, cb, true);
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
        IFS::FileHandle fd = fs->OpenForRead(file);
        PersistRange range = column_offsets.RangeAt(info, pos_in_block_stream);
        return make_shared<PersistLocationInFile>(fs, file, fd, range.offset, range.size, false);
    }

    size_t GetBlockCount(const SortedSchema &schema) override {
        if (schema.empty())
            return 0;
        if (column_offsets.empty())
            column_offsets.LoadAll(schema);
        return column_offsets[schema[0].name].RangeCount();
    }

private:
    FSPtr fs;
    const string path;

    IndexPersistSingleFile index;
    OffsetsMapPersistSingleFile column_offsets;
    FDMap column_fds;
};

template <typename TColumnsLayout>
struct TablesLayoutByDir : public ITablesLayout {
    TablesLayoutByDir(const FSPtr &fs_, const string & path_) : fs(fs_), path(path_) {
        fs->MkDir(path);
    }

    ColumnsLayoutPtr GetColumnsLayout(const string &table) override {
        string dir = path + "/" + table;
        fs->MkDir(dir);
        return make_shared<TColumnsLayout>(fs, dir);
    }

private:
    FSPtr fs;
    const string path;
};

}
