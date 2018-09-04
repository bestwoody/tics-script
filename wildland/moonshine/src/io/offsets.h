#pragma once

#include <string>

#include "data/schema.h"
#include "data/offsets.h"
#include "fs/fs.h"
#include "io/base.h"

namespace moonshine {

using std::string;

class OffsetsMapPersistSingleFile : public OffsetsMap {
public:
    OffsetsMapPersistSingleFile(const string &dir_, const string &suffix_) : dir(dir_), suffix(suffix_) {}

    PersistOffsetType AppendAndGetOffset(const TypeAndName &type, size_t cb) {
        return GetColumnOffsets(type.name).AppendSize(cb);
    }

    PersistOffsetType OffsetAt(const TypeAndName &type, size_t pos_in_block_stream) {
        return GetColumnOffsets(type.name).OffsetAt(pos_in_block_stream);
    }

    PersistRange RangeAt(const TypeAndName &type, size_t pos_in_block_stream) {
        return GetColumnOffsets(type.name).RangeAt(pos_in_block_stream);
    }

    // TODO: partail write
    void FlushAll() {
        for (auto &it: *this) {
            const Offsets &offsets = it.second;
            if (offsets.empty())
                continue;
            string file = dir + "/" + it.first + suffix;
            FS::FileHandle fd = FS::OpenForWrite(file, true, false);
            FS::Write(fd, (const char *)&offsets[0], offsets.size() * sizeof(Offsets::value_type), 0, file);
            FS::Close(fd, file);
        }
    }

    void LoadAll(const SortedSchema &schema) {
        for (auto type: schema)
            GetColumnOffsets(type.name);
    }

private:
    Offsets & GetColumnOffsets(const string &name) {
        auto it = find(name);
        if (it != end())
            return it->second;
        Offsets &offsets = emplace(name, Offsets()).first->second;
        string file = dir + "/" + name + suffix;
        try {
            FS::FileHandle fd = FS::OpenForRead(file);
            offsets.resize(FS::GetFileSize(fd, file) / sizeof(Offsets::value_type));
            FS::Read(fd, (char *)&offsets[0], offsets.size() * sizeof(Offsets::value_type), 0, file);
        } catch (FS::ErrFileNotExists) {
        }
        return offsets;
    }

    const string dir;
    const string suffix;
};

}
