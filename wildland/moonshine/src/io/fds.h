#pragma once

#include <string>
#include <unordered_map>

#include <fcntl.h>

#include "fs/fs.h"
#include "io/base.h"

namespace moonshine {

using std::string;
using std::unordered_map;

struct FDMap : public unordered_map<string, FS::FileHandle> {
    FS::FileHandle GetFD(const string &file) {
        auto it = find(file);
        if (it != end())
            return it->second;
        FS::FileHandle fd = FS::OpenForWrite(file, true, false);
        return emplace(file, fd).first->second;
    }

    void FlushAll(bool close_fd = false) {
        for (auto it: *this) {
            const string &name = it.first;
            FS::FileHandle fd = it.second;
            FS::FSync(fd, name);
            if (close_fd)
                FS::Close(fd, name);
        }
    }
};

}
