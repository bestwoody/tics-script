#pragma once

#include <string>
#include <unordered_map>

#include "fs/fs.h"

namespace moonshine {

using std::string;
using std::unordered_map;

class FDMap : public unordered_map<string, IFS::FileHandle> {
public:
    FDMap(const FSPtr &fs_) : fs(fs_) {}

    IFS::FileHandle GetFD(const string &file) {
        auto it = find(file);
        if (it != end())
            return it->second;
        IFS::FileHandle fd = fs->OpenForWrite(file, true, false);
        return emplace(file, fd).first->second;
    }

    void FlushAll(bool close_fd = false) {
        for (auto it: *this) {
            const string &name = it.first;
            IFS::FileHandle fd = it.second;
            fs->FSync(fd, name);
            if (close_fd)
                fs->Close(fd, name);
        }
    }

private:
    FSPtr fs;
};

}
