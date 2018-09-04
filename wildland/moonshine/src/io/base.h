#pragma once

#include <string>
#include <vector>

#include <fcntl.h>
#include <dirent.h>

#include <fs/fs.h>

namespace moonshine {

using std::string;
using std::vector;

struct ListDirs : public vector<string> {
    ListDirs(const string &path) {
        DIR *dir;
        dir = opendir(path.c_str());
        if (!dir)
            throw FS::ErrDirOpenFailed(path);
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (!(ent->d_type & DT_DIR))
                continue;
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;
            emplace_back(ent->d_name);
        }
    }
};

struct StringPersist {
    static void Load(const string &file, string &text) {
        FS::FileHandle fd = FS::OpenForRead(file);
        size_t size = FS::GetFileSize(fd, file);
        text.resize(size);
        FS::Read(fd, text.data(), size, 0, file);
    }

    static void Save(const string &file, const char *text, size_t cb) {
        FS::FileHandle fd = FS::OpenForWrite(file, true, false);
        FS::Write(fd, text, cb, 0, file);
        FS::Close(fd, file);
    }
};

}
