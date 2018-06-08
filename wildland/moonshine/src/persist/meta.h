#pragma once

#include <string>

#include "fs/fs.h"
#include "persist/persist.h"

namespace moonshine {

using std::string;

struct CreateSqlPersistSingleFile : public ICreateSqlPersist {
    CreateSqlPersistSingleFile(const FSPtr &fs_) : fs(fs_) {}

    void Load(const string &file, string &text) {
        IFS::FileHandle fd = fs->OpenForRead(file);
        size_t size = fs->GetFileSize(fd, file);
        text.resize(size);
        fs->Read(fd, text.data(), size, 0, file);
    }

    void Save(const string &file, const char *text, size_t cb) {
        IFS::FileHandle fd = fs->OpenForWrite(file, true, false);
        fs->Write(fd, text, cb, 0, file);
        fs->Close(fd, file);
    }

private:
    FSPtr fs;
};

}
