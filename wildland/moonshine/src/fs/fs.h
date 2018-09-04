#pragma once

#include <string>
#include <vector>
#include <memory>

#include "base/err.h"

namespace moonshine {

using std::string;
using std::vector;
using std::shared_ptr;

struct IFS {
    using FileHandle = int;

    virtual void ListSubDirs(const string &path, vector<string> &dirs) {
        throw ErrNoImpl("IFS.ListSubDirs");
    }
    virtual void MkDir(const string &path) {
        throw ErrNoImpl("IFS.MkDir");
    }

    virtual size_t GetFileSize(FileHandle fd, const string &display_file_name) {
        throw ErrNoImpl("IFS.MkDir");
    }
    virtual void Close(FileHandle fd, const string &display_file_name) {
        throw ErrNoImpl("IFS.MkDir");
    }

    virtual FileHandle OpenForRead(const string &display_file_name) {
        throw ErrNoImpl("IFS.OpenForRead");
    }
    virtual size_t Read(FileHandle fd, char *buf, size_t size, size_t offset, const string &display_file_name) {
        throw ErrNoImpl("IFS.Read");
    }

    virtual FileHandle OpenForWrite(const string &file, bool create, bool truncate) {
        throw ErrNoImpl("IFS.OpenForWrite");
    }
    virtual size_t Write(FileHandle fd, const char *data, size_t size, size_t offset, const string &display_file_name) {
        throw ErrNoImpl("IFS.Write");
    }
    virtual void FSync(FileHandle fd, const string &display_file_name) {
        throw ErrNoImpl("IFS.FSync");
    }
};

using FSPtr = shared_ptr<IFS>;

}
