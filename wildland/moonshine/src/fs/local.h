#pragma once

#include <string>
#include <vector>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

#include "fs/err.h"
#include "fs/fs.h"

namespace moonshine {

using std::string;
using std::vector;

struct FSLocalSync : public IFS {
    void ListSubDirs(const string &path, vector<string> &dirs) override {
        DIR *dir = ::opendir(path.c_str());
        if (!dir)
            throw ErrDirOpenFailed(path, errno);
        struct dirent *ent;
        while ((ent = ::readdir(dir)) != NULL) {
            if (!(ent->d_type & DT_DIR))
                continue;
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;
            dirs.emplace_back(ent->d_name);
        }
    }

    void MkDir(const string &path) override {
        if (::mkdir(path.c_str(), 0700) < 0 && errno != EEXIST)
            throw ErrCreateDirFailed(path, errno);
    }

    size_t GetFileSize(FileHandle fd, const string &display_file_name) override {
        struct ::stat sb;
        if (fstat(fd, &sb) < 0)
            throw ErrFileCannotStat(display_file_name, errno);
        return sb.st_size;
    }

    FileHandle OpenForRead(const string &file) override {
        FileHandle fd = ::open(file.c_str(), O_RDONLY);
        if (fd < 0) {
            if (errno == ENOENT)
                throw ErrFileNotExists(file, errno);
            else
                throw ErrFileCannotOpenForRead(file, errno);
        }
        return fd;
    }

    size_t Read(FileHandle fd, char *buf, size_t size, size_t offset, const string &display_file_name) override {
        int read = ::pread(fd, buf, size, offset);
        if (read < 0)
            throw ErrFileReadFailed(display_file_name, errno);
        return size_t(read);
    }

    FileHandle OpenForWrite(const string &file, bool create, bool truncate) override {
        int flag = O_WRONLY;
        if (create)
            flag |= O_CREAT;
        if (truncate)
            flag |= O_TRUNC;
        FileHandle fd = ::open(file.c_str(), flag, 0644);
        if (fd < 0)
            throw ErrFileCannotOpenForWrite(file, errno);
        return fd;
    }

    size_t Write(FileHandle fd, const char *data, size_t size, size_t offset, const string &display_file_name) override {
        int written = ::pwrite(fd, data, size, offset);
        if (written < 0)
            throw ErrFileWriteFailed(display_file_name, errno);
        return size_t(written);
    }

    void Close(FileHandle fd, const string &display_file_name) override {
        if (::fsync(fd) < 0)
            throw ErrFileSyncFailed(display_file_name, errno);
        if (::close(fd) < 0)
            throw ErrFileCloseFailed(display_file_name, errno);
    }

    void FSync(FileHandle fd, const string &display_file_name) override {
        if (::fsync(fd) < 0)
            throw ErrFileSyncFailed(display_file_name, errno);
    }
};

}
