#pragma once

#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace moonshine {

using std::string;

struct FSLocalSync {
    using FileHandle = int;

    struct ErrPersist : public Err {
        ErrPersist(const string &msg) {
            stringstream ss;
            ss << msg << ", errno: " << errno;
            this->msg = ss.str();
        }
    };

    struct ErrFileNotExists : public ErrPersist {
        ErrFileNotExists(const string &file) : ErrPersist("file can not open for read: '" + file + "'") {}
    };

    struct ErrFileCannotOpenForWrite : public ErrPersist {
        ErrFileCannotOpenForWrite(const string &file) : ErrPersist("file can not open for write: '" + file + "'") {}
    };

    struct ErrFileCannotOpenForRead : public ErrPersist {
        ErrFileCannotOpenForRead(const string &file) : ErrPersist("file can not open for read: '" + file + "'") {}
    };

    struct ErrFileCannotStat : public ErrPersist {
        ErrFileCannotStat(const string &file) : ErrPersist("file can not stat: '" + file + "'") {}
    };

    struct ErrCreateDirFailed : public ErrPersist {
        ErrCreateDirFailed(const string &dir) : ErrPersist("create dir failed: '" + dir + "'") {}
    };

    struct ErrFileWriteFailed : public ErrPersist {
        ErrFileWriteFailed(const string &file) : ErrPersist("file write failed: '" + file + "'") {}
    };

    struct ErrFileReadFailed : public ErrPersist {
        ErrFileReadFailed(const string &file) : ErrPersist("file read failed: '" + file + "'") {}
    };

    struct ErrFileSyncFailed : public ErrPersist {
        ErrFileSyncFailed(const string &file) : ErrPersist("file sync failed: '" + file + "'") {}
    };

    struct ErrFileCloseFailed : public ErrPersist {
        ErrFileCloseFailed(const string &file) : ErrPersist("file close failed: '" + file + "'") {}
    };

    struct ErrDirOpenFailed : public ErrPersist {
        ErrDirOpenFailed(const string &dir) : ErrPersist("dir open failed: '" + dir+ "'") {}
    };

    static size_t GetFileSize(FileHandle fd, const string &file = "") {
        struct ::stat sb;
        if (fstat(fd, &sb) < 0)
            throw ErrFileCannotStat(FileName(file));
        return sb.st_size;
    }

    static FileHandle OpenForRead(const string &file) {
        FileHandle fd = ::open(file.c_str(), O_RDONLY);
        if (fd < 0) {
            if (errno == ENOENT)
                throw ErrFileNotExists(file);
            else
                throw ErrFileCannotOpenForRead(file);
        }
        return fd;
    }

    static size_t Read(FileHandle fd, char *buf, size_t size, size_t offset, const string &file = "") {
        int read = ::pread(fd, buf, size, offset);
        if (read < 0)
            throw ErrFileReadFailed(FileName(file));
        return size_t(read);
    }

    static FileHandle OpenForWrite(const string &file, bool create, bool truncate) {
        int flag = O_WRONLY;
        if (create)
            flag |= O_CREAT;
        if (truncate)
            flag |= O_TRUNC;
        FileHandle fd = ::open(file.c_str(), flag, 0644);
        if (fd < 0)
            throw ErrFileCannotOpenForWrite(file);
        return fd;
    }

    static size_t Write(FileHandle fd, const char *data, size_t size, size_t offset, const string &file = "") {
        int written = ::pwrite(fd, data, size, offset);
        if (written < 0)
            throw ErrFileWriteFailed(FileName(file));
        return size_t(written);
    }

    static void Close(FileHandle fd, const string &file = "") {
        if (::fsync(fd) < 0)
            throw ErrFileSyncFailed(FileName(file));
        if (::close(fd) < 0)
            throw ErrFileCloseFailed(FileName(file));
    }

    static void FSync(FileHandle fd, const string &file = "") {
        if (::fsync(fd) < 0)
            throw ErrFileSyncFailed(FileName(file));
    }

private:
    static string FileName(const string &file) {
        return file.empty() ? "<not present>" : file;
    }
};

}
