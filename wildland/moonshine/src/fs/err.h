#pragma once

#include <string>

#include "base/err.h"

namespace moonshine {

using std::string;

struct ErrPersist : public Err {
    ErrPersist(const string &msg, int err) {
        stringstream ss;
        ss << msg << ", errno: " << err;
        this->msg = ss.str();
    }
};

struct ErrFileNotExists : public ErrPersist {
    ErrFileNotExists(const string &file, int err) :
        ErrPersist("file can not open for read: '" + file + "'", err) {}
};

struct ErrFileCannotOpenForWrite : public ErrPersist {
    ErrFileCannotOpenForWrite(const string &file, int err) :
        ErrPersist("file can not open for write: '" + file + "'", err) {}
};

struct ErrFileCannotOpenForRead : public ErrPersist {
    ErrFileCannotOpenForRead(const string &file, int err) :
        ErrPersist("file can not open for read: '" + file + "'", err) {}
};

struct ErrFileCannotStat : public ErrPersist {
    ErrFileCannotStat(const string &file, int err) :
        ErrPersist("file can not stat: '" + file + "'", err) {}
};

struct ErrCreateDirFailed : public ErrPersist {
    ErrCreateDirFailed(const string &dir, int err) :
        ErrPersist("create dir failed: '" + dir + "'", err) {}
};

struct ErrFileWriteFailed : public ErrPersist {
    ErrFileWriteFailed(const string &file, int err) :
        ErrPersist("file write failed: '" + file + "'", err) {}
};

struct ErrFileReadFailed : public ErrPersist {
    ErrFileReadFailed(const string &file, int err) :
        ErrPersist("file read failed: '" + file + "'", err) {}
};

struct ErrFileSyncFailed : public ErrPersist {
    ErrFileSyncFailed(const string &file, int err) :
        ErrPersist("file sync failed: '" + file + "'", err) {}
};

struct ErrFileCloseFailed : public ErrPersist {
    ErrFileCloseFailed(const string &file, int err) :
        ErrPersist("file close failed: '" + file + "'", err) {}
};

struct ErrDirOpenFailed : public ErrPersist {
    ErrDirOpenFailed(const string &dir, int err) :
        ErrPersist("dir open failed: '" + dir + "'", err) {}
};

}
