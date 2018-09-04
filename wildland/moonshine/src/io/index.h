#pragma once

#include <string>

#include "data/index.h"
#include "fs/fs.h"
#include "io/base.h"

namespace moonshine {

using std::string;

class IndexPersistSingleFile : public Index {
public:
    IndexPersistSingleFile(const string &file_) : file(file_) {}

    void Read(const string &file, const Type &type) {
        try {
            FS::FileHandle fd = FS::OpenForRead(file);
            size_t size = FS::GetFileSize(fd, file);
            FS::Read(fd, AssignPrepare(size, type), size, 0, file);
        } catch (FS::ErrFileNotExists) {
            return;
        }
    }

    void Read(const Type &type) {
        Read(file, type);
    }

    // TODO: partail write
    void Write(const string &file) {
        if (Size() == 0)
            return;
        FS::FileHandle fd = FS::OpenForWrite(file, true, true);
        Buf buf = Data(0, Size());
        FS::Write(fd, buf.data, buf.size, 0, file);
        FS::Close(fd, file);
    }

    void Write() {
        Write(file);
    }

private:
    const string file;
};

}
