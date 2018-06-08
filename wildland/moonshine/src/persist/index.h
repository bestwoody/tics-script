#pragma once

#include <string>

#include "data/index.h"
#include "fs/err.h"
#include "fs/fs.h"

namespace moonshine {

using std::string;

// TODO: interface defination
class IndexPersistSingleFile : public Index {
public:
    IndexPersistSingleFile(const FSPtr &fs_, const string &file_) : fs(fs_), file(file_) {}

    void Read(const string &file, const Type &type) {
        try {
            IFS::FileHandle fd = fs->OpenForRead(file);
            size_t size = fs->GetFileSize(fd, file);
            fs->Read(fd, AssignPrepare(size, type), size, 0, file);
        } catch (ErrFileNotExists) {
        }
    }

    void Read(const Type &type) {
        Read(file, type);
    }

    // TODO: partail write
    void Write(const string &file) {
        if (Size() == 0)
            return;
        IFS::FileHandle fd = fs->OpenForWrite(file, true, true);
        Buf buf = Data(0, Size());
        fs->Write(fd, buf.data, buf.size, 0, file);
        fs->Close(fd, file);
    }

    void Write() {
        Write(file);
    }

private:
    FSPtr fs;
    const string file;
};

}
