#pragma once

#include <vector>
#include <ostream>
#include <functional>
#include <memory>

#include <cstring>
#include <ctime>

#include "base/err.h"
#include "base/typedef.h"
#include "base/buf.h"
#include "data/base.h"

namespace moonshine {

using std::vector;
using std::ostream;
using std::function;
using std::shared_ptr;
using std::min;
using std::sort;

struct Permutation : public vector<InBlockSizeType> {
    friend ostream & operator << (ostream &w, const Permutation &perm) {
        for (size_t i = 0; i < perm.size(); ++i)
            w << perm[i] << ((i == perm.size() - 1) ? "" : ", ");
        return w;
    }
};

struct IColumn {
    // Return element count
    virtual size_t Size() const {
        throw ErrNoImpl("IColumn.Size");
    }

    // Return sorting(sorted) order array
    virtual void GetPermutation(Permutation &perm, size_t limit) const {
        throw ErrNoImpl("IColumn.GetPermutation");
    }

    // Sort elements by sorting order arry
    virtual void ApplyPermutation(const Permutation &perm) {
        throw ErrNoImpl("IColumn.ApplyPermutation");
    }

    // All implements must guarantee that all data are stored in one continuous buffer.
    virtual Buf Data(size_t first, size_t count) const {
        throw ErrNoImpl("IColumn.Data");
    }

    // Prepare inner buffer and expose it for receive data
    virtual char* AssignBegin(size_t bytes_count) {
        throw ErrNoImpl("IColumn.AssignBegin");
    }

    // After receive data, this method will be called
    virtual void AssignDone() {
    }

    // Fill column with random value, for debug only
    virtual void FillRand(size_t size) {
        throw ErrNoImpl("IColumn.FillRand");
    }

    // Print element, for debug only
    virtual void DebugPrint(ostream &w, size_t pos) const {
        throw ErrNoImpl("IColumn.DebugPrint");
    }
};

using ColumnPtr = shared_ptr<IColumn>;

template <typename T>
class VectorColumn : public vector<T>, public IColumn {
    using SelfType = vector<T>;

public:
    size_t Size() const override {
        return SelfType::size();
    }

    void GetPermutation(Permutation &perm, size_t limit) const override {
        if (limit == 0)
            limit = SelfType::size();
        else
            limit = min(limit, SelfType::size());
        perm.resize(limit);
        for (size_t i = 0; i < limit; ++i)
            perm[i] = i;
        sort(perm.begin(), perm.end(), [&] (size_t lhs, size_t rhs) {
            return (*this)[lhs] < (*this)[rhs];
        });
    }

    void ApplyPermutation(const Permutation &perm) override {
        SelfType sorting(SelfType::size());
        for (size_t i = 0; i < perm.size(); ++i)
            sorting[i] = (*this)[perm[i]];
        sorting.swap(*this);
    }

    Buf Data(size_t first, size_t count) const override {
        size_t cb = count * sizeof(T);
        return (cb == 0) ? Buf() : Buf((const char *)&(*this)[0] + first * sizeof(T), cb);
    }

    char * AssignBegin(size_t bytes_count) override {
        SelfType::resize(bytes_count/ sizeof(T));
        if (SelfType::empty())
            return NULL;
        return (char *)&(*this)[0];
    }

    void DebugPrint(ostream &w, size_t pos) const override {
        w << (*this)[pos];
    }
};

template <typename T>
struct NumberVectorColumn : public VectorColumn<T> {
    void FillRand(size_t size) override {
        VectorColumn<T>::resize(size);
        for (T &v: *this)
            v = rand();
    }
};

template <typename T>
struct FloatVectorColumn : public VectorColumn<T> {
    void FillRand(size_t size) override {
        VectorColumn<T>::resize(size);
        for (T &v: *this)
            v = ((T)rand()) / ((T)rand());
    }
};

template <typename T>
class ByteVectorColumn : public NumberVectorColumn<T> {
    void DebugPrint(ostream &w, size_t pos) const override {
        w << (int)(*this)[pos];
    }
};

struct ColumnUInt8 : public ByteVectorColumn<UInt8> {};
struct ColumnUInt16 : public NumberVectorColumn<UInt16> {};
struct ColumnUInt32 : public NumberVectorColumn<UInt32> {};
struct ColumnUInt64 : public NumberVectorColumn<UInt64> {};
struct ColumnInt8 : public ByteVectorColumn<Int8> {};
struct ColumnInt16 : public NumberVectorColumn<Int16> {};
struct ColumnInt32 : public NumberVectorColumn<Int32> {};
struct ColumnInt64 : public NumberVectorColumn<Int64> {};
struct ColumnFloat32 : public FloatVectorColumn<Float32> {};
struct ColumnFloat64 : public FloatVectorColumn<Float64> {};

struct ColumnTimestamp : public VectorColumn<UInt32> {
    void FillRand(size_t size) override {
        resize(size);
        for (auto &v: *this)
            v = 1200000000 + rand() % 400000000;
    }
};

struct ColumnDateTime : public VectorColumn<UInt32> {
    void DebugPrint(ostream &w, size_t pos) const override {
        time_t v = (*this)[pos];
        w << std::put_time(std::localtime(&v), "%F %T");
    }
};

struct GenRandWords {
    const string & Get() {
        static const string words("The quick brown fox jumps over the lazy dog.");
        return words;
    }
};

class ColumnString : public IColumn {
public:
    ColumnString() : offsets(1, 0) {}

    void Append(const ColumnString &rhs, size_t rhs_pos) {
        Append((const char *)&(rhs.bytes[rhs.offsets[rhs_pos]]), rhs.offsets[rhs_pos + 1] - rhs.offsets[rhs_pos] - 1);
    }

    void Append(const char *str, size_t size) {
        bytes.resize(bytes.size() + size);
        memcpy(&(bytes[offsets[offsets.size() - 1]]), str, size);
        bytes.emplace_back(0);
        offsets.emplace_back(bytes.size());
    }

    size_t Size() const override {
        return offsets.size() - 1;
    }

    void GetPermutation(Permutation &perm, size_t limit) const override {
        if (limit == 0)
            limit = Size();
        else
            limit = min(limit, Size());
        perm.resize(limit);
        for (size_t i = 0; i < limit; ++i)
            perm[i] = i;
        sort(perm.begin(), perm.end(), [&] (size_t lhs, size_t rhs) {
            return strncmp(
                (const char *)&(bytes[offsets[lhs]]),
                (const char *)&(bytes[offsets[rhs]]),
                offsets[lhs + 1] - offsets[lhs]) < 0;
        });
    }

    void ApplyPermutation(const Permutation &perm) override {
        ColumnString sorting;
        sorting.offsets.reserve(offsets.size());
        sorting.bytes.reserve(bytes.size());
        for (size_t i = 0; i < perm.size(); ++i) {
            sorting.Append(*this, perm[i]);
        }
        sorting.offsets.swap(offsets);
        sorting.bytes.swap(bytes);
    }

    Buf Data(size_t first, size_t count) const override {
        size_t cb = offsets[first + count] - offsets[first];
        return (cb == 0) ? Buf() : Buf((const char *)&bytes[0] + offsets[first], cb);
    }

    char * AssignBegin(size_t bytes_count) override {
        bytes.resize(bytes_count);
        offsets.clear();
        offsets.emplace_back(0);
        if (bytes.empty())
            return NULL;
        return (char *)&bytes[0];
    }

    void AssignDone() override {
        for (size_t i = 0; i < bytes.size(); ++i)
            if (!bytes[i])
                offsets.emplace_back(i + 1);
    }

    void FillRand(size_t size) override {
        GenRandWords grw;
        const string &words = grw.Get();
        for (size_t i = 0; i < size; ++i) {
            size_t begin = rand() % (words.size() - 2);
            size_t size = rand() % (words.size() - begin - 1) + 1;
            Append(words.c_str() + begin, size);
        }
    }

    void DebugPrint(ostream &w, size_t pos) const override {
        w.put('\"');
        w.write((const char *)&bytes[offsets[pos]], offsets[pos + 1] - offsets[pos]);
        w.put('\"');
    }

private:
    vector<UInt32> offsets;
    vector<UInt8> bytes;
};

class ColumnFixedString : public IColumn {
public:
    ColumnFixedString(size_t string_size_) : string_size(string_size_) {}

    void Append(const ColumnFixedString &rhs, size_t rhs_pos) {
        Append((const char *)&(rhs.bytes[rhs_pos * rhs.string_size]), rhs.string_size);
    }

    void Append(const char *str, size_t size) {
        if (size > string_size)
            throw ErrWrongUsage("append a too long string to ColumnFixedString");
        size_t pos = bytes.size();
        bytes.resize(bytes.size() + string_size, 0);
        memcpy(&(bytes[pos]), str, size);
    }

    size_t Size() const override {
        return bytes.size() / string_size;
    }

    void GetPermutation(Permutation &perm, size_t limit) const override {
        if (limit == 0)
            limit = Size();
        else
            limit = min(limit, Size());
        perm.resize(limit);
        for (size_t i = 0; i < limit; ++i)
            perm[i] = i;
        sort(perm.begin(), perm.end(), [&] (size_t lhs, size_t rhs) {
            return strncmp(
                (const char *)&(bytes[lhs * string_size]),
                (const char *)&(bytes[rhs * string_size]),
                string_size) < 0;
        });
    }

    void ApplyPermutation(const Permutation &perm) override {
        ColumnFixedString sorting(string_size);
        sorting.bytes.reserve(bytes.size());
        for (size_t i = 0; i < perm.size(); ++i)
            sorting.Append(*this, perm[i]);
        sorting.bytes.swap(bytes);
    }

    Buf Data(size_t first, size_t count) const override {
        size_t cb = count * string_size;
        return (cb == 0) ? Buf() : Buf((const char *)&bytes[0] + first * string_size, cb);
    }

    char * AssignBegin(size_t bytes_count) override {
        bytes.resize(bytes_count);
        if (bytes.empty())
            return NULL;
        return (char *)&bytes[0];
    }

    void FillRand(size_t size) override {
        GenRandWords grw;
        const string &words = grw.Get();
        for (size_t i = 0; i < size; ++i) {
            size_t begin = rand() % (words.size() - 2);
            size_t size = rand() % (words.size() - begin - 1) + 1;
            size = min((size_t)string_size, size);
            Append(words.c_str() + begin, size);
        }
    }

    void DebugPrint(ostream &w, size_t pos) const override {
        w.put('\"');
        w.write((const char *)&bytes[pos * string_size], string_size);
        w.put('\"');
    }

private:
    size_t string_size;
    vector<UInt8> bytes;
};

}
