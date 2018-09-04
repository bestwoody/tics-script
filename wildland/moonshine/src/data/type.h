#pragma once

#include <string>
#include <ostream>
#include <sstream>
#include <unordered_map>
#include <memory>

#include "data/column.h"

namespace moonshine {

using std::string;
using std::ostream;
using std::stringstream;
using std::unordered_map;
using std::make_shared;
using std::shared_ptr;

struct ErrInvalidTypeName : public Err {
    ErrInvalidTypeName(const string &name) : Err("invalid type name: '" + name + "'") {}
};

struct Type {
    enum Code {
        Invalid = 0,
        UInt8 = 10,
        UInt16 = 11,
        UInt32 = 12,
        UInt64 = 13,
        Int8 = 20,
        Int16 = 21,
        Int32 = 22,
        Int64 = 23,
        Float32 = 30,
        Float64 = 31,
        Timestamp = 40,
        DateTime = 41,
        String = 50,
        FixedString = 51,
        Max = 60
    };

    Code code;
    string name;
    size_t data_size;
    bool is_numberic;
    bool is_fixed_size;

    Type() : code(Invalid) {}

    Type(Code code_, const string &name_, size_t data_size_, bool is_numberic_, bool is_fixed_size_) :
        code(code_), name(name_), data_size(data_size_), is_numberic(is_numberic_), is_fixed_size(is_fixed_size_) {
    }

    void operator =(const Type &x) {
        code = x.code;
        name = x.name;
        data_size = x.data_size;
        is_numberic = x.is_numberic;
        is_fixed_size = x.is_fixed_size;
    }

    bool operator !=(const Type &x) {
        return
            code != x.code ||
            name != x.name ||
            data_size != x.data_size ||
            is_numberic != x.is_numberic ||
            is_fixed_size != x.is_fixed_size;
    }

    friend ostream & operator <<(ostream &w, const Type &type) {
        return w << "code: " << type.code << ", name: " << type.name << ", size: " <<
            type.data_size << ", numberic: " << (type.is_numberic ? "true" : "false") <<
            ", fixed size: " << (type.is_fixed_size ? "true" : "false");
    }

    virtual ColumnPtr CreateColumn() const {
        throw ErrNoImpl("Type(" + name + ").CreateColumn");
    }
};

using TypePtr = shared_ptr<Type>;

struct TypeUInt8 : public Type {
    TypeUInt8() : Type(Type::UInt8, "UInt8", sizeof(UInt8), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnUInt8>();
    }
};

struct TypeUInt16 : public Type {
    TypeUInt16() : Type(Type::UInt16, "UInt16", sizeof(UInt16), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnUInt16>();
    }
};

struct TypeUInt32 : public Type {
    TypeUInt32() : Type(Type::UInt32, "UInt32", sizeof(UInt32), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnUInt32>();
    }
};

struct TypeUInt64 : public Type {
    TypeUInt64() : Type(Type::UInt64, "UInt64", sizeof(UInt64), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnUInt64>();
    }
};

struct TypeInt8 : public Type {
    TypeInt8() : Type(Type::Int8, "Int8", sizeof(Int8), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnInt8>();
    }
};

struct TypeInt16 : public Type {
    TypeInt16() : Type(Type::Int16, "Int16", sizeof(Int16), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnInt16>();
    }
};

struct TypeInt32 : public Type {
    TypeInt32() : Type(Type::Int32, "Int32", sizeof(Int32), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnInt32>();
    }
};

struct TypeInt64 : public Type {
    TypeInt64() : Type(Type::Int64, "Int64", sizeof(Int64), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnInt64>();
    }
};

struct TypeFloat32 : public Type {
    TypeFloat32() : Type(Type::Float32, "Float32", sizeof(Float32), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnFloat32>();
    }
};

struct TypeFloat64 : public Type {
    TypeFloat64() : Type(Type::Float64, "Float64", sizeof(Float64), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnFloat64>();
    }
};

struct TypeTimestamp : public Type {
    TypeTimestamp() : Type(Type::Timestamp, "Timestamp", sizeof(UInt32), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnTimestamp>();
    }
};

struct TypeDateTime : public Type {
    TypeDateTime() : Type(Type::DateTime, "DateTime", sizeof(UInt32), true, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnDateTime>();
    }
};

struct TypeString : public Type {
    TypeString() : Type(Type::String, "String", 0, false, false) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnString>();
    }
};

struct TypeFixedString : public Type {
    TypeFixedString(size_t string_size) : Type(Type::FixedString, "FixedString", string_size, false, true) {
    }
    ColumnPtr CreateColumn() const override {
        return make_shared<ColumnFixedString>(data_size);
    }
};

struct ErrInvalidTypeCode : public Err {
    ErrInvalidTypeCode(Type::Code code) {
        stringstream ss;
        ss << "invalid type code: " << code;
        msg = ss.str();
    }
};

class TypeFactory {
public:
    TypeFactory() {
        Reg<TypeUInt8>();
        Reg<TypeUInt16>();
        Reg<TypeUInt32>();
        Reg<TypeUInt64>();
        Reg<TypeInt8>();
        Reg<TypeInt16>();
        Reg<TypeInt32>();
        Reg<TypeInt64>();
        Reg<TypeFloat32>();
        Reg<TypeFloat64>();
        Reg<TypeTimestamp>();
        Reg<TypeDateTime>();
        Reg<TypeString>();

        // TODO: FixedString length parse
        TypePtr type = make_shared<TypeFixedString>(16);
        names_map.emplace(type->name, type);
    }

    TypePtr FromString(string type) const {
        auto it = names_map.find(type);
        if (it != names_map.end())
            return it->second;
        throw ErrInvalidTypeName(type);
    }

private:
    template <typename T>
    void Reg() {
        TypePtr type = make_shared<T>();
        names_map.emplace(type->name, type);
    }

    unordered_map<string, TypePtr> names_map;
};

}
