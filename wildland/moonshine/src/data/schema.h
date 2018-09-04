#pragma once

#include <string>
#include <sstream>
#include <vector>
#include <ostream>
#include <unordered_map>

#include "base/tokenize.h"
#include "data/type.h"

namespace moonshine {

using std::string;
using std::stringstream;
using std::vector;
using std::ostream;
using std::unordered_map;

struct TypeAndName {
    TypePtr type;
    string name;

    TypeAndName() {}
    TypeAndName(const TypePtr &type_, const string &name_) : type(type_), name(name_) {}

    friend ostream & operator <<(ostream &w, const TypeAndName &column) {
        return w << "column: '" << column.name << "'. " << *(column.type);
    }
};

struct ErrInvalidColumnName : public Err {
    ErrInvalidColumnName(const string &msg) : Err("invalid column name: '" + msg + "'") {}
};

struct ErrDuplicatedColumnName : public Err {
    ErrDuplicatedColumnName(const string &msg) : Err("duplicated column name: '" + msg + "'") {}
};

struct ErrSchemaParsing: public Err {
    ErrSchemaParsing(const string &msg) : Err("schema parsing error, " + msg) {}
};

struct ErrSchemaParsingUnexpected: public ErrSchemaParsing {
    ErrSchemaParsingUnexpected(const string &unparsed) : ErrSchemaParsing(string("unexpected: '") + unparsed + "'") {}
};

struct ErrSchemaParsingUnexpectedChar: public ErrSchemaParsing {
    ErrSchemaParsingUnexpectedChar(char expected, const string &got) : ErrSchemaParsing("") {
        stringstream ss;
        ss << msg << "expected: '" << expected << "', got: '" << got << "'";
        msg = ss.str();
    }
};

struct ErrSchemaParsingUnexpectedString: public ErrSchemaParsing {
    ErrSchemaParsingUnexpectedString(const string &expected, const string &got) : ErrSchemaParsing("") {
        stringstream ss;
        ss << msg << "expected: '" << expected << "', got: '" << got << "'";
        msg = ss.str();
    }
};

class Schema : public vector<TypeAndName> {
public:
    Schema() {}

    size_t GetIndex(const string &name) const {
        auto it = name_to_index.find(name);
        if (it == name_to_index.end())
            throw ErrInvalidColumnName(name);
        return it->second;
    }

    bool Has(const string &name) const {
        return name_to_index.find(name) != name_to_index.end();
    }

    // TODO: FixedString parse
    const char * FromString(const TypeFactory &types, const char *source, char delim) {
        TokenParser parser(source);
        do {
            string name;
            if (!parser.MatchToken(name))
                throw ErrSchemaParsingUnexpected(parser.Unparsed());
            if (name_to_index.find(name) != name_to_index.end())
                throw ErrDuplicatedColumnName(name);
            name_to_index.emplace(name, size());
            string type;
            if (!parser.MatchToken(type))
                throw ErrSchemaParsingUnexpected(parser.Unparsed());
            emplace_back(types.FromString(type), name);
        } while (parser.MatchChar(delim));
        return parser.Unparsed();
    }

    friend ostream & operator <<(ostream &w, const Schema &schema) {
        for (auto &column: schema)
            w << column << '\n';
        return w;
    }

private:
    unordered_map<string, size_t> name_to_index;
};

}
