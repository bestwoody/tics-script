#pragma once

#include <string>
#include <vector>
#include <ostream>
#include <unordered_set>

#include "data/schema.h"

namespace moonshine {

using std::string;
using std::vector;
using std::ostream;
using std::unordered_set;
using std::transform;

struct ErrColumnNameNotExists : public Err {
    ErrColumnNameNotExists(const string &name) : Err("column name not exists: '" + name + "'") {}
};

struct SortColumnDesc {
    string name;
    bool asc;

    SortColumnDesc(const string& name_, bool asc_ = true) : name(name_), asc(asc_) {}
};

struct SortedSchema : public Schema {
    size_t pk_column_count;
};

struct UnidirectSortDesc : public vector<size_t> {
    UnidirectSortDesc() {}

    const char * FromString(const char * source, char delim, const Schema &schema) {
        if (!TokenParser::IsTokenChar(*source))
            return source;
        unordered_set<string> names;
        TokenParser parser(source);
        do {
            string name;
            parser.MatchToken(name);
            if (name.empty())
                throw ErrInvalidColumnName(parser.LastParsed());
            if (names.find(name) != names.end())
                throw ErrDuplicatedColumnName(name);
            emplace_back(schema.GetIndex(name));
            names.insert(name);
        } while (parser.MatchChar(delim));
        return parser.Unparsed();
    }

    void SortSchemaColumns(const Schema &origin, SortedSchema &sorting) const {
        sorting.pk_column_count = size();
        sorting.reserve(origin.size());
        vector<bool> hitteds(origin.size());
        size_t i = 0;
        for (; i < size(); ++i) {
            size_t hitted = (*this)[i];
            sorting.emplace_back(origin[hitted]);
            hitteds[hitted] = true;
        }
        for (size_t j = 0; j < hitteds.size(); ++j) {
            if (hitteds[j])
                continue;
            sorting.emplace_back(origin[j]);
        }
    }

    friend ostream & operator <<(ostream &w, const UnidirectSortDesc &sorting) {
        for (size_t i = 0; i < sorting.size(); ++i)
            w << sorting[i] << (sorting.size() - 1 == i ? "" : ", ");
        return w;
    }
};

struct SortDesc : public vector<SortColumnDesc> {
    SortDesc() {}

    const char * FromString(const char *source, char delim, const Schema &schema) {
        if (!TokenParser::IsTokenChar(*source))
            return source;
        unordered_set<string> names;
        TokenParser parser(source);
        do {
            string name;
            parser.MatchToken(name);
            if (name.empty())
                throw ErrInvalidColumnName(parser.LastParsed());
            if (names.find(name) != names.end())
                throw ErrDuplicatedColumnName(name);
            if (!schema.Has(name))
                throw ErrColumnNameNotExists(name);
            string asc;
            parser.MatchToken(asc);
            transform(asc.begin(), asc.end(), asc.begin(), tolower);
            emplace_back(name, asc.empty() || asc == "1" || asc == "true" || asc == "asc");
            names.insert(name);
        } while (parser.MatchChar(delim));
        return parser.Unparsed();
    }
};

}
