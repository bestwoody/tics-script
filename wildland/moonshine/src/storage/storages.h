#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

#include "storage/storage.h"
#include "fs/fs.h"

namespace moonshine {

using std::string;
using std::vector;
using std::unordered_map;
using std::make_shared;
using std::shared_ptr;

struct ErrTableExists : public Err {
    ErrTableExists(const string &msg) : Err(msg + " exists") {}
};

struct ErrTableNotExists : public Err {
    ErrTableNotExists(const string &msg) : Err(msg + " not exists") {}
};

// TODO: mutex lock()
class Storages {
public:
    Storages(const FSPtr &fs_, const CreateSqlPersistPtr &meta_persist_, const string &path_) :
        fs(fs_), meta_persist(meta_persist_), path(path_) {}

    void LoadTables() {
        vector<string> table_names;
        try {
            fs->ListSubDirs(path, table_names);
        } catch (ErrDirOpenFailed) {
        }
        for (const auto &name: table_names) {
            string create_sql;
            meta_persist->Load(path + "/" + name + "/create.sql", create_sql);
            CreateTable(create_sql);
        }
    }

    TablePtr CreateTable(const string &create_sql) {
        string name;
        TablePtr table = LoadTable(create_sql, name);
        tables[name] = table;
        meta_persist->Save(path + "/" + name + "/create.sql", create_sql.c_str(), create_sql.size());
        return table;
    }

    TablePtr LoadTable(const string &create_sql, string &name) {
        TokenParser parser(create_sql.c_str());

        if (!parser.MatchString("create"))
            throw ErrSchemaParsingUnexpectedString("create", parser.Unparsed());
        if (!parser.MatchString("table"))
            throw ErrSchemaParsingUnexpectedString("table", parser.Unparsed());

        bool ignore_if_exists = false;
        if (parser.MatchString("if"))
        {
            if (!parser.MatchString("not"))
                throw ErrSchemaParsingUnexpectedString("not", parser.Unparsed());
            if (!parser.MatchString("exists"))
                throw ErrSchemaParsingUnexpectedString("exists", parser.Unparsed());
            ignore_if_exists = true;
        }

        if (!parser.MatchToken(name))
            throw ErrSchemaParsingUnexpected(parser.Unparsed());

        if (!parser.MatchChar('('))
            throw ErrSchemaParsingUnexpectedChar('(', parser.Unparsed());
        Schema schema;
        parser.Assign(schema.FromString(type_factory, parser.Unparsed(), ','));
        if (!parser.MatchChar(')'))
            throw ErrSchemaParsingUnexpectedChar(')', parser.Unparsed());

        if (!parser.MatchString("primary"))
            throw ErrSchemaParsingUnexpectedString("primary", parser.Unparsed());
        if (!parser.MatchString("key"))
            throw ErrSchemaParsingUnexpectedString("key", parser.Unparsed());

        if (!parser.MatchChar('('))
            throw ErrSchemaParsingUnexpectedChar('(', parser.Unparsed());
        UnidirectSortDesc pk;
        parser.Assign(pk.FromString(parser.Unparsed(), ',', schema));
        if (!parser.MatchChar(')'))
            throw ErrSchemaParsingUnexpectedChar(')', parser.Unparsed());

        if (!parser.MatchString("engine"))
            throw ErrSchemaParsingUnexpectedString("engine", parser.Unparsed());
        if (!parser.MatchChar('='))
            throw ErrSchemaParsingUnexpectedChar('=', parser.Unparsed());
        string engine;
        if (!parser.MatchToken(engine))
            throw ErrSchemaParsingUnexpected(parser.Unparsed());

        // TODO: parse engine args
        // if (!parser.MatchChar('('))
        //     throw ErrSchemaParsingUnexpectedChar('(', parser.Unparsed());
        Args args;
        args["sort"] = true;
        args["print"] = true;
        // parser.Assign(args.FromString());
        // if (!parser.MatchChar(')'))
        //     throw ErrSchemaParsingUnexpectedChar(')', parser.Unparsed());

        return LoadTable(engine, name, schema, pk, args, ignore_if_exists);
    }

    TablePtr LoadTable(
        const string &engine,
        const string &name,
        const Schema &schema,
        const UnidirectSortDesc &pk,
        const Args &args,
        bool ignore_if_exists) {

        if (tables.find(name) != tables.end())
        {
            if (ignore_if_exists)
                return GetTable(name);
            else
                throw ErrTableExists("on creating table: '" + name + "'");
        }

        auto storage = storages.find(engine);
        if (storage == storages.end())
            throw ErrWrongUsage("unknown storage name: '" + engine + "'");

        string err = storage_args[engine].MatchedGetHelp(args);
        if (!err.empty())
            throw ErrWrongUsage("on create table '" + name + " ': " + err);

        SortedSchema sorted_schema;
        pk.SortSchemaColumns(schema, sorted_schema);
        return storage->second->CreateTable(name, sorted_schema, args);
    }

    TablePtr GetTable(const string &name) const {
        auto table = tables.find(name);
        if (table == tables.end())
            throw ErrTableNotExists("on get table: '" + name + "'");
        return table->second;
    }

    const TypeFactory & GetTypeFactory() const {
        return type_factory;
    }

    void Regester(StoragePtr &&storage) {
        const string name = storage->GetName();
        storages[name] = storage;
        storage_args[name] = storage->GetExpectedArgs();
    }

private:
    FSPtr fs;
    CreateSqlPersistPtr meta_persist;

    string path;
    unordered_map<string, StoragePtr> storages;
    unordered_map<string, TablePtr> tables;
    unordered_map<string, ExpectedArgs> storage_args;

    TypeFactory type_factory;
};

using StoragesPtr = shared_ptr<Storages>;

}
