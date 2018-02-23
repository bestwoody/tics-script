#pragma once

#include <Core/Names.h>
#include <ext/singleton.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class HiddenColumns : public ext::singleton<HiddenColumns>
{
public:
    HiddenColumns()
    {
        mutable_hidden.push_back(mutable_version_column_name);
        mutable_hidden.push_back(mutable_delmark_column_name);

        all_hidden.insert(all_hidden.end(), mutable_hidden.begin(), mutable_hidden.end());
    }

    const OrderedNameSet & get(std::string table_type_name)
    {
        if ("MutableMergeTree" == table_type_name)
            return mutable_hidden;
        return empty;
    }

    void eraseHiddenColumns(Block & block, std::string table_type_name)
    {
        const OrderedNameSet & names = get(table_type_name);
        for (auto & it : names)
            if (block.has(it))
                block.erase(it);
    }

    static const std::string mutable_version_column_name;
    static const std::string mutable_delmark_column_name;

private:
    OrderedNameSet empty;
    OrderedNameSet mutable_hidden;
    OrderedNameSet all_hidden;
};

}
