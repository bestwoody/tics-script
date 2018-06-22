#pragma once

#include <Core/Names.h>
#include <ext/singleton.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class MutableSupport : public ext::singleton<MutableSupport>
{
public:
    MutableSupport()
    {
        mutable_hidden.push_back(version_column_name);
        mutable_hidden.push_back(delmark_column_name);

        all_hidden.insert(all_hidden.end(), mutable_hidden.begin(), mutable_hidden.end());
    }

    const OrderedNameSet & hiddenColumns(std::string table_type_name)
    {
        if (storage_name == table_type_name)
            return mutable_hidden;
        return empty;
    }

    void eraseHiddenColumns(Block & block, std::string table_type_name)
    {
        const OrderedNameSet & names = hiddenColumns(table_type_name);
        for (auto & it : names)
            if (block.has(it))
                block.erase(it);
    }

    static const std::string storage_name;
    static const std::string version_column_name;
    static const std::string delmark_column_name;

    enum DeduperType
    {
        DeduperOriginStreams            = 0,
        DeduperOriginUnity              = 1,
        DeduperReplacingUnity           = 2,
        DeduperReplacingPartitioning    = 3,
        DeduperDedupPartitioning        = 4,
        DeduperReplacingPartitioningOpt = 5,
    };

    static DeduperType toDeduperType(UInt64 type)
    {
        if(type > 5){
            throw Exception("illegal DeduperType: " + toString(type));
        }
        return (DeduperType)type;
    }

private:
    OrderedNameSet empty;
    OrderedNameSet mutable_hidden;
    OrderedNameSet all_hidden;
};

}