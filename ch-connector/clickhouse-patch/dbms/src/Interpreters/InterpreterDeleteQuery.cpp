#include <IO/ConcatReadBuffer.h>

#include <DataStreams/ProhibitColumnsBlockOutputStream.h>
#include <DataStreams/MaterializingBlockOutputStream.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/NullableAdapterBlockInputStream.h>
#include <DataStreams/CastTypeBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/InterpreterDeleteQuery.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>


namespace ProfileEvents
{
    extern const Event DeleteQuery;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}


InterpreterDeleteQuery::InterpreterDeleteQuery(const ASTPtr & query_ptr_, const Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
    ProfileEvents::increment(ProfileEvents::DeleteQuery);
}


BlockIO InterpreterDeleteQuery::execute()
{
    ASTDeleteQuery & query = typeid_cast<ASTDeleteQuery &>(*query_ptr);
    StoragePtr table = context.getTable(query.database, query.table);

    StorageMergeTree * merge_tree = typeid_cast<StorageMergeTree *>(&*table);
    if (merge_tree && merge_tree->getData().merging_params.mode != MergeTreeData::MergingParams::Mutable)
        throw("Only MutableMergeTree support Delete.");

    auto table_lock = table->lockStructure(true);

    NamesAndTypesListPtr required_columns = std::make_shared<NamesAndTypesList>(table->getColumnsList());

    BlockOutputStreamPtr out;

    out = std::make_shared<PushingToViewsBlockOutputStream>(query.database, query.table, context, query_ptr);

    out = std::make_shared<MaterializingBlockOutputStream>(out);

    out = std::make_shared<AddingDefaultBlockOutputStream>(out,
        required_columns, table->column_defaults, context, static_cast<bool>(context.getSettingsRef().strict_insert_defaults));

    out = std::make_shared<ProhibitColumnsBlockOutputStream>(out, table->materialized_columns);

    out = std::make_shared<SquashingBlockOutputStream>(out,
        context.getSettingsRef().min_insert_block_size_rows,
        context.getSettingsRef().min_insert_block_size_bytes);

    auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
    out_wrapper->setProcessListElement(context.getProcessListElement());
    out = std::move(out_wrapper);

    BlockIO res;
    res.out_sample = table->getSampleBlockNonMaterializedNoHidden();

    if (!query.where)
        throw("Delete query must have WHERE.");

    InterpreterSelectQuery interpreter_select{query.select, context};

    res.in_sample = interpreter_select.getSampleBlock();

    res.in = interpreter_select.execute().in;

    res.in = std::make_shared<NullableAdapterBlockInputStream>(res.in, res.in_sample, res.out_sample);
    res.in = std::make_shared<CastTypeBlockInputStream>(context, res.in, res.out_sample);
    res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, out);

    return res;
}


}
